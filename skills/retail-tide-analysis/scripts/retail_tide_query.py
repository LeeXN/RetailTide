#!/usr/bin/env python3
"""Read-only RetailTide API client that emits bounded JSON for LLM analysis."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

SHANGHAI = timezone(timedelta(hours=8))
CONTENT_FILTERS = ("all", "retail", "buy", "sell", "hold", "wait", "fomo", "panic", "promotion")
DEFAULT_BASE_URL = os.getenv("RETAIL_TIDE_URL", "http://127.0.0.1:8000")


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc


def validate_base_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise argparse.ArgumentTypeError("base URL must use http:// or https://")
    return value.rstrip("/")


def request_json(base_url: str, path: str, params: dict[str, Any] | None, timeout: float) -> Any:
    query = {
        key: value
        for key, value in (params or {}).items()
        if value is not None and value != ""
    }
    url = urljoin(f"{base_url}/", path.lstrip("/"))
    if query:
        url = f"{url}?{urlencode(query)}"
    request = Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "RetailTide-analysis-skill/1"},
        method="GET",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"RetailTide API returned HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"RetailTide API is unavailable: {exc.reason}") from exc


def range_values(
    base_url: str,
    from_date: date | None,
    to_date: date | None,
    timeout: float,
) -> tuple[date, date, dict[str, Any]]:
    if (from_date is None) != (to_date is None):
        raise ValueError("--from-date and --to-date must be provided together")
    if from_date is not None and to_date is not None:
        if from_date > to_date:
            raise ValueError("from-date cannot be after to-date")
        if (to_date - from_date).days >= 366:
            raise ValueError("date range cannot exceed 366 calendar days")
        overview = request_json(
            base_url,
            "/topics/overview",
            {"from_date": from_date.isoformat(), "to_date": to_date.isoformat()},
            timeout,
        )
        return from_date, to_date, overview

    overview = request_json(base_url, "/topics/overview", None, timeout)
    selected = overview.get("selected_date")
    if not selected:
        raise RuntimeError("RetailTide overview has no selected_date")
    end = date.fromisoformat(str(selected))
    start = end - timedelta(days=29)
    return start, end, overview


def api_time_bounds(start: date, end: date) -> tuple[str, str]:
    start_at = datetime.combine(start, time.min, tzinfo=SHANGHAI)
    end_at = datetime.combine(end, time.max, tzinfo=SHANGHAI)
    return start_at.isoformat(), end_at.isoformat()


def shanghai_date(value: Any) -> date | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(SHANGHAI).date()


def topic_row(topics: list[dict[str, Any]], slug: str | None) -> dict[str, Any] | None:
    if not slug:
        return None
    selected = next((row for row in topics if row.get("slug") == slug), None)
    if selected is None:
        known = ", ".join(sorted(str(row.get("slug")) for row in topics))
        raise ValueError(f"unknown topic {slug!r}; available topics: {known}")
    return selected


def trim_post(item: dict[str, Any], max_body_chars: int) -> dict[str, Any]:
    body = " ".join(str(item.get("body") or "").split())
    truncated = len(body) > max_body_chars
    if truncated:
        tail = min(160, max_body_chars // 4)
        body = f"{body[: max_body_chars - tail]} … {body[-tail:]}"
    analysis = item.get("analysis") or None
    return {
        "id": item.get("id"),
        "source": item.get("source_name"),
        "source_item_id": item.get("source_item_id"),
        "published_at": item.get("published_at"),
        "time_semantics": item.get("time_semantics"),
        "reference_date": item.get("reference_date"),
        "title": item.get("title"),
        "body": body,
        "body_truncated": truncated,
        "url": item.get("url"),
        "engagement": {
            key: item.get(key)
            for key in ("likes", "favorites", "comments", "shares", "views", "engagement_sum")
        },
        "analysis": analysis,
    }


def fetch_posts(
    base_url: str,
    *,
    topic_id: int | None,
    start: date,
    end: date,
    source: str,
    content_filter: str,
    post_limit: int,
    max_body_chars: int,
    timeout: float,
) -> dict[str, Any]:
    path = f"/topics/{topic_id}/contents" if topic_id is not None else "/contents"
    from_at, to_at = api_time_bounds(start, end)
    items: list[dict[str, Any]] = []
    offset = 0
    total = 0
    facets: dict[str, Any] = {}
    source_facets: dict[str, Any] = {}
    while len(items) < post_limit:
        page_limit = min(100, post_limit - len(items))
        payload = request_json(
            base_url,
            path,
            {
                "period": "custom",
                "from_at": from_at,
                "to_at": to_at,
                "source": source,
                "filter": content_filter,
                "limit": page_limit,
                "offset": offset,
            },
            timeout,
        )
        total = int(payload.get("total") or 0)
        facets = dict(payload.get("facets") or {})
        source_facets = dict(payload.get("source_facets") or {})
        page = list(payload.get("items") or [])
        items.extend(trim_post(item, max_body_chars) for item in page)
        offset += len(page)
        if not page or offset >= total:
            break
    return {
        "total": total,
        "returned": len(items),
        "truncated": len(items) < total,
        "facets": facets,
        "source_facets": source_facets,
        "items": items,
    }


def compact_source_status(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "name": row.get("name"),
            "enabled": row.get("enabled"),
            "health_status": row.get("health_status"),
            "configuration": row.get("configuration"),
            "evidence": row.get("evidence"),
            "quality": row.get("quality"),
            "collector_version": row.get("collector_version"),
        }
        for row in rows
    ]


def filtered_rows(
    rows: list[dict[str, Any]],
    *,
    start: date,
    end: date,
    topic_id: int | None,
    date_field: str,
) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        day = shanghai_date(row.get(date_field))
        if day is None or not start <= day <= end:
            continue
        if topic_id is not None and row.get("topic_id") != topic_id:
            continue
        result.append(row)
    return result


def warnings_for_bundle(
    overview: dict[str, Any],
    source_status: list[dict[str, Any]],
    posts: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    coverage = overview.get("coverage") or {}
    if coverage.get("analysis_pending_count"):
        warnings.append(
            f"selected end day has {coverage['analysis_pending_count']} content item(s) pending analysis"
        )
    if coverage.get("collection_status") not in {None, "complete"}:
        warnings.append(
            f"selected end-day collection status is {coverage.get('collection_status')}"
        )
    unhealthy = [
        str(row.get("name"))
        for row in source_status
        if row.get("enabled") and row.get("health_status") not in {None, "healthy"}
    ]
    if unhealthy:
        warnings.append("non-healthy enabled sources: " + ", ".join(sorted(unhealthy)))
    if posts.get("truncated"):
        warnings.append(
            f"post evidence is truncated to {posts.get('returned')} of {posts.get('total')} items"
        )
    return warnings


def command_bundle(args: argparse.Namespace) -> dict[str, Any]:
    start, end, overview = range_values(
        args.base_url, args.from_date, args.to_date, args.timeout
    )
    topics = request_json(args.base_url, "/topics", None, args.timeout)
    selected_topic = topic_row(topics, args.topic)
    topic_id = int(selected_topic["id"]) if selected_topic else None
    if selected_topic:
        overview = {
            **overview,
            "topics": [
                row for row in overview.get("topics") or [] if row.get("id") == topic_id
            ],
        }
    posts = fetch_posts(
        args.base_url,
        topic_id=topic_id,
        start=start,
        end=end,
        source=args.source,
        content_filter=args.content_filter,
        post_limit=args.post_limit,
        max_body_chars=args.max_body_chars,
        timeout=args.timeout,
    )
    attention = filtered_rows(
        request_json(
            args.base_url,
            "/trends/attention",
            {"limit": args.attention_limit},
            args.timeout,
        ),
        start=start,
        end=end,
        topic_id=topic_id,
        date_field="observed_at",
    )
    event_params: dict[str, Any] = {"limit": min(5000, args.event_limit * 5)}
    if topic_id is not None:
        event_params["topic_id"] = topic_id
    events = filtered_rows(
        request_json(args.base_url, "/events", event_params, args.timeout),
        start=start,
        end=end,
        topic_id=topic_id,
        date_field="started_at",
    )[: args.event_limit]
    source_status = compact_source_status(
        request_json(args.base_url, "/sources/status", None, args.timeout)
    )
    return {
        "meta": {
            "base_url": args.base_url,
            "timezone": "Asia/Shanghai",
            "from_date": start.isoformat(),
            "to_date": end.isoformat(),
            "topic": selected_topic,
            "source_filter": args.source,
            "content_filter": args.content_filter,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "read_only": True,
        },
        "warnings": warnings_for_bundle(overview, source_status, posts),
        "source_status": source_status,
        "overview": overview,
        "attention": attention,
        "posts": posts,
        "events": events,
    }


def command_overview(args: argparse.Namespace) -> dict[str, Any]:
    _start, _end, overview = range_values(
        args.base_url, args.from_date, args.to_date, args.timeout
    )
    if args.topic:
        topics = request_json(args.base_url, "/topics", None, args.timeout)
        selected = topic_row(topics, args.topic)
        overview = {
            **overview,
            "topics": [
                row
                for row in overview.get("topics") or []
                if row.get("id") == selected.get("id")
            ],
        }
    return overview


def command_posts(args: argparse.Namespace) -> dict[str, Any]:
    start, end, _overview = range_values(
        args.base_url, args.from_date, args.to_date, args.timeout
    )
    topics = request_json(args.base_url, "/topics", None, args.timeout)
    selected = topic_row(topics, args.topic)
    return fetch_posts(
        args.base_url,
        topic_id=int(selected["id"]) if selected else None,
        start=start,
        end=end,
        source=args.source,
        content_filter=args.content_filter,
        post_limit=args.post_limit,
        max_body_chars=args.max_body_chars,
        timeout=args.timeout,
    )


def command_research(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "topic": args.topic,
        "event_study": request_json(
            args.base_url,
            "/research/event-study",
            {"topic": args.topic, "event": args.event},
            args.timeout,
        ),
        "quantile_study": request_json(
            args.base_url,
            "/research/quantile-study",
            {"topic": args.topic, "metric": args.metric, "horizon": args.horizon},
            args.timeout,
        ),
    }


def add_range_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--from-date", type=parse_date, help="Inclusive Shanghai date")
    parser.add_argument("--to-date", type=parse_date, help="Inclusive Shanghai date")


def add_post_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--topic", help="Topic slug; omit for the whole market")
    parser.add_argument("--source", default="all", help="Source name or all")
    parser.add_argument(
        "--filter",
        dest="content_filter",
        choices=CONTENT_FILTERS,
        default="all",
    )
    parser.add_argument("--post-limit", type=int, default=100)
    parser.add_argument("--max-body-chars", type=int, default=800)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", type=validate_base_url, default=DEFAULT_BASE_URL)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bundle = subparsers.add_parser("bundle", help="Build a bounded analysis evidence bundle")
    add_range_arguments(bundle)
    add_post_arguments(bundle)
    bundle.add_argument("--attention-limit", type=int, default=5000)
    bundle.add_argument("--event-limit", type=int, default=50)
    bundle.set_defaults(handler=command_bundle)

    overview = subparsers.add_parser("overview", help="Read market/topic aggregate history")
    add_range_arguments(overview)
    overview.add_argument("--topic", help="Topic slug; omit for all topics")
    overview.set_defaults(handler=command_overview)

    posts = subparsers.add_parser("posts", help="Read bounded post evidence")
    add_range_arguments(posts)
    add_post_arguments(posts)
    posts.set_defaults(handler=command_posts)

    sources = subparsers.add_parser("sources", help="Read source health and persisted evidence")
    sources.set_defaults(
        handler=lambda args: compact_source_status(
            request_json(args.base_url, "/sources/status", None, args.timeout)
        )
    )

    research = subparsers.add_parser("research", help="Read event and quantile studies")
    research.add_argument("--topic", default="gold")
    research.add_argument("--event", default="fomo_spike")
    research.add_argument("--metric", default="fomo_ratio")
    research.add_argument("--horizon", choices=("1d", "3d", "5d", "10d", "20d"), default="5d")
    research.set_defaults(handler=command_research)
    return parser


def validate_limits(args: argparse.Namespace) -> None:
    if args.timeout <= 0:
        raise ValueError("timeout must be positive")
    for name, minimum, maximum in (
        ("post_limit", 1, 500),
        ("max_body_chars", 100, 5000),
        ("attention_limit", 1, 5000),
        ("event_limit", 1, 500),
    ):
        value = getattr(args, name, None)
        if value is not None and not minimum <= value <= maximum:
            raise ValueError(
                f"{name.replace('_', '-')} must be between {minimum} and {maximum}"
            )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        validate_limits(args)
        payload = args.handler(args)
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=None if args.compact else 2,
            separators=(",", ":") if args.compact else None,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
