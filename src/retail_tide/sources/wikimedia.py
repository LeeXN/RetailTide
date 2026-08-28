from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

import httpx

from ..schemas import CollectResult, RawObservation
from ..time import as_utc, now_utc
from .base import ProbeResult, SourceError, public_get

WIKIMEDIA_PAGEVIEWS_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
WIKIMEDIA_AGGREGATE_PAGEVIEWS_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/aggregate"


def _target(query: str) -> tuple[str, str]:
    value = str(query or "").strip()
    if value.startswith("{"):
        try:
            payload = json.loads(value)
        except json.JSONDecodeError as exc:
            raise SourceError("wikimedia target JSON is malformed") from exc
        if isinstance(payload, dict):
            project = str(payload.get("project") or "zh.wikipedia.org")
            article = str(payload.get("article") or payload.get("title") or "").strip()
            if article:
                return project, article
    if "|" in value:
        project, article = value.split("|", 1)
        if article.strip():
            return project.strip() or "zh.wikipedia.org", article.strip()
    return "zh.wikipedia.org", value


def _day_from_timestamp(value: str) -> date:
    return datetime.strptime(str(value)[:8], "%Y%m%d").replace(tzinfo=timezone.utc).date()


def _utc_bucket_starts(since: datetime, until: datetime) -> list[datetime]:
    """Return UTC daily buckets represented by a bounded product window."""

    start_at = as_utc(since) or since
    end_at = as_utc(until) or until
    bucket_at = datetime.combine(start_at.date(), datetime.min.time(), tzinfo=timezone.utc)
    if bucket_at < start_at:
        bucket_at += timedelta(days=1)
    buckets = []
    while bucket_at < end_at:
        buckets.append(bucket_at)
        bucket_at += timedelta(days=1)
    return buckets


class WikimediaPageviewsSource:
    source_type = "trend"

    def __init__(
        self,
        *,
        user_agent: str | None = None,
        min_interval: float = 1.0,
        use_fixture: bool = False,
        transport: httpx.AsyncBaseTransport | None = None,
        **_kwargs,
    ):
        self.name = "wikimedia-pageviews"
        self.user_agent = user_agent
        self.min_interval = max(0.0, float(min_interval))
        self.use_fixture = use_fixture
        self.transport = transport

    async def collect(
        self,
        query: str,
        since: datetime,
        cursor: str | None = None,
        *,
        until: datetime | None = None,
    ) -> CollectResult:
        if cursor:
            raise SourceError("wikimedia pageviews does not support pagination cursors")
        if self.use_fixture:
            now = as_utc(until or now_utc()) or now_utc()
            return CollectResult(
                items=[
                    RawObservation(
                        source=self.name,
                        source_item_id=f"fixture:{query}:{now.date().isoformat()}",
                        observation_kind="pageviews",
                        published_at=now,
                        observed_at=now,
                        payload={
                            "keyword": query,
                            "project": "zh.wikipedia.org",
                            "article": query,
                            "date": now.date().isoformat(),
                            "value": 100,
                            "unit": "views",
                        },
                    )
                ],
                exhausted=True,
            )
        if not self.user_agent:
            raise SourceError("wikimedia-pageviews requires RETAIL_TIDE_HTTP_USER_AGENT")

        start_at = as_utc(since) or since
        end_at = as_utc(until) or now_utc()
        if start_at >= end_at:
            raise SourceError("wikimedia pageviews window must have since before until")
        expected_buckets = _utc_bucket_starts(start_at, end_at)
        if not expected_buckets:
            return CollectResult(
                items=[],
                exhausted=True,
                diagnostics={
                    "time_basis": "utc_daily",
                    "expected_utc_dates": [],
                    "available_utc_dates": [],
                    "pending_utc_dates": [],
                    "availability_pending": False,
                },
            )
        start = expected_buckets[0].date()
        end = expected_buckets[-1].date()
        project, article = _target(query)
        if not article:
            raise SourceError("wikimedia pageviews target article is empty")
        article_url = (
            f"{WIKIMEDIA_PAGEVIEWS_URL}/{quote(project, safe='')}/all-access/user/"
            f"{quote(article, safe='')}/daily/{start:%Y%m%d}/{end:%Y%m%d}"
        )
        aggregate_url = (
            f"{WIKIMEDIA_AGGREGATE_PAGEVIEWS_URL}/{quote(project, safe='')}/"
            f"all-access/user/daily/{start:%Y%m%d}/{end:%Y%m%d}"
        )
        headers = {"Accept": "application/json", "User-Agent": self.user_agent}
        try:
            async with httpx.AsyncClient(
                timeout=30,
                headers=headers,
                transport=self.transport,
            ) as client:
                try:
                    aggregate_response = await public_get(
                        client,
                        aggregate_url,
                        rate_source=self.name,
                        min_interval=self.min_interval,
                    )
                    aggregate_payload = aggregate_response.json()
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code != 404:
                        raise
                    aggregate_payload = {"items": []}

                aggregate_items = aggregate_payload.get("items")
                if not isinstance(aggregate_items, list):
                    raise SourceError("wikimedia aggregate pageviews response has no items list")
                available_days = set()
                for record in aggregate_items:
                    if not isinstance(record, dict):
                        continue
                    try:
                        available_days.add(_day_from_timestamp(record.get("timestamp", "")))
                    except ValueError:
                        continue

                article_payload: dict = {"items": []}
                if available_days:
                    try:
                        article_response = await public_get(
                            client,
                            article_url,
                            rate_source=self.name,
                            min_interval=self.min_interval,
                        )
                        article_payload = article_response.json()
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code != 404:
                            raise
        except httpx.HTTPStatusError as exc:
            raise SourceError(
                f"wikimedia pageviews failed: HTTP {exc.response.status_code}"
            ) from exc
        except SourceError:
            raise
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            detail = str(exc).strip() or type(exc).__name__
            raise SourceError(
                f"wikimedia pageviews failed: {type(exc).__name__}: {detail}"
            ) from exc

        if not isinstance(article_payload, dict) or not isinstance(
            article_payload.get("items"), list
        ):
            raise SourceError("wikimedia pageviews response has no items list")
        records_by_day: dict[date, dict] = {}
        for record in article_payload["items"]:
            if not isinstance(record, dict):
                continue
            try:
                records_by_day[_day_from_timestamp(record.get("timestamp", ""))] = record
            except ValueError:
                continue

        expected_days = [bucket.date() for bucket in expected_buckets]
        pending_days = [day for day in expected_days if day not in available_days]
        items: list[RawObservation] = []
        observed_at = now_utc()
        for bucket_at in expected_buckets:
            bucket_day = bucket_at.date()
            if bucket_day not in available_days:
                continue
            record = records_by_day.get(bucket_day)
            timestamp = (
                str(record.get("timestamp"))
                if record is not None and record.get("timestamp")
                else f"{bucket_day:%Y%m%d}00"
            )
            value = record.get("views") if record is not None else 0
            value = 0 if value is None else value
            record_payload = record or {}
            item_id = f"{project}:{article}:{timestamp}:user"
            items.append(
                RawObservation(
                    source=self.name,
                    source_item_id=item_id,
                    observation_kind="pageviews",
                    published_at=bucket_at,
                    observed_at=observed_at,
                    payload={
                        "id": item_id,
                        "keyword": article,
                        "project": project,
                        "article": article,
                        "date": bucket_day.isoformat(),
                        "timestamp": timestamp,
                        "value": float(value),
                        "unit": "views",
                        "access": record_payload.get("access", "all-access"),
                        "agent": record_payload.get("agent", "user"),
                        "granularity": record_payload.get("granularity", "daily"),
                        "provider": "wikimedia-pageviews",
                        "time_basis": "utc_daily",
                        "availability_verified_by": "project-aggregate",
                        "provider_missing_as_zero": record is None,
                    },
                )
            )
        warnings = []
        if pending_days:
            warnings.append(
                "wikimedia UTC daily bucket(s) are not available yet: "
                + ", ".join(day.isoformat() for day in pending_days)
            )
        return CollectResult(
            items=items,
            exhausted=not pending_days,
            warnings=warnings,
            partial=bool(pending_days),
            diagnostics={
                "time_basis": "utc_daily",
                "expected_utc_dates": [day.isoformat() for day in expected_days],
                "available_utc_dates": sorted(day.isoformat() for day in available_days),
                "pending_utc_dates": [day.isoformat() for day in pending_days],
                "availability_pending": bool(pending_days),
            },
        )

    def probe(self) -> ProbeResult:
        return ProbeResult(
            source=self.name,
            source_type=self.source_type,
            source_role="trend",
            checks={
                "transport": "wikimedia-analytics-rest",
                "daily_pageviews": True,
                "text_content": False,
                "user_agent_required": True,
                "pagination": False,
            },
            notes=[
                "Pageviews are aggregate attention data and are not sent to the content LLM.",
                "Recent buckets may be delayed by the upstream analytics pipeline.",
            ],
        )
