from __future__ import annotations

from collections import Counter, defaultdict
from datetime import timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Content, RawObservation, Source
from ..time import as_utc, parse_datetime

VERIFIED_PUBLICATION_SEMANTICS = {"published", "created"}


def publication_time_audit(session: Session, *, sample_limit: int = 20) -> dict[str, Any]:
    """Audit the newest immutable source version behind every normalized post."""

    raw_rows = session.scalars(select(RawObservation).order_by(RawObservation.id)).all()
    latest: dict[tuple[int, str], RawObservation] = {}
    for raw in raw_rows:
        latest[(raw.source_id, raw.source_item_id)] = raw
    contents = {
        (content.source_id, content.source_item_id): content
        for content in session.scalars(select(Content)).all()
    }
    sources = {source.id: source.name for source in session.scalars(select(Source)).all()}
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "items": 0,
            "verified": 0,
            "ambiguous": 0,
            "missing_publication_time": 0,
            "future_after_observation": 0,
            "updated_before_published": 0,
            "normalized_missing": 0,
            "content_time_mismatch": 0,
            "timestamp_fields": Counter(),
        }
    )
    problems: list[dict[str, Any]] = []
    for key, raw in latest.items():
        source_name = sources.get(raw.source_id, "unknown")
        row = stats[source_name]
        row["items"] += 1
        payload = raw.payload or {}
        semantics = str(payload.get("timestamp_semantics") or "ambiguous")
        verified = semantics in VERIFIED_PUBLICATION_SEMANTICS
        row["verified" if verified else "ambiguous"] += 1
        field = str(payload.get("source_timestamp_field") or "unspecified")
        row["timestamp_fields"][field] += 1
        published_at = as_utc(raw.published_at)
        observed_at = as_utc(raw.observed_at)
        issue_names: list[str] = []
        if published_at is None:
            row["missing_publication_time"] += 1
            issue_names.append("missing_publication_time")
        if (
            published_at is not None
            and observed_at is not None
            and published_at > observed_at + timedelta(minutes=10)
        ):
            row["future_after_observation"] += 1
            issue_names.append("future_after_observation")
        updated_value = payload.get("updated_at")
        if published_at is not None and updated_value not in (None, ""):
            try:
                updated_at = parse_datetime(updated_value)
            except ValueError:
                updated_at = None
                issue_names.append("invalid_updated_at")
            if updated_at is not None and updated_at < published_at:
                row["updated_before_published"] += 1
                issue_names.append("updated_before_published")
        content = contents.get(key)
        content_time = as_utc(content.published_at) if content is not None else None
        if content is None:
            row["normalized_missing"] += 1
            issue_names.append("normalized_missing")
        elif verified and published_at is not None and content_time != published_at:
            row["content_time_mismatch"] += 1
            issue_names.append("content_time_mismatch")
        if (issue_names or not verified) and len(problems) < sample_limit:
            problems.append(
                {
                    "source": source_name,
                    "source_item_id": raw.source_item_id,
                    "semantics": semantics,
                    "source_timestamp_field": field,
                    "published_at": published_at,
                    "observed_at": observed_at,
                    "content_published_at": content_time,
                    "issues": issue_names or ["ambiguous_timestamp_semantics"],
                    "url": payload.get("url"),
                }
            )
    source_rows = []
    for source_name in sorted(stats):
        row = stats[source_name]
        items = int(row["items"])
        source_rows.append(
            {
                **{key: value for key, value in row.items() if key != "timestamp_fields"},
                "source": source_name,
                "verified_ratio": row["verified"] / items if items else 0.0,
                "timestamp_fields": dict(row["timestamp_fields"].most_common()),
            }
        )
    return {
        "items": len(latest),
        "verified": sum(row["verified"] for row in stats.values()),
        "ambiguous": sum(row["ambiguous"] for row in stats.values()),
        "sources": source_rows,
        "problem_samples": problems,
    }
