from __future__ import annotations

import hashlib
import hmac
import json
from datetime import date, datetime, time, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import Settings, get_settings
from ..models import Author, Content, RawObservation, RawObservationTopic, Source, TrendObservation
from ..sources.zhihu import zhihu_answer_reference_eligibility
from ..time import SHANGHAI, as_utc, now_utc, parse_datetime


def content_payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def author_hash(secret: str, source: str, source_user_id: str) -> str:
    message = f"{source}:{source_user_id}".encode()
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def _source_user_id(payload: dict[str, Any]) -> str | None:
    for key in ("author_id", "source_user_id", "user_id", "author", "user"):
        value = payload.get(key)
        if isinstance(value, dict):
            value = value.get("id") or value.get("uid")
        if value not in (None, ""):
            return str(value)
    return None


def insert_raw_observation(
    session: Session,
    source_id: int,
    observation,
    *,
    collector_version: str = "collector-v2",
    collected_at: datetime | None = None,
) -> tuple[RawObservation, bool]:
    """Insert one raw version; return (row, inserted).

    The unique key includes the payload hash so an edited post is appended as a
    new version while an exact re-collection is a no-op.
    """
    from ..sources.base import payload_hash

    collected_at = as_utc(collected_at or now_utc())
    assert collected_at is not None
    observed_at = as_utc(observation.observed_at) or collected_at
    published_at = as_utc(observation.published_at)
    semantics = str(observation.payload.get("timestamp_semantics") or "")
    if semantics in {"published", "created"} and published_at is None:
        raise ValueError("verified publication semantics require a publication time")
    if published_at is not None and published_at > observed_at + timedelta(minutes=10):
        raise ValueError("publication time is more than 10 minutes after observation time")
    updated_value = observation.payload.get("updated_at")
    if published_at is not None and updated_value not in (None, ""):
        updated_at = parse_datetime(updated_value)
        if updated_at is not None and updated_at < published_at:
            raise ValueError("updated_at is before published_at")
    digest = payload_hash(observation.payload)
    existing = session.scalar(
        select(RawObservation).where(
            RawObservation.source_id == source_id,
            RawObservation.source_item_id == observation.source_item_id,
            RawObservation.payload_hash == digest,
        )
    )
    if existing:
        return existing, False
    if observation.payload.get("body_truncated") is True:
        # A temporary detail-page failure must not make the newest immutable
        # version poorer than a complete version already stored for this post.
        # The source will capture a real edit once a complete detail response is
        # available again.
        versions = session.scalars(
            select(RawObservation)
            .where(
                RawObservation.source_id == source_id,
                RawObservation.source_item_id == observation.source_item_id,
            )
            .order_by(RawObservation.id.desc())
        ).all()
        complete = next(
            (
                version
                for version in versions
                if (version.payload or {}).get("body")
                and (version.payload or {}).get("body_truncated") is not True
            ),
            None,
        )
        if complete is not None:
            return complete, False
    row = RawObservation(
        source_id=source_id,
        source_item_id=observation.source_item_id,
        observation_kind=observation.observation_kind,
        published_at=published_at,
        observed_at=observed_at,
        collected_at=collected_at,
        payload=observation.payload,
        payload_hash=digest,
        collector_version=collector_version,
        created_at=collected_at,
    )
    session.add(row)
    session.flush()
    return row, True


def link_raw_observation_topic(
    session: Session,
    raw: RawObservation,
    *,
    topic_id: int,
    collection_query: str,
) -> bool:
    """Persist the configured topic whose query returned this observation."""

    existing = session.scalar(
        select(RawObservationTopic).where(
            RawObservationTopic.raw_observation_id == raw.id,
            RawObservationTopic.topic_id == topic_id,
        )
    )
    if existing is not None:
        return False
    session.add(
        RawObservationTopic(
            raw_observation_id=raw.id,
            topic_id=topic_id,
            collection_query=collection_query,
            created_at=now_utc(),
        )
    )
    session.flush()
    return True


def _upsert_author(
    session: Session, source_id: int, payload: dict[str, Any], settings: Settings, seen_at: datetime
):
    user_id = _source_user_id(payload)
    if user_id is None:
        return None
    from ..models import Source

    source_row = session.get(Source, source_id)
    if source_row is None:
        return None
    digest = author_hash(settings.author_hmac_secret, source_row.name, user_id)
    author = session.scalar(
        select(Author).where(Author.source_id == source_id, Author.author_hash == digest)
    )
    if author is None:
        author = Author(
            source_id=source_id,
            author_hash=digest,
            followers_bucket=str(payload.get("followers_bucket"))
            if payload.get("followers_bucket")
            else None,
            following_bucket=str(payload.get("following_bucket"))
            if payload.get("following_bucket")
            else None,
            account_age_bucket=str(payload.get("account_age_bucket"))
            if payload.get("account_age_bucket")
            else None,
            first_seen_at=seen_at,
            last_seen_at=seen_at,
        )
        session.add(author)
        session.flush()
    else:
        current_last_seen = as_utc(author.last_seen_at) or seen_at
        author.last_seen_at = max(current_last_seen, seen_at)
    return author


def normalize_raw_observation(
    session: Session, raw: RawObservation, *, settings: Settings | None = None
) -> Content | TrendObservation:
    settings = settings or get_settings()
    payload = raw.payload or {}
    observed_at = as_utc(raw.observed_at) or now_utc()
    if raw.observation_kind == "archived_snapshot":
        # Common Crawl snapshots are linked to known Content by enrichment.
        raise ValueError(f"{raw.observation_kind} remains raw-only evidence")
    reference_date: date | None = None
    if raw.observation_kind == "zhihu_answer_snapshot":
        eligible, reason = zhihu_answer_reference_eligibility(payload)
        if not eligible:
            raise ValueError(f"zhihu answer is not eligible for reference analysis: {reason}")
        reference_date = date.fromisoformat(str(payload["market_session_date"]))
        # This is an explicit analysis bucket, not a claimed publication time.
        # ``kind`` and the API time semantics keep that distinction visible.
        observed_at = as_utc(
            datetime.combine(reference_date, time(15), tzinfo=SHANGHAI)
        ) or observed_at
    if raw.observation_kind in {"search_index", "topic_rank", "trend", "pageviews"}:
        keyword = str(payload.get("keyword") or payload.get("query") or "")
        value = float(payload.get("value", 0))
        # Pageviews are daily buckets. Index them by the bucket date rather
        # than the later ingestion time so delayed responses remain a usable
        # time series.
        trend_at = (
            as_utc(raw.published_at) or observed_at
            if raw.observation_kind == "pageviews"
            else observed_at
        )
        existing = session.scalar(
            select(TrendObservation).where(
                TrendObservation.raw_observation_id == raw.id,
                TrendObservation.keyword == keyword,
                TrendObservation.observed_at == trend_at,
            )
        )
        if existing:
            return existing
        row = TrendObservation(
            raw_observation_id=raw.id,
            source_id=raw.source_id,
            keyword=keyword,
            topic_id=raw.topic_matches[0].topic_id if len(raw.topic_matches) == 1 else None,
            observed_at=trend_at,
            value=value,
            unit=payload.get("unit"),
            metadata_json=payload,
        )
        session.add(row)
        session.flush()
        return row

    source_item_id = (
        f"{raw.source_item_id}@{reference_date.isoformat()}"
        if reference_date is not None
        else raw.source_item_id
    )
    content = session.scalar(
        select(Content).where(
            Content.source_id == raw.source_id, Content.source_item_id == source_item_id
        )
    )
    published_at = as_utc(raw.published_at) or observed_at
    body = str(payload.get("body") or payload.get("text") or payload.get("content") or "")
    title = payload.get("title")
    digest = content_payload_hash(payload)
    author = _upsert_author(session, raw.source_id, payload, settings, observed_at)
    if content is None:
        content = Content(
            source_id=raw.source_id,
            source_item_id=source_item_id,
            kind=(
                "reference_answer"
                if reference_date is not None
                else str(payload.get("kind") or raw.observation_kind)
            ),
            author_id=author.id if author else None,
            published_at=published_at,
            first_collected_at=raw.collected_at,
            last_seen_at=observed_at,
            title=str(title) if title is not None else None,
            body=body,
            url=str(payload.get("url")) if payload.get("url") else None,
            likes=_integer(payload.get("likes")),
            favorites=_integer(payload.get("favorites")),
            comments=_integer(payload.get("comments")),
            shares=_integer(payload.get("shares")),
            views=_integer(payload.get("views")),
            content_hash=digest,
            language=str(payload.get("language")) if payload.get("language") else None,
        )
        session.add(content)
        session.flush()
    else:
        # Normalized state may point to the newest immutable raw version.
        current_last_seen = as_utc(content.last_seen_at) or observed_at
        current_published = as_utc(content.published_at) or published_at
        content.last_seen_at = max(current_last_seen, observed_at)
        # A source observation with explicit creation/publication semantics is
        # stronger evidence than an older ambiguous search timestamp. This is
        # especially important when a detail lookup corrects a prior
        # publish-or-edit timestamp.
        if payload.get("timestamp_semantics") in {"published", "created"}:
            content.published_at = published_at
        else:
            content.published_at = min(current_published, published_at)
        content.title = str(title) if title is not None else content.title
        # Archive enrichment can provide a longer body than the discovery
        # payload. A later normalization pass must not replace that richer
        # derived text with a shorter search snippet.
        if body and (not content.body or len(body) >= len(content.body)):
            content.body = body
        content.url = str(payload.get("url")) if payload.get("url") else content.url
        content.likes = _integer(payload.get("likes"), content.likes)
        content.favorites = _integer(payload.get("favorites"), content.favorites)
        content.comments = _integer(payload.get("comments"), content.comments)
        content.shares = _integer(payload.get("shares"), content.shares)
        content.views = _integer(payload.get("views"), content.views)
        content.content_hash = digest
        if author and content.author_id is None:
            content.author_id = author.id
    return content


def _integer(value: Any, default: int | None = None) -> int | None:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def normalize_pending(
    session: Session,
    *,
    limit: int = 500,
    settings: Settings | None = None,
    source_names: set[str] | None = None,
) -> int:
    settings = settings or get_settings()
    query = select(RawObservation)
    if source_names:
        query = query.join(Source).where(Source.name.in_(source_names))
    candidates = session.scalars(query.order_by(RawObservation.id)).all()
    latest_content_raw: dict[tuple[int, str], int] = {}
    for raw in candidates:
        if raw.observation_kind == "zhihu_answer_snapshot":
            eligible, _reason = zhihu_answer_reference_eligibility(raw.payload or {})
            if eligible:
                session_date = str((raw.payload or {}).get("market_session_date"))
                latest_content_raw[
                    (raw.source_id, f"{raw.source_item_id}@{session_date}")
                ] = raw.id
            continue
        if raw.observation_kind not in {
            "search_index",
            "topic_rank",
            "trend",
            "pageviews",
            "archived_snapshot",
            "zhihu_answer_snapshot",
        }:
            latest_content_raw[(raw.source_id, raw.source_item_id)] = raw.id
    raws = []
    for raw in candidates:
        if raw.observation_kind in {"search_index", "topic_rank", "trend", "pageviews"}:
            existing = session.scalar(
                select(TrendObservation).where(TrendObservation.raw_observation_id == raw.id)
            )
            if existing is None:
                raws.append(raw)
        elif raw.observation_kind == "archived_snapshot":
            continue
        elif raw.observation_kind == "zhihu_answer_snapshot":
            eligible, _reason = zhihu_answer_reference_eligibility(raw.payload or {})
            if not eligible:
                continue
            session_date = str((raw.payload or {}).get("market_session_date"))
            source_item_id = f"{raw.source_item_id}@{session_date}"
            if latest_content_raw.get((raw.source_id, source_item_id)) != raw.id:
                continue
            content = session.scalar(
                select(Content).where(
                    Content.source_id == raw.source_id,
                    Content.source_item_id == source_item_id,
                )
            )
            if content is None or content.content_hash != content_payload_hash(raw.payload):
                raws.append(raw)
        elif latest_content_raw.get((raw.source_id, raw.source_item_id)) == raw.id:
            content = session.scalar(
                select(Content).where(
                    Content.source_id == raw.source_id,
                    Content.source_item_id == raw.source_item_id,
                )
            )
            if content is None or content.content_hash != content_payload_hash(raw.payload):
                raws.append(raw)
        if len(raws) >= limit:
            break
    count = 0
    for raw in raws:
        normalize_raw_observation(session, raw, settings=settings)
        count += 1
    session.commit()
    return count
