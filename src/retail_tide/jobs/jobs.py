from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from ..config import Settings, get_settings, llm_config_status, source_config_status
from ..models import (
    ArchiveLookupState,
    ArchiveSnapshot,
    CollectionCheckpoint,
    CollectionTask,
    Content,
    RawObservation,
    Source,
    Topic,
)
from ..observability import increment, timer
from ..pipeline import (
    analysis_task_summary,
    analyze_pending,
    enqueue_pending_analysis_tasks,
    insert_raw_observation,
    link_raw_observation_topic,
    normalize_pending,
    resolve_pending_entities,
)
from ..pipeline.events import detect_events
from ..pipeline.metrics import aggregate_metrics
from ..pipeline.quality import refresh_source_quality
from ..pipeline.returns import evaluate_events
from ..pipeline.trends import aggregate_trend_signals
from ..schemas import RawObservation as RawObservationSchema
from ..sources import SourceError, source_for_name
from ..sources.commoncrawl import CommonCrawlSource, canonical_url
from ..sources.guba import guba_hot_cursor
from ..sources.xiaohongshu import xiaohongshu_strategy_cursor
from ..sources.zhihu import zhihu_market_question_query
from ..time import SHANGHAI, as_utc, now_utc, resolve_collection_window

INCREMENTAL_FIRST_WINDOW = timedelta(hours=24)
INCREMENTAL_OVERLAP = timedelta(hours=2)
logger = logging.getLogger(__name__)

ZHIHU_MARKET_SCOPES = {
    "broad-a-share": "A股",
    "hang-seng-tech": "港股",
    "nasdaq": "美股",
}

SOURCE_BACKFILL_BATCH_CAP = {
    # These public/community sources react poorly to long request bursts. The
    # checkpoint loop still makes progress, but cools down after a small batch.
    "guba": 10,
    "taoguba": 2,
    "xiaohongshu": 1,
}
PARTIAL_SOURCE_CIRCUIT_BREAKER = 3
SOURCE_TRANSPORT_REVISIONS = {
    "guba": "webarticlelist-hot-v2",
    "xiaohongshu": "dual-transport-history-sampling-v4",
}
DEFAULT_XIAOHONGSHU_DISCOVERY_QUERIES = (
    "投资",
    "股票",
    "ETF",
    "基金",
    "持仓",
    "散户",
)


def _merge_collection_diagnostics(
    target: dict[str, Any], incoming: dict[str, Any]
) -> dict[str, Any]:
    """Merge credential-free per-page evidence into a collection summary."""

    for key, value in incoming.items():
        if isinstance(value, dict):
            bucket = target.setdefault(key, {})
            if not isinstance(bucket, dict):
                target[key] = dict(value)
                continue
            for nested_key, nested_value in value.items():
                if isinstance(nested_value, int | float) and not isinstance(nested_value, bool):
                    bucket[nested_key] = bucket.get(nested_key, 0) + nested_value
                else:
                    bucket[nested_key] = nested_value
        elif isinstance(value, bool):
            target[key] = bool(target.get(key)) or value
        elif isinstance(value, int | float):
            target[key] = target.get(key, 0) + value
        else:
            target[key] = value
    return target


def ensure_source(session: Session, name: str, *, settings: Settings | None = None) -> Source:
    settings = settings or get_settings()
    source = session.scalar(select(Source).where(Source.name == name))
    if source is None:
        source_type = {
            "wikimedia-pageviews": "trend",
            "common-crawl": "archive",
        }.get(name, "content")
        source = Source(
            name=name,
            source_type=source_type,
            enabled=True,
            collector_version=settings.collector_version,
            health_status="healthy",
            created_at=now_utc(),
        )
        session.add(source)
        session.flush()
    return source


def _observation_in_window(item, since: datetime, until: datetime) -> bool:
    # Discovery ranking is not permission to leak old posts into a bounded
    # historical rebuild. Every content source must provide a publication time
    # and pass the same requested window.
    # Trend providers own the bucket filtering. A daily provider point may be
    # observed now while its bucket starts before a short incremental window.
    if item.observation_kind in {"search_index", "topic_rank", "trend", "pageviews"}:
        return True
    if item.observation_kind == "zhihu_answer_snapshot":
        try:
            target = datetime.fromisoformat(str(item.payload["market_session_date"])).date()
        except (KeyError, TypeError, ValueError):
            return False
        local_start = since.astimezone(SHANGHAI).date()
        local_end = (until - timedelta(microseconds=1)).astimezone(SHANGHAI).date()
        return local_start <= target <= local_end
    observed_at = as_utc(item.published_at or item.observed_at)
    return observed_at is not None and since <= observed_at < until


@asynccontextmanager
async def _collection_write_section(
    write_lock: asyncio.Lock | None,
) -> AsyncIterator[None]:
    """Serialize synchronous SQLAlchemy work while source requests run concurrently."""

    if write_lock is None:
        yield
        return
    async with write_lock:
        yield


async def collect_source_async(
    session: Session,
    source_name: str,
    *,
    query: str = "黄金",
    since: datetime,
    until: datetime | None = None,
    settings: Settings | None = None,
    max_pages: int = 100,
    topic_id: int | None = None,
    start_cursor: str | None = None,
    allow_partial: bool = False,
    _write_lock: asyncio.Lock | None = None,
) -> dict[str, Any]:
    settings = settings or get_settings()
    normalized_name = source_name.lower().replace("_", "-")
    if settings.data_mode == "live" and normalized_name not in settings.enabled_sources:
        logger.warning(
            "event=source_collection_skipped source=%s reason=disabled",
            normalized_name,
        )
        return {
            "source": normalized_name,
            "pages": 0,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_degraded": True,
            "error": "source is disabled; add it to RETAIL_TIDE_ENABLED_SOURCES first",
            "missing_config": ["RETAIL_TIDE_ENABLED_SOURCES"],
        }
    readiness = source_config_status(normalized_name, settings=settings)
    if not readiness["configured"]:
        logger.warning(
            "event=source_collection_skipped source=%s reason=incomplete_config missing=%s",
            normalized_name,
            ",".join(str(item) for item in readiness["missing"]),
        )
        return {
            "source": normalized_name,
            "pages": 0,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_degraded": True,
            "error": "source configuration is incomplete; no network request was made",
            "missing_config": readiness["missing"],
        }
    since = as_utc(since) or since
    until = as_utc(until) or now_utc()
    if since >= until:
        logger.warning(
            "event=source_collection_skipped source=%s reason=invalid_window since=%s until=%s",
            normalized_name,
            since.isoformat(),
            until.isoformat(),
        )
        return {
            "source": normalized_name,
            "pages": 0,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_degraded": True,
            "error": "collection window must have since before until",
        }
    interval = settings.request_interval(normalized_name)
    async with _collection_write_section(_write_lock):
        try:
            source_row = ensure_source(session, normalized_name, settings=settings)
            source_row.collector_version = settings.collector_version
            known_rows = session.execute(
                select(
                    RawObservation.id,
                    RawObservation.source_item_id,
                    RawObservation.published_at,
                )
                .where(RawObservation.source_id == source_row.id)
                .order_by(RawObservation.id)
            ).all()
            known_raw_ids: dict[str, int] = {}
            known_published_at: dict[str, datetime] = {}
            for raw_id, source_item_id, published_at in known_rows:
                known_raw_ids[source_item_id] = raw_id
                if published_at is None:
                    continue
                current = known_published_at.get(source_item_id)
                if current is None or published_at > current:
                    known_published_at[source_item_id] = published_at
            kwargs = {
                "credential": settings.source_credential(normalized_name),
                "use_fixture": settings.data_mode == "demo",
            }
            if normalized_name in {"common-crawl", "wikimedia-pageviews"}:
                kwargs.update(user_agent=settings.http_user_agent, min_interval=interval)
            elif normalized_name in {"guba", "taoguba"}:
                kwargs["min_public_interval"] = interval
                kwargs["session_file"] = settings.source_session_file(normalized_name)
                if normalized_name == "guba" and until - since > timedelta(days=1):
                    # Historical list pages already contain a usable body/abstract.
                    # Avoid multiplying each page into up to four extra article
                    # requests; normalization retains body_truncated provenance and a
                    # later archive/detail enrichment can fill selected rows.
                    kwargs["max_detail_requests"] = 0
            elif normalized_name == "xiaohongshu":
                kwargs["min_request_interval"] = interval
                kwargs["spider_endpoint"] = settings.xiaohongshu_spider_endpoint
                kwargs["spider_credential"] = settings.xiaohongshu_spider_credential()
                kwargs["known_source_item_ids"] = set(known_raw_ids)
                kwargs["known_source_published_at"] = known_published_at
            elif normalized_name == "zhihu":
                kwargs["min_public_interval"] = interval
            source_row_id = source_row.id
            session.commit()
        except Exception:
            session.rollback()
            raise
    collector = source_for_name(normalized_name, **kwargs)
    collection_started = time.monotonic()
    logger.info(
        "event=source_collection_started source=%s topic_id=%s query=%r since=%s until=%s "
        "max_pages=%d min_interval_seconds=%.3f",
        normalized_name,
        topic_id,
        query,
        since.isoformat(),
        until.isoformat(),
        max_pages,
        interval,
    )
    cursor = start_cursor
    seen_cursors = set()
    pages = 0
    inserted = 0
    duplicates = 0
    topic_links_added = 0
    warnings: list[str] = []
    diagnostics: dict[str, Any] = {}
    try:
        while pages < max_pages:
            if cursor in seen_cursors:
                raise SourceError("source returned a repeated pagination cursor")
            if cursor:
                seen_cursors.add(cursor)
            increment("collector_requests_total")
            page_started = time.monotonic()
            before_inserted = inserted
            before_duplicates = duplicates
            before_links = topic_links_added
            logger.debug(
                "event=source_page_requested source=%s topic_id=%s page=%d cursor_present=%s",
                normalized_name,
                topic_id,
                pages + 1,
                bool(cursor),
            )
            result = await collector.collect(query, since, cursor, until=until)
            _merge_collection_diagnostics(diagnostics, result.diagnostics)
            for warning in result.warnings:
                if warning not in warnings:
                    warnings.append(warning)
            increment("collector_items_total", len(result.items))
            async with _collection_write_section(_write_lock):
                try:
                    for item in result.items:
                        if not _observation_in_window(item, since, until):
                            continue
                        # Most content sources are de-duplicated by stable item
                        # ID before hashing. Zhihu rank snapshots are different:
                        # the same answer may be a valid reference on more than
                        # one market session, so its session-specific payload
                        # must reach append-only raw storage.
                        known_raw_id = (
                            None
                            if item.observation_kind == "zhihu_answer_snapshot"
                            else known_raw_ids.get(item.source_item_id)
                        )
                        if known_raw_id is not None:
                            duplicates += 1
                            if topic_id is not None:
                                known_raw = session.get(RawObservation, known_raw_id)
                                if known_raw is not None:
                                    topic_links_added += int(
                                        link_raw_observation_topic(
                                            session,
                                            known_raw,
                                            topic_id=topic_id,
                                            collection_query=query,
                                        )
                                    )
                            continue
                        row, did_insert = insert_raw_observation(
                            session,
                            source_row_id,
                            item,
                            collector_version=settings.collector_version,
                        )
                        inserted += int(did_insert)
                        duplicates += int(not did_insert)
                        known_raw_ids[item.source_item_id] = row.id
                        if topic_id is not None:
                            topic_links_added += int(
                                link_raw_observation_topic(
                                    session,
                                    row,
                                    topic_id=topic_id,
                                    collection_query=query,
                                )
                            )
                    pages += 1
                    session.commit()
                except Exception:
                    session.rollback()
                    raise
            logger.info(
                "event=source_page_completed source=%s topic_id=%s page=%d received=%d "
                "inserted=%d duplicates=%d topic_links_added=%d exhausted=%s "
                "elapsed_seconds=%.3f",
                normalized_name,
                topic_id,
                pages,
                len(result.items),
                inserted - before_inserted,
                duplicates - before_duplicates,
                topic_links_added - before_links,
                result.exhausted or not result.next_cursor,
                time.monotonic() - page_started,
            )
            if result.exhausted or not result.next_cursor:
                break
            cursor = result.next_cursor
        else:
            if allow_partial and cursor:
                async with _collection_write_section(_write_lock):
                    source_row = session.get(Source, source_row_id)
                    if source_row is not None:
                        source_row.health_status = "partial" if result.partial else "healthy"
                    session.commit()
                logger.warning(
                    "event=source_collection_partial source=%s topic_id=%s pages=%d inserted=%d "
                    "duplicates=%d warnings=%d elapsed_seconds=%.3f",
                    normalized_name,
                    topic_id,
                    pages,
                    inserted,
                    duplicates,
                    len(warnings),
                    time.monotonic() - collection_started,
                )
                return {
                    "source": normalized_name,
                    "pages": pages,
                    "items_collected": inserted,
                    "duplicates": duplicates,
                    "topic_links_added": topic_links_added,
                    "source_degraded": False,
                    "source_partial": result.partial,
                    "warnings": warnings,
                    "diagnostics": diagnostics,
                    "exhausted": False,
                    "next_cursor": cursor,
                }
            raise SourceError("source pagination exceeded max_pages")
        async with _collection_write_section(_write_lock):
            source_row = session.get(Source, source_row_id)
            if source_row is not None:
                source_row.health_status = "partial" if result.partial else "healthy"
            session.commit()
        logger.info(
            "event=source_collection_completed source=%s topic_id=%s pages=%d inserted=%d "
            "duplicates=%d topic_links_added=%d warnings=%d elapsed_seconds=%.3f",
            normalized_name,
            topic_id,
            pages,
            inserted,
            duplicates,
            topic_links_added,
            len(warnings),
            time.monotonic() - collection_started,
        )
        return {
            "source": normalized_name,
            "pages": pages,
            "items_collected": inserted,
            "duplicates": duplicates,
            "topic_links_added": topic_links_added,
            "source_degraded": False,
            "source_partial": result.partial,
            "warnings": warnings,
            "diagnostics": diagnostics,
            "exhausted": True,
            "next_cursor": None,
        }
    except Exception as exc:  # noqa: BLE001 - every collector failure marks source degraded
        increment("collector_errors_total")
        error_code = getattr(exc, "error_code", None)
        retry_after_seconds = getattr(exc, "retry_after_seconds", None)
        transport_name = getattr(exc, "transport_name", None)
        if error_code:
            diagnostics.update(
                {
                    "error_code": error_code,
                    "retry_after_seconds": retry_after_seconds,
                    "failed_transport": transport_name,
                }
            )
        async with _collection_write_section(_write_lock):
            session.rollback()
            source_row = session.get(Source, source_row_id)
            if source_row:
                source_row.health_status = "degraded"
                session.commit()
        logger.warning(
            "event=source_collection_failed source=%s topic_id=%s pages=%d inserted=%d "
            "duplicates=%d elapsed_seconds=%.3f error=%r",
            normalized_name,
            topic_id,
            pages,
            inserted,
            duplicates,
            time.monotonic() - collection_started,
            str(exc),
        )
        return {
            "source": normalized_name,
            "pages": pages,
            "items_collected": inserted,
            "duplicates": duplicates,
            "topic_links_added": topic_links_added,
            "source_degraded": True,
            "source_partial": bool(warnings),
            "warnings": warnings,
            "diagnostics": diagnostics,
            "error": str(exc),
            "exhausted": False,
            "next_cursor": cursor,
        }


def collect_source(session: Session, source_name: str, **kwargs):
    checkpoint_topic = kwargs.pop("checkpoint_topic", None)
    checkpoint_query = kwargs.get("query", "黄金")
    explicit_window = bool(kwargs.pop("explicit_window", True))
    result = asyncio.run(collect_source_async(session, source_name, **kwargs))
    if "until" in kwargs and kwargs["until"] is not None:
        record_collection_checkpoint(
            session,
            source_name=source_name,
            query=checkpoint_query,
            topic=checkpoint_topic,
            until=kwargs["until"],
            result=result,
            explicit_window=explicit_window,
        )
    return result


def active_topics(session: Session) -> list[Topic]:
    return session.scalars(select(Topic).where(Topic.status == "active").order_by(Topic.id)).all()


def _query_fingerprint(query: str) -> str:
    return hashlib.sha256(str(query).strip().encode("utf-8")).hexdigest()


def _checkpoint_scope(*, topic: Topic | None, query: str) -> tuple[str, str]:
    if topic is not None:
        return "topic", topic.slug
    return "query", _query_fingerprint(query)


def resolve_incremental_window(
    session: Session,
    source_name: str,
    *,
    query: str,
    topic: Topic | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    days: int | None = None,
    now: datetime | None = None,
) -> tuple[datetime, datetime, bool]:
    """Resolve explicit or per-source/topic incremental collection windows."""

    current = as_utc(now) or now_utc()
    if since is not None or until is not None or days is not None:
        start, end = resolve_collection_window(since=since, until=until, days=days, now=current)
        return start, end, True
    source = _normalized_source_name(source_name)
    scope_kind, scope_key = _checkpoint_scope(topic=topic, query=query)
    source_row = session.scalar(select(Source).where(Source.name == source))
    checkpoint = None
    if source_row is not None:
        checkpoint = session.scalar(
            select(CollectionCheckpoint).where(
                CollectionCheckpoint.source_id == source_row.id,
                CollectionCheckpoint.scope_kind == scope_kind,
                CollectionCheckpoint.scope_key == scope_key,
                CollectionCheckpoint.query_fingerprint == _query_fingerprint(query),
            )
        )
    last = as_utc(checkpoint.last_successful_until) if checkpoint else None
    start = (last - INCREMENTAL_OVERLAP) if last else current - INCREMENTAL_FIRST_WINDOW
    return start, current, False


def record_collection_checkpoint(
    session: Session,
    *,
    source_name: str,
    query: str,
    topic: Topic | None,
    until: datetime,
    result: dict[str, Any],
    explicit_window: bool,
) -> None:
    """Advance only a complete automatic collection; explicit backfills never move it."""

    if explicit_window or result.get("source_degraded") or result.get("source_partial"):
        return
    source = session.scalar(
        select(Source).where(Source.name == _normalized_source_name(source_name))
    )
    if source is None:
        return
    scope_kind, scope_key = _checkpoint_scope(topic=topic, query=query)
    fingerprint = _query_fingerprint(query)
    row = session.scalar(
        select(CollectionCheckpoint).where(
            CollectionCheckpoint.source_id == source.id,
            CollectionCheckpoint.scope_kind == scope_kind,
            CollectionCheckpoint.scope_key == scope_key,
            CollectionCheckpoint.query_fingerprint == fingerprint,
        )
    )
    now = now_utc()
    if row is None:
        row = CollectionCheckpoint(
            source_id=source.id,
            topic_id=topic.id if topic else None,
            scope_kind=scope_kind,
            scope_key=scope_key,
            query_fingerprint=fingerprint,
            last_successful_until=as_utc(until),
            last_attempt_at=now,
            last_status="healthy",
            updated_at=now,
        )
        session.add(row)
    else:
        row.last_successful_until = as_utc(until)
        row.last_attempt_at = now
        row.last_status = "healthy"
        row.last_error = None
        row.updated_at = now
    session.commit()


def _source_queries(config_dir: str | Path) -> dict[str, dict[str, str]]:
    path = Path(config_dir) / "topics.yaml"
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    result: dict[str, dict[str, str]] = {}
    for item in payload.get("topics", []):
        if not isinstance(item, dict) or not item.get("slug"):
            continue
        queries = item.get("source_queries") or {}
        if isinstance(queries, dict):
            result[str(item["slug"])] = {
                str(name).lower().replace("_", "-"): str(query)
                for name, query in queries.items()
                if query not in (None, "")
            }
    return result


def _source_query_variants(config_dir: str | Path) -> dict[str, dict[str, list[str]]]:
    path = Path(config_dir) / "topics.yaml"
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    result: dict[str, dict[str, list[str]]] = {}
    for item in payload.get("topics", []):
        if not isinstance(item, dict) or not item.get("slug"):
            continue
        configured = item.get("source_query_variants") or {}
        if not isinstance(configured, dict):
            continue
        source_variants: dict[str, list[str]] = {}
        for name, raw_variants in configured.items():
            values = raw_variants if isinstance(raw_variants, list) else [raw_variants]
            variants = list(
                dict.fromkeys(str(value).strip() for value in values if str(value).strip())
            )
            if variants:
                source_variants[str(name).lower().replace("_", "-")] = variants
        if source_variants:
            result[str(item["slug"])] = source_variants
    return result


def _xiaohongshu_discovery_config(config_dir: str | Path) -> dict[str, Any]:
    path = Path(config_dir) / "topics.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    configured = (payload or {}).get("xiaohongshu_discovery") or {}
    if not isinstance(configured, dict):
        configured = {}
    raw_queries = configured.get("queries") or DEFAULT_XIAOHONGSHU_DISCOVERY_QUERIES
    if not isinstance(raw_queries, list | tuple):
        raw_queries = [raw_queries]
    queries = list(dict.fromkeys(str(query).strip() for query in raw_queries if str(query).strip()))
    if not queries:
        queries = list(DEFAULT_XIAOHONGSHU_DISCOVERY_QUERIES)
    return {
        "queries": queries,
        "daily_max_pages": max(1, int(configured.get("daily_max_pages") or 1)),
        "historical_max_pages": max(1, int(configured.get("historical_max_pages") or 20)),
    }


def _normalized_source_name(source_name: str) -> str:
    return source_name.lower().replace("_", "-")


def _backfill_job_specs(
    topics: list[Topic],
    source_names: list[str] | tuple[str, ...],
    *,
    since: datetime,
    until: datetime,
    configured_queries: dict[str, dict[str, str]],
    configured_variants: dict[str, dict[str, list[str]]],
    configured_page_limits: dict[str, dict[str, int]],
    default_page_limit: int,
    xiaohongshu_discovery: dict[str, Any] | None = None,
    configured_source_strategies: dict[str, dict[str, dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    """Return round-robin job specs so a bounded run advances several topics."""

    groups: list[list[dict[str, Any]]] = []
    normalized_sources = {_normalized_source_name(name) for name in source_names}
    if "xiaohongshu" in normalized_sources:
        discovery = xiaohongshu_discovery or {
            "queries": list(DEFAULT_XIAOHONGSHU_DISCOVERY_QUERIES),
            "daily_max_pages": 1,
            "historical_max_pages": 20,
        }
        recent_daily_window = (
            until - since <= timedelta(days=1)
            and until >= now_utc() - timedelta(days=2)
        )
        page_limit_key = "daily_max_pages" if recent_daily_window else "historical_max_pages"
        page_limit = max(1, int(discovery[page_limit_key]))
        groups.append(
            [
                {
                    "key": f"market-wide:xiaohongshu:query:{index:02d}",
                    "topic": None,
                    "source": "xiaohongshu",
                    "query": query,
                    "sort_by": "最新",
                    "initial_cursor": xiaohongshu_strategy_cursor("最新"),
                    "page_limit": page_limit,
                    "sampling_mode": "daily" if recent_daily_window else "historical",
                }
                for index, query in enumerate(discovery["queries"])
            ]
        )
    for topic in topics:
        for source_name in source_names:
            source = _normalized_source_name(source_name)
            if source in {"common-crawl", "xiaohongshu"}:
                continue
            primary_query = configured_queries.get(topic.slug, {}).get(source, topic.name)
            page_limit = configured_page_limits.get(topic.slug, {}).get(source, default_page_limit)
            source_strategy = (configured_source_strategies or {}).get(topic.slug, {}).get(
                source, {}
            )
            if source == "guba" and source_strategy.get("mode") == "hot":
                max_items = max(1, int(source_strategy.get("max_items") or 200))
                groups.append(
                    [
                        {
                            "key": f"{topic.slug}:{source}:hot",
                            "topic": topic,
                            "source": source,
                            "query": primary_query,
                            "sort_by": "热门",
                            "initial_cursor": guba_hot_cursor(
                                primary_query,
                                max_items=max_items,
                            ),
                            "page_limit": min(page_limit, (max_items + 39) // 40),
                        }
                    ]
                )
                continue
            if source == "zhihu":
                market_scope = ZHIHU_MARKET_SCOPES.get(topic.slug)
                if market_scope is None:
                    continue
                local_start = since.astimezone(SHANGHAI).date()
                local_end = (until - timedelta(microseconds=1)).astimezone(SHANGHAI).date()
                session_dates = []
                current_date = local_end
                while current_date >= local_start:
                    if current_date.weekday() < 5:
                        session_dates.append(current_date)
                    current_date -= timedelta(days=1)
                market_jobs = []
                for session_date in session_dates:
                    anchor = datetime(
                        session_date.year,
                        session_date.month,
                        session_date.day,
                        16,
                        tzinfo=SHANGHAI,
                    )
                    query, resolved_date = zhihu_market_question_query(market_scope, anchor)
                    market_jobs.append(
                        {
                            "key": f"{topic.slug}:{source}:session:{resolved_date.isoformat()}",
                            "topic": topic,
                            "source": source,
                            "query": query,
                            "sort_by": None,
                            "initial_cursor": None,
                            "page_limit": 1,
                        }
                    )
                groups.append(market_jobs)
                continue
            groups.append(
                [
                    {
                        "key": f"{topic.slug}:{source}",
                        "topic": topic,
                        "source": source,
                        "query": primary_query,
                        "sort_by": None,
                        "initial_cursor": None,
                        "page_limit": page_limit,
                    }
                ]
            )
    return [
        group[index]
        for index in range(max((len(group) for group in groups), default=0))
        for group in groups
        if index < len(group)
    ]


def _source_page_limits(config_dir: str | Path) -> dict[str, dict[str, int]]:
    path = Path(config_dir) / "topics.yaml"
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    result: dict[str, dict[str, int]] = {}
    for item in payload.get("topics", []):
        if not isinstance(item, dict) or not item.get("slug"):
            continue
        limits = item.get("source_max_pages") or {}
        if isinstance(limits, dict):
            result[str(item["slug"])] = {
                str(name).lower().replace("_", "-"): int(limit)
                for name, limit in limits.items()
                if int(limit) > 0
            }
    return result


def _source_collection_strategies(
    config_dir: str | Path,
) -> dict[str, dict[str, dict[str, Any]]]:
    path = Path(config_dir) / "topics.yaml"
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    result: dict[str, dict[str, dict[str, Any]]] = {}
    for item in payload.get("topics", []):
        if not isinstance(item, dict) or not item.get("slug"):
            continue
        raw_strategies = item.get("source_strategies") or {}
        if not isinstance(raw_strategies, dict):
            continue
        topic_strategies: dict[str, dict[str, Any]] = {}
        for source_name, raw_strategy in raw_strategies.items():
            if not isinstance(raw_strategy, dict):
                continue
            source = _normalized_source_name(str(source_name))
            mode = str(raw_strategy.get("mode") or "").strip().lower()
            if source == "guba" and mode == "hot":
                topic_strategies[source] = {
                    "mode": "hot",
                    "max_items": max(1, int(raw_strategy.get("max_items") or 200)),
                }
        if topic_strategies:
            result[str(item["slug"])] = topic_strategies
    return result


def _collection_task_run_key(
    *,
    resume_key: str | None,
    source: str,
    topic: Topic,
    start: datetime,
    end: datetime,
    explicit_window: bool,
) -> str:
    scope = resume_key or (
        f"explicit:{start.isoformat()}:{end.isoformat()}"
        if explicit_window
        else f"incremental:{end.isoformat()}"
    )
    return hashlib.sha256(f"{scope}:{source}:{topic.slug}".encode()).hexdigest()


def _collection_retry_at(*, error: str | None, partial: bool) -> datetime:
    normalized = str(error or "").casefold()
    if any(
        marker in normalized
        for marker in (
            "auth_required",
            "login is unavailable",
            "login required",
            "登录已过期",
            "安全限制",
            "风控",
            "returned http 403",
        )
    ):
        return now_utc() + timedelta(hours=6)
    if any(
        marker in normalized
        for marker in (
            "429",
            "rate limit",
            "rate_limited",
            "identity-verification",
            "身份验证",
            "请登录",
        )
    ):
        return now_utc() + timedelta(minutes=30)
    return now_utc() + timedelta(minutes=15)


def _source_collection_retry_at(
    session: Session,
    source_id: int,
    *,
    exclude_task_id: int | None = None,
) -> datetime | None:
    """Return the latest active cooldown shared by every window for a source."""

    query = select(func.max(CollectionTask.next_retry_at)).where(
        CollectionTask.source_id == source_id,
        CollectionTask.status.in_(("degraded", "partial")),
        CollectionTask.next_retry_at.is_not(None),
    )
    if exclude_task_id is not None:
        query = query.where(CollectionTask.id != exclude_task_id)
    return as_utc(session.scalar(query))


def _task_result(
    task: CollectionTask,
    *,
    topic: Topic,
    source: str,
    skip_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "topic_id": topic.id,
        "topic_slug": topic.slug,
        "topic_name": topic.name,
        "query": task.query,
        "page_limit": task.page_limit,
        "source": source,
        "pages": 0,
        "items_collected": 0,
        "duplicates": 0,
        "topic_links_added": 0,
        "source_degraded": task.status == "degraded",
        "source_partial": task.status == "partial",
        "warnings": [],
        "collection_skipped": True,
        "skip_reason": skip_reason or task.status,
        "resume": {
            "task_id": task.id,
            "status": task.status,
            "cursor_present": bool(task.cursor),
            "attempts": task.attempts,
            "cumulative_pages": task.pages,
            "cumulative_items_collected": task.items_collected,
            "next_retry_at": task.next_retry_at,
        },
        "error": task.last_error,
    }


def collect_active_topics(
    session: Session,
    *,
    source_names: list[str] | tuple[str, ...],
    since: datetime | None = None,
    until: datetime | None = None,
    days: int | None = None,
    settings: Settings | None = None,
    max_pages: int = 100,
    batch_pages: int | None = None,
    topic_slugs: set[str] | None = None,
    resume_key: str | None = None,
) -> list[dict[str, Any]]:
    """Collect active topics with a durable per-window source/topic checkpoint."""

    settings = settings or get_settings()
    topics = active_topics(session)
    if topic_slugs:
        requested = {slug.strip() for slug in topic_slugs if slug.strip()}
        topics = [topic for topic in topics if topic.slug in requested]
        missing = requested - {topic.slug for topic in topics}
        if missing:
            raise ValueError(f"unknown or inactive topic(s): {', '.join(sorted(missing))}")
    if not topics:
        raise ValueError("no active topics; run retail-tide registry sync first")
    batch_started = time.monotonic()
    logger.info(
        "event=collection_batch_started topics=%d sources=%s explicit_window=%s",
        len(topics),
        ",".join(source_names),
        since is not None or until is not None or days is not None,
    )
    configured_queries = _source_queries(settings.config_dir)
    configured_page_limits = _source_page_limits(settings.config_dir)
    configured_source_strategies = _source_collection_strategies(settings.config_dir)
    results: list[dict[str, Any]] = []
    unavailable_sources: set[str] = set()
    for topic in topics:
        for source_name in source_names:
            normalized_source = source_name.lower().replace("_", "-")
            if normalized_source == "zhihu" and topic.slug not in ZHIHU_MARKET_SCOPES:
                continue
            query = configured_queries.get(topic.slug, {}).get(normalized_source, topic.name)
            page_limit = configured_page_limits.get(topic.slug, {}).get(
                normalized_source, max_pages
            )
            initial_cursor = None
            strategy_fingerprint = query
            source_strategy = configured_source_strategies.get(topic.slug, {}).get(
                normalized_source, {}
            )
            if normalized_source == "guba" and source_strategy.get("mode") == "hot":
                max_items = max(1, int(source_strategy.get("max_items") or 200))
                page_limit = min(page_limit, (max_items + 39) // 40)
                initial_cursor = guba_hot_cursor(query, max_items=max_items)
                strategy_fingerprint = f"{query}\0guba-hot:{max_items}"
            if normalized_source == "common-crawl":
                logger.debug(
                    "event=collection_job_skipped topic=%s source=%s reason=archive_enricher",
                    topic.slug,
                    normalized_source,
                )
                continue
            start, end, explicit_window = resolve_incremental_window(
                session,
                normalized_source,
                query=query,
                topic=topic,
                since=since,
                until=until,
                days=days,
            )
            if normalized_source == "zhihu":
                query, _session_date = zhihu_market_question_query(
                    ZHIHU_MARKET_SCOPES[topic.slug], end
                )
                start, end, explicit_window = resolve_incremental_window(
                    session,
                    normalized_source,
                    query=query,
                    topic=topic,
                    since=since,
                    until=until,
                    days=days,
                )
            source_row = ensure_source(session, normalized_source, settings=settings)
            fingerprint = _query_fingerprint(strategy_fingerprint)
            task = None
            if resume_key is None and not explicit_window:
                # An unfinished automatic task owns its original immutable
                # window. Finish it before opening a newer incremental window.
                task = session.scalar(
                    select(CollectionTask)
                    .where(
                        CollectionTask.source_id == source_row.id,
                        CollectionTask.topic_id == topic.id,
                        CollectionTask.status != "complete",
                    )
                    .order_by(CollectionTask.id)
                )
            run_key = _collection_task_run_key(
                resume_key=resume_key,
                source=normalized_source,
                topic=topic,
                start=start,
                end=end,
                explicit_window=explicit_window,
            )
            if task is None:
                task = session.scalar(
                    select(CollectionTask).where(
                        CollectionTask.source_id == source_row.id,
                        CollectionTask.topic_id == topic.id,
                        CollectionTask.run_key == run_key,
                        CollectionTask.query_fingerprint == fingerprint,
                    )
                )
            if task is None:
                task = CollectionTask(
                    source_id=source_row.id,
                    topic_id=topic.id,
                    run_key=run_key,
                    query=query,
                    query_fingerprint=fingerprint,
                    window_start=start,
                    window_end=end,
                    explicit_window=explicit_window,
                    page_limit=page_limit,
                    cursor=initial_cursor,
                    status="pending",
                    created_at=now_utc(),
                    updated_at=now_utc(),
                )
                session.add(task)
                session.commit()
            else:
                query = task.query
                start = as_utc(task.window_start) or start
                end = as_utc(task.window_end) or end
                explicit_window = task.explicit_window
                task.page_limit = max(task.page_limit, page_limit)
                session.commit()

            if task.status == "complete":
                logger.info(
                    "event=collection_job_skipped topic=%s source=%s reason=checkpoint_complete "
                    "task_id=%d",
                    topic.slug,
                    normalized_source,
                    task.id,
                )
                results.append(
                    _task_result(
                        task,
                        topic=topic,
                        source=normalized_source,
                        skip_reason="checkpoint_complete",
                    )
                )
                continue
            retry_at = as_utc(task.next_retry_at)
            if retry_at is not None and retry_at > now_utc():
                logger.info(
                    "event=collection_job_deferred topic=%s source=%s reason=retry_cooldown "
                    "task_id=%d next_retry_at=%s",
                    topic.slug,
                    normalized_source,
                    task.id,
                    retry_at.isoformat(),
                )
                results.append(
                    _task_result(
                        task,
                        topic=topic,
                        source=normalized_source,
                        skip_reason="retry_cooldown",
                    )
                )
                if task.status in {"degraded", "partial"}:
                    unavailable_sources.add(normalized_source)
                continue
            if normalized_source in unavailable_sources:
                results.append(
                    _task_result(
                        task,
                        topic=topic,
                        source=normalized_source,
                        skip_reason="source_cooldown_after_failure",
                    )
                )
                continue
            source_retry_at = _source_collection_retry_at(
                session,
                source_row.id,
                exclude_task_id=task.id,
            )
            if source_retry_at is not None and source_retry_at > now_utc():
                logger.info(
                    "event=collection_job_deferred topic=%s source=%s "
                    "reason=source_retry_cooldown task_id=%d next_retry_at=%s",
                    topic.slug,
                    normalized_source,
                    task.id,
                    source_retry_at.isoformat(),
                )
                deferred = _task_result(
                    task,
                    topic=topic,
                    source=normalized_source,
                    skip_reason="source_retry_cooldown",
                )
                deferred["source_degraded"] = True
                deferred["error"] = (
                    "source cooldown is active after another collection window; "
                    f"retry after {source_retry_at.isoformat()}"
                )
                deferred["resume"]["next_retry_at"] = source_retry_at
                results.append(deferred)
                unavailable_sources.add(normalized_source)
                continue

            remaining_pages = task.page_limit - task.pages
            if remaining_pages <= 0:
                task.status = "degraded"
                task.last_error = "collection page limit reached before source exhaustion"
                task.next_retry_at = _collection_retry_at(error=task.last_error, partial=False)
                task.updated_at = now_utc()
                session.commit()
                results.append(_task_result(task, topic=topic, source=normalized_source))
                continue
            logger.info(
                "event=collection_job_started topic=%s source=%s query=%r since=%s until=%s "
                "page_limit=%d automatic_window=%s task_id=%d resume_cursor=%s",
                topic.slug,
                normalized_source,
                query,
                start.isoformat(),
                end.isoformat(),
                remaining_pages,
                not explicit_window,
                task.id,
                bool(task.cursor),
            )
            job_started = time.monotonic()
            current_cursor = task.cursor
            task.status = "running"
            task.attempts += 1
            task.next_retry_at = None
            task.updated_at = now_utc()
            session.commit()
            request_pages = remaining_pages
            if batch_pages is not None:
                request_pages = min(
                    remaining_pages,
                    batch_pages,
                    SOURCE_BACKFILL_BATCH_CAP.get(normalized_source, batch_pages),
                )
            result = collect_source(
                session,
                source_name,
                query=query,
                since=start,
                until=end,
                settings=settings,
                max_pages=request_pages,
                topic_id=topic.id,
                start_cursor=current_cursor,
                allow_partial=True,
                checkpoint_topic=topic,
                explicit_window=explicit_window,
            )
            task.pages += int(result.get("pages") or 0)
            task.items_collected += int(result.get("items_collected") or 0)
            task.duplicates += int(result.get("duplicates") or 0)
            task.topic_links_added += int(result.get("topic_links_added") or 0)
            if result.get("source_degraded"):
                task.status = "degraded"
                task.cursor = result.get("next_cursor") or current_cursor
                task.last_error = result.get("error") or "source collection failed"
                task.next_retry_at = _collection_retry_at(error=task.last_error, partial=False)
                unavailable_sources.add(normalized_source)
            elif result.get("source_partial"):
                task.status = "partial"
                task.cursor = result.get("next_cursor") or current_cursor
                task.last_error = "source returned a partial result"
                task.next_retry_at = _collection_retry_at(error=task.last_error, partial=True)
                unavailable_sources.add(normalized_source)
            elif result.get("exhausted"):
                task.status = "complete"
                task.cursor = None
                task.last_error = None
                task.next_retry_at = None
            else:
                task.status = "pending"
                task.cursor = result.get("next_cursor") or current_cursor
                task.last_error = None
                task.next_retry_at = None
            task.updated_at = now_utc()
            session.commit()
            result["resume"] = {
                "task_id": task.id,
                "status": task.status,
                "cursor_present": bool(task.cursor),
                "attempts": task.attempts,
                "cumulative_pages": task.pages,
                "cumulative_items_collected": task.items_collected,
                "next_retry_at": task.next_retry_at,
            }
            log_method = logger.warning if result.get("source_degraded") else logger.info
            log_method(
                "event=collection_job_completed topic=%s source=%s pages=%d inserted=%d "
                "duplicates=%d links=%d degraded=%s partial=%s elapsed_seconds=%.3f error=%r",
                topic.slug,
                normalized_source,
                int(result.get("pages", 0)),
                int(result.get("items_collected", 0)),
                int(result.get("duplicates", 0)),
                int(result.get("topic_links_added", 0)),
                bool(result.get("source_degraded")),
                bool(result.get("source_partial")),
                time.monotonic() - job_started,
                result.get("error"),
            )
            results.append(
                {
                    "topic_id": topic.id,
                    "topic_slug": topic.slug,
                    "topic_name": topic.name,
                    "query": query,
                    "page_limit": page_limit,
                    **result,
                }
            )
    logger.info(
        "event=collection_batch_completed jobs=%d inserted=%d duplicates=%d degraded=%d "
        "elapsed_seconds=%.3f",
        len(results),
        sum(int(row.get("items_collected", 0)) for row in results),
        sum(int(row.get("duplicates", 0)) for row in results),
        sum(bool(row.get("source_degraded")) for row in results),
        time.monotonic() - batch_started,
    )
    return results


async def enrich_common_crawl_async(
    session: Session,
    *,
    since: datetime,
    until: datetime,
    settings: Settings | None = None,
    topic_ids: set[int] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Fill missing/truncated Content bodies from known Common Crawl URLs."""

    settings = settings or get_settings()
    readiness = source_config_status("common-crawl", settings=settings)
    if not readiness["configured"]:
        return {
            "source": "common-crawl",
            "source_degraded": True,
            "error": "source configuration is incomplete; no network request was made",
            "missing_config": readiness["missing"],
            "urls_checked": 0,
            "captures_inserted": 0,
        }
    source_row = ensure_source(session, "common-crawl", settings=settings)
    cc = CommonCrawlSource(
        user_agent=settings.http_user_agent,
        min_interval=settings.request_interval("common-crawl"),
        use_fixture=settings.data_mode == "demo",
    )
    try:
        crawl_ids = await cc.crawl_ids()
        crawl_id = crawl_ids[0]
    except Exception as exc:  # noqa: BLE001 - source health is reported below
        source_row.health_status = "degraded"
        session.commit()
        return {
            "source": "common-crawl",
            "source_degraded": True,
            "error": str(exc),
            "urls_checked": 0,
            "captures_inserted": 0,
        }

    queue_now = now_utc()
    query = (
        select(Content)
        .outerjoin(ArchiveLookupState, ArchiveLookupState.content_id == Content.id)
        .where(
            Content.url.is_not(None),
            Content.source_id != source_row.id,
            Content.published_at >= as_utc(since),
            Content.published_at < as_utc(until),
            or_(
                ArchiveLookupState.id.is_(None),
                ArchiveLookupState.last_crawl_id != crawl_id,
                ~ArchiveLookupState.status.in_({"no_capture", "complete"}),
            ),
            or_(
                ArchiveLookupState.next_retry_at.is_(None),
                ArchiveLookupState.next_retry_at <= queue_now,
            ),
        )
    )
    if topic_ids:
        from ..models import ContentEntity

        query = query.join(ContentEntity, ContentEntity.content_id == Content.id).where(
            ContentEntity.entity_type == "topic",
            ContentEntity.entity_id.in_(topic_ids),
        )
    contents = session.scalars(
        query.distinct().order_by(Content.id).limit(limit or settings.common_crawl_url_limit)
    ).all()
    logger.info(
        "event=archive_queue_started source=common-crawl candidates=%d limit=%d",
        len(contents),
        limit or settings.common_crawl_url_limit,
    )
    checked = captures = bodies_filled = deferred = 0
    errors: list[str] = []
    for queue_index, content in enumerate(contents, start=1):
        if not content.url:
            continue
        state = session.scalar(
            select(ArchiveLookupState).where(ArchiveLookupState.content_id == content.id)
        )
        now = now_utc()
        if state and state.last_crawl_id == crawl_id and state.status in {"no_capture", "complete"}:
            continue
        if (
            state
            and state.next_retry_at
            and as_utc(state.next_retry_at)
            and as_utc(state.next_retry_at) > now
        ):
            deferred += 1
            continue
        checked += 1
        lookup_started = time.monotonic()
        logger.info(
            "event=archive_lookup_started source=common-crawl content_id=%d queue_index=%d "
            "queue_size=%d checked=%d",
            content.id,
            queue_index,
            len(contents),
            checked,
        )
        try:
            capture = await cc.lookup_url(content.url, crawl_id=crawl_id)
            if capture is None:
                if state is None:
                    state = ArchiveLookupState(content_id=content.id, updated_at=now)
                    session.add(state)
                state.last_crawl_id = crawl_id
                state.status = "no_capture"
                state.checked_at = now
                state.next_retry_at = None
                state.last_error = None
                state.updated_at = now
                session.commit()
                logger.info(
                    "event=archive_lookup_completed source=common-crawl content_id=%d "
                    "status=no_capture elapsed_seconds=%.3f",
                    content.id,
                    time.monotonic() - lookup_started,
                )
                continue
            capture_inserted = False
            body_was_filled = False
            existing = session.scalar(
                select(ArchiveSnapshot).where(
                    ArchiveSnapshot.content_id == content.id,
                    ArchiveSnapshot.crawl_id == crawl_id,
                    ArchiveSnapshot.captured_at == capture.captured_at,
                    ArchiveSnapshot.digest == capture.digest,
                )
            )
            if existing is None:
                if captures >= settings.common_crawl_warc_limit:
                    deferred += 1
                    continue
                capture = await cc.fetch_body(capture)
                payload = {
                    "id": f"{canonical_url(content.url)}:{crawl_id}:{capture.captured_at.isoformat()}:{capture.digest}",
                    "canonical_url": canonical_url(content.url),
                    "crawl_id": crawl_id,
                    "captured_at": capture.captured_at.isoformat(),
                    "digest": capture.digest,
                    "filename": capture.filename,
                    "offset": capture.offset,
                    "length": capture.length,
                    "status": capture.status,
                    "mime": capture.mime,
                    "body": capture.body or "",
                    "body_truncated": capture.body_truncated,
                }
                item_id = hashlib.sha256(payload["id"].encode("utf-8")).hexdigest()
                raw_schema = RawObservationSchema(
                    source="common-crawl",
                    source_item_id=item_id,
                    observation_kind="archived_snapshot",
                    published_at=capture.captured_at,
                    observed_at=now,
                    payload=payload,
                )
                raw, _inserted = insert_raw_observation(
                    session, source_row.id, raw_schema, collector_version=settings.collector_version
                )
                existing = ArchiveSnapshot(
                    content_id=content.id,
                    raw_observation_id=raw.id,
                    canonical_url=canonical_url(content.url),
                    crawl_id=crawl_id,
                    captured_at=capture.captured_at,
                    digest=capture.digest,
                    body=capture.body or "",
                    body_truncated=capture.body_truncated,
                    metadata_json=payload,
                    created_at=now,
                )
                session.add(existing)
                captures += 1
                capture_inserted = True
                if capture.body and (
                    not content.body or capture.body_truncated is False and len(content.body) < 80
                ):
                    content.body = capture.body
                    bodies_filled += 1
                    body_was_filled = True
            if state is None:
                state = ArchiveLookupState(content_id=content.id, updated_at=now)
                session.add(state)
            state.last_crawl_id = crawl_id
            state.status = "complete"
            state.checked_at = now
            state.next_retry_at = None
            state.last_error = None
            state.updated_at = now
            session.commit()
            logger.info(
                "event=archive_lookup_completed source=common-crawl content_id=%d "
                "status=complete capture_inserted=%s body_filled=%s elapsed_seconds=%.3f",
                content.id,
                capture_inserted,
                body_was_filled,
                time.monotonic() - lookup_started,
            )
        except Exception as exc:  # noqa: BLE001 - persist the failed URL before pausing
            session.rollback()
            state = session.scalar(
                select(ArchiveLookupState).where(ArchiveLookupState.content_id == content.id)
            )
            if state is None:
                state = ArchiveLookupState(content_id=content.id, updated_at=now)
                session.add(state)
            state.last_crawl_id = crawl_id
            state.status = "retry"
            state.checked_at = now
            state.next_retry_at = now + timedelta(hours=6)
            state.last_error = str(exc)[:1000]
            state.updated_at = now
            session.commit()
            errors.append(f"content {content.id}: {exc}")
            logger.warning(
                "event=archive_lookup_failed source=common-crawl content_id=%d "
                "elapsed_seconds=%.3f error=%r",
                content.id,
                time.monotonic() - lookup_started,
                str(exc),
            )
            # The archive client and crawl index are shared by every queued URL.
            # Once the source degrades, stop this batch and leave the unattempted
            # URLs untouched for a later scheduled retry instead of hammering it.
            deferred += len(contents) - queue_index
            break
    source_row.health_status = "degraded" if errors else "healthy"
    session.commit()
    logger.info(
        "event=archive_queue_completed source=common-crawl candidates=%d checked=%d "
        "captures=%d bodies_filled=%d deferred=%d errors=%d",
        len(contents),
        checked,
        captures,
        bodies_filled,
        deferred,
        len(errors),
    )
    return {
        "source": "common-crawl",
        "source_degraded": bool(errors),
        "urls_checked": checked,
        "captures_inserted": captures,
        "bodies_filled": bodies_filled,
        "deferred": deferred,
        "errors": errors[:20],
        "crawl_id": crawl_id,
    }


def enrich_common_crawl(session: Session, **kwargs) -> dict[str, Any]:
    return asyncio.run(enrich_common_crawl_async(session, **kwargs))


def _save_backfill_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


async def _collect_backfill_attempts_async(
    session: Session,
    attempts: list[dict[str, Any]],
    *,
    since: datetime,
    until: datetime,
    settings: Settings,
    source_concurrency: int,
) -> list[dict[str, Any]]:
    """Run one bounded job per source concurrently with a single DB writer."""

    if not attempts:
        return []
    sources = [str(attempt["spec"]["source"]) for attempt in attempts]
    if len(sources) != len(set(sources)):
        raise ValueError("parallel collection accepts at most one job per source")
    semaphore = asyncio.Semaphore(max(1, source_concurrency))
    writer_lock = asyncio.Lock()

    async def run(attempt: dict[str, Any]) -> dict[str, Any]:
        spec = attempt["spec"]
        topic = spec["topic"]
        async with semaphore:
            return await collect_source_async(
                session,
                spec["source"],
                query=attempt["query"],
                since=since,
                until=until,
                settings=settings,
                max_pages=attempt["max_pages"],
                topic_id=topic.id if topic else None,
                start_cursor=attempt["current_cursor"],
                allow_partial=True,
                _write_lock=writer_lock,
            )

    logger.info(
        "event=parallel_collection_started jobs=%d sources=%s concurrency=%d",
        len(attempts),
        ",".join(sources),
        min(len(attempts), max(1, source_concurrency)),
    )
    started = time.monotonic()
    outcomes = await asyncio.gather(
        *(run(attempt) for attempt in attempts),
        return_exceptions=True,
    )
    results: list[dict[str, Any]] = []
    for attempt, outcome in zip(attempts, outcomes, strict=True):
        if isinstance(outcome, Exception):
            source = str(attempt["spec"]["source"])
            session.rollback()
            source_row = ensure_source(session, source, settings=settings)
            source_row.health_status = "degraded"
            session.commit()
            logger.error(
                "event=parallel_collection_worker_failed source=%s error_type=%s error=%r",
                source,
                type(outcome).__name__,
                str(outcome),
            )
            results.append(
                {
                    "source": source,
                    "pages": 0,
                    "items_collected": 0,
                    "duplicates": 0,
                    "topic_links_added": 0,
                    "source_partial": False,
                    "source_degraded": True,
                    "warnings": [],
                    "error": f"parallel source worker failed: {outcome}",
                    "exhausted": False,
                    "next_cursor": attempt["current_cursor"],
                }
            )
        else:
            results.append(outcome)
    logger.info(
        "event=parallel_collection_completed jobs=%d sources=%s elapsed_seconds=%.3f",
        len(attempts),
        ",".join(sources),
        time.monotonic() - started,
    )
    return results


def _collect_backfill_attempts(
    session: Session,
    attempts: list[dict[str, Any]],
    *,
    since: datetime,
    until: datetime,
    settings: Settings,
    source_concurrency: int,
) -> list[dict[str, Any]]:
    return asyncio.run(
        _collect_backfill_attempts_async(
            session,
            attempts,
            since=since,
            until=until,
            settings=settings,
            source_concurrency=source_concurrency,
        )
    )


def _apply_backfill_job_result(
    job: dict[str, Any],
    spec: dict[str, Any],
    result: dict[str, Any],
    *,
    current_cursor: str | None,
    source_retry_at: dict[str, datetime],
    unavailable_sources: set[str],
    max_retries: int,
) -> None:
    """Apply one collector result to coordinator-owned checkpoint state."""

    source = str(spec["source"])
    job["attempts"] = int(job.get("attempts") or 0) + 1
    if not result.get("source_degraded") and not result.get("source_partial"):
        job["pages"] += int(result.get("pages") or 0)
    job["items_collected"] += int(result.get("items_collected") or 0)
    job["duplicates"] += int(result.get("duplicates") or 0)
    job["topic_links_added"] += int(result.get("topic_links_added") or 0)
    _merge_collection_diagnostics(
        job.setdefault("diagnostics", {}),
        dict(result.get("diagnostics") or {}),
    )
    job["partial"] = bool(result.get("source_partial"))
    job["warnings"] = list(result.get("warnings") or [])
    if result.get("source_degraded"):
        # A failed page must be retried from the same cursor. Early
        # configuration errors do not carry a cursor at all.
        job["cursor"] = result.get("next_cursor") or current_cursor
        job["retries"] += 1
        job["error"] = result.get("error") or "source collection failed"
        job["error_code"] = (result.get("diagnostics") or {}).get("error_code")
        retry_at = _collection_retry_at(
            error=f"{job['error']} {job.get('error_code') or ''}", partial=False
        )
        job["next_retry_at"] = retry_at.isoformat()
        source_retry_at[source] = retry_at
        unavailable_sources.add(source)
    elif result.get("source_partial"):
        # Preserve the strategy cursor so a later bounded run can retry it,
        # while still keeping any useful rows from this attempt.
        availability_pending = bool((result.get("diagnostics") or {}).get("availability_pending"))
        job["cursor"] = current_cursor or spec["initial_cursor"]
        if availability_pending:
            # Wikimedia publishes UTC daily buckets after the period closes,
            # sometimes many hours later. Waiting for provider availability is
            # not a failed collection attempt and must not exhaust the normal
            # transport/parser retry budget.
            job["availability_waits"] = int(job.get("availability_waits") or 0) + 1
            job["error"] = "wikimedia UTC daily bucket is not available yet"
            job["error_code"] = "upstream_data_pending"
        else:
            job["retries"] += 1
            job["error"] = "source returned a partial strategy result"
            job["error_code"] = (result.get("diagnostics") or {}).get("error_code")
        retry_at = _collection_retry_at(error=job["error"], partial=True)
        job["next_retry_at"] = retry_at.isoformat()
        job["done"] = False
        job["completion_reason"] = None
    else:
        job["cursor"] = result.get("next_cursor")
        job["retries"] = 0
        job["error"] = None
        job["error_code"] = None
        job["done"] = bool(result.get("exhausted"))
        job["terminal"] = False
        job["terminal_reason"] = None
        job["completion_reason"] = (
            "window_start_reached"
            if job["done"]
            and bool((job.get("diagnostics") or {}).get("reached_window_start"))
            else "source_exhausted" if job["done"] else None
        )
        job["next_retry_at"] = None
        if not job["done"] and not job.get("cursor"):
            job["error"] = "source did not provide a resumable cursor"
            job["retries"] += 1
            retry_at = _collection_retry_at(error=job["error"], partial=False)
            job["next_retry_at"] = retry_at.isoformat()
            source_retry_at[source] = retry_at
            unavailable_sources.add(source)
    if (
        not job.get("done")
        and not job.get("terminal")
        and int(job.get("retries") or 0) >= max_retries
    ):
        job["terminal"] = True
        job["terminal_reason"] = "retry_limit_exhausted"
        job["next_retry_at"] = None
        job.pop("deferred_reason", None)
        unavailable_sources.add(source)
    job["updated_at"] = now_utc().isoformat()


def backfill_active_topics(
    session: Session,
    *,
    source_names: list[str] | tuple[str, ...],
    since: datetime,
    until: datetime,
    settings: Settings | None = None,
    topic_slugs: set[str] | None = None,
    state_path: str | Path = "retail_tide.backfill.json",
    batch_pages: int = 100,
    default_page_limit: int = 500,
    cooldown_seconds: float = 30,
    max_retries: int = 6,
    max_jobs: int | None = None,
    max_jobs_per_source: int | None = None,
    attempt_sources: set[str] | None = None,
    reset_state: bool = False,
    one_batch_per_job: bool = True,
    source_concurrency: int = 1,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Resumable, source-friendly historical collection in bounded page batches."""

    settings = settings or get_settings()
    state_path = Path(state_path)
    if reset_state and state_path.exists():
        state_path.unlink()
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("version") != 1:
            raise ValueError(f"unsupported backfill checkpoint: {state_path}")
        since = datetime.fromisoformat(str(state["since"]))
        until = datetime.fromisoformat(str(state["until"]))
    else:
        state = {
            "version": 1,
            "since": since.isoformat(),
            "until": until.isoformat(),
            "created_at": now_utc().isoformat(),
            "jobs": {},
        }
    since = as_utc(since) or since
    until = as_utc(until) or until
    if since >= until:
        raise ValueError("backfill window must have since before until")
    if max_jobs is not None and max_jobs < 1:
        raise ValueError("max_jobs must be at least 1")
    if max_jobs_per_source is not None and max_jobs_per_source < 1:
        raise ValueError("max_jobs_per_source must be at least 1")
    if source_concurrency < 1:
        raise ValueError("source_concurrency must be at least 1")
    if attempt_sources is not None:
        attempt_sources = {_normalized_source_name(name) for name in attempt_sources}

    topics = active_topics(session)
    if topic_slugs:
        requested = {slug.strip() for slug in topic_slugs if slug.strip()}
        topics = [topic for topic in topics if topic.slug in requested]
        missing = requested - {topic.slug for topic in topics}
        if missing:
            raise ValueError(f"unknown or inactive topic(s): {', '.join(sorted(missing))}")
    if not topics:
        raise ValueError("no active topics; run retail-tide registry sync first")

    configured_queries = _source_queries(settings.config_dir)
    configured_variants = _source_query_variants(settings.config_dir)
    configured_page_limits = _source_page_limits(settings.config_dir)
    configured_source_strategies = _source_collection_strategies(settings.config_dir)
    xiaohongshu_discovery = _xiaohongshu_discovery_config(settings.config_dir)
    specs = _backfill_job_specs(
        topics,
        source_names,
        since=since,
        until=until,
        configured_queries=configured_queries,
        configured_variants=configured_variants,
        configured_page_limits=configured_page_limits,
        default_page_limit=default_page_limit,
        xiaohongshu_discovery=xiaohongshu_discovery,
        configured_source_strategies=configured_source_strategies,
    )
    jobs: dict[str, Any] = state.setdefault("jobs", {})
    active_job_keys = {spec["key"] for spec in specs}
    retired_jobs: dict[str, Any] = state.setdefault("retired_jobs", {})
    for stale_key in list(jobs):
        if stale_key in active_job_keys:
            continue
        retired_jobs[stale_key] = {
            **jobs.pop(stale_key),
            "retired_at": now_utc().isoformat(),
            "retired_reason": "job no longer belongs to the configured backfill plan",
        }
    for spec in specs:
        topic = spec["topic"]
        key = spec["key"]
        job = jobs.setdefault(
            key,
            {
                "topic_slug": topic.slug if topic else None,
                "topic_name": topic.name if topic else "全市场投资内容",
                "source": spec["source"],
                "transport_revision": SOURCE_TRANSPORT_REVISIONS.get(spec["source"]),
                "query": spec["query"],
                "sort_by": spec["sort_by"],
                "sampling_mode": spec.get("sampling_mode"),
                "page_limit": spec["page_limit"],
                "pages": 0,
                "attempts": 0,
                "items_collected": 0,
                "duplicates": 0,
                "topic_links_added": 0,
                "cursor": spec["initial_cursor"],
                "retries": 0,
                "done": False,
                "terminal": False,
                "terminal_reason": None,
                "completion_reason": None,
                "next_retry_at": None,
                "error": None,
                "error_code": None,
                "warnings": [],
                "partial": False,
                "diagnostics": {},
            },
        )
        strategy_changed = (
            job.get("query") != spec["query"] or job.get("sort_by") != spec["sort_by"]
        )
        if strategy_changed:
            if job.get("cursor") and int(job.get("pages") or 0) > 0:
                raise ValueError(
                    f"strategy changed for {key} while a cursor is active; "
                    "rerun with a reset backfill checkpoint"
                )
            job.update(
                {
                    "query": spec["query"],
                    "sort_by": spec["sort_by"],
                    "sampling_mode": spec.get("sampling_mode"),
                    "pages": 0,
                    "attempts": 0,
                    "items_collected": 0,
                    "duplicates": 0,
                    "topic_links_added": 0,
                    "cursor": spec["initial_cursor"],
                    "retries": 0,
                    "done": False,
                    "terminal": False,
                    "terminal_reason": None,
                    "completion_reason": None,
                    "next_retry_at": None,
                    "error": None,
                    "error_code": None,
                    "warnings": [],
                    "partial": False,
                    "diagnostics": {},
                }
            )
        transport_revision = SOURCE_TRANSPORT_REVISIONS.get(spec["source"])
        if (
            transport_revision
            and job.get("transport_revision") != transport_revision
            and not job.get("done")
        ):
            # A new transport can remove the condition that created an old
            # source-wide cooldown (for example HTML verification replaced by
            # Eastmoney's own JSON list, or MCP search replaced by Spider
            # pagination). Keep collected rows/pages/cursors, but permit one
            # immediate attempt under the new revision. Failures recorded by
            # this revision retain their normal cooldown on subsequent runs.
            job.update(
                {
                    "transport_revision": transport_revision,
                    "retries": 0,
                    "terminal": False,
                    "terminal_reason": None,
                    "completion_reason": None,
                    "next_retry_at": None,
                    "error": None,
                    "error_code": None,
                    "warnings": [],
                    "partial": False,
                }
            )
            job.pop("deferred_reason", None)
        if spec["source"] == "xiaohongshu":
            # Xiaohongshu search is a ranked discovery sample, not an
            # exhaustive chronological API. Honor the configured page budget
            # exactly, including intentional reductions of an existing plan.
            job["page_limit"] = int(spec["page_limit"])
            job["sampling_mode"] = spec.get("sampling_mode")
        else:
            job["page_limit"] = max(
                int(job.get("page_limit") or 0), int(spec["page_limit"])
            )
        job.setdefault("attempts", int(job.get("pages") or 0))
        job.setdefault("terminal", False)
        job.setdefault("terminal_reason", None)
        job.setdefault("completion_reason", None)
        job.setdefault("next_retry_at", None)
        job.setdefault("diagnostics", {})
        if (
            not job.get("done")
            and not job.get("terminal")
            and int(job.get("retries") or 0) >= max_retries
        ):
            job["terminal"] = True
            job["terminal_reason"] = "retry_limit_exhausted"
            job["next_retry_at"] = None
            job.pop("deferred_reason", None)
        # Checkpoints written by the first strategy implementation counted a
        # warning/partial response as its only page. Restore that budget so the
        # same strategy can actually be retried without discarding useful rows.
        if job.get("partial") and not job.get("done") and job.get("cursor"):
            job["pages"] = min(
                int(job.get("pages") or 0),
                max(int(job["page_limit"]) - 1, 0),
            )

    # A transport/access failure is normally shared by every strategy that uses
    # the same upstream.  Older checkpoints only stored the retry timestamp on
    # the attempted job, so derive a source-wide cooldown from those job fields
    # as well as maintaining it for failures observed in this run. Partial
    # results remain job-local: one bad keyword must not stall every other XHS
    # strategy, and the durable source limiter still spaces their requests.
    source_retry_at: dict[str, datetime] = {}
    partial_retry_at: dict[str, list[datetime]] = {}
    checkpoint_now = now_utc()
    for checkpoint_job in jobs.values():
        if checkpoint_job.get("done") or checkpoint_job.get("terminal"):
            continue
        retry_value = checkpoint_job.get("next_retry_at")
        source_value = checkpoint_job.get("source")
        if not retry_value or not source_value:
            continue
        retry_at = as_utc(datetime.fromisoformat(str(retry_value)))
        source_name = _normalized_source_name(str(source_value))
        if retry_at is None or retry_at <= checkpoint_now:
            continue
        if checkpoint_job.get("partial"):
            partial_retry_at.setdefault(source_name, []).append(retry_at)
            continue
        source_retry_at[source_name] = max(
            retry_at,
            source_retry_at.get(source_name, retry_at),
        )
    for source_name, retry_values in partial_retry_at.items():
        if len(retry_values) >= PARTIAL_SOURCE_CIRCUIT_BREAKER:
            source_retry_at[source_name] = max(retry_values)

    # Scheduled collection windows and manual history backfills share source
    # pressure. Honor the durable database cooldown before advancing a JSON
    # checkpoint so one workflow cannot invalidate another workflow's wait.
    for source_name in {_normalized_source_name(str(row["source"])) for row in jobs.values()}:
        source_row = session.scalar(select(Source).where(Source.name == source_name))
        if source_row is None:
            continue
        retry_at = _source_collection_retry_at(session, source_row.id)
        if retry_at is not None and retry_at > checkpoint_now:
            source_retry_at[source_name] = max(
                retry_at,
                source_retry_at.get(source_name, retry_at),
            )

    unavailable_sources: set[str] = set()
    attempted_jobs = 0
    attempted_jobs_by_source: dict[str, int] = {}
    parallel_mode = (
        source_concurrency > 1
        and one_batch_per_job
        and max_jobs_per_source == 1
    )
    parallel_attempts: list[dict[str, Any]] = []
    next_job_key = state.get("next_job_key")
    start_index = next(
        (index for index, spec in enumerate(specs) if spec["key"] == next_job_key),
        0,
    )
    scheduled_specs = [*specs[start_index:], *specs[:start_index]]
    spec_indexes = {spec["key"]: index for index, spec in enumerate(specs)}
    for spec in scheduled_specs:
        topic = spec["topic"]
        key = spec["key"]
        job = jobs[key]
        source = spec["source"]
        if attempt_sources is not None and source not in attempt_sources:
            continue
        if job.get("done") or job.get("terminal"):
            job.pop("deferred_reason", None)
            continue
        source_cooldown = source_retry_at.get(source)
        if source_cooldown is not None and source_cooldown > now_utc():
            job["deferred_reason"] = f"source cooldown until {source_cooldown.isoformat()}"
            continue
        if source in unavailable_sources:
            # Only the attempted strategy is degraded. Keep untouched strategies
            # clean and resumable so the checkpoint does not misreport hundreds
            # of network requests that were never made.
            job["deferred_reason"] = "source reached its retry limit earlier in this bounded run"
            job["updated_at"] = now_utc().isoformat()
            _save_backfill_state(state_path, state)
            continue
        if job.get("next_retry_at"):
            retry_at = as_utc(datetime.fromisoformat(str(job["next_retry_at"])))
            if retry_at is not None and retry_at > now_utc():
                job["deferred_reason"] = f"source cooldown until {retry_at.isoformat()}"
                continue
        # A previous run may have marked this job deferred because another job
        # from the same source failed. Once that source-wide cooldown has
        # elapsed, clear the stale marker even when this job is skipped by the
        # current round's per-source budget.
        job.pop("deferred_reason", None)
        if max_jobs is not None and attempted_jobs >= max_jobs:
            continue
        if (
            max_jobs_per_source is not None
            and attempted_jobs_by_source.get(source, 0) >= max_jobs_per_source
        ):
            continue
        attempted_jobs += 1
        attempted_jobs_by_source[source] = attempted_jobs_by_source.get(source, 0) + 1
        current_index = spec_indexes[key]
        if specs:
            state["next_job_key"] = specs[(current_index + 1) % len(specs)]["key"]
        while not job.get("done"):
            remaining = int(job["page_limit"]) - int(job["pages"])
            if remaining <= 0:
                if source == "xiaohongshu":
                    if job.get("sampling_mode") == "daily":
                        job["done"] = True
                        job["error"] = None
                        job["partial"] = False
                        job["terminal"] = False
                        job["terminal_reason"] = "configured_daily_sample_complete"
                        job["completion_reason"] = "configured_daily_sample_complete"
                        job["next_retry_at"] = None
                        job["updated_at"] = now_utc().isoformat()
                        _save_backfill_state(state_path, state)
                        break
                    reached_start = bool(
                        (job.get("diagnostics") or {}).get("reached_window_start")
                    )
                    job["done"] = reached_start
                    job["error"] = (
                        None
                        if reached_start
                        else "historical sampling budget reached before window start"
                    )
                    job["partial"] = not reached_start
                    job["terminal"] = not reached_start
                    job["terminal_reason"] = (
                        "window_start_reached" if reached_start else "partial_budget_exhausted"
                    )
                    job["completion_reason"] = (
                        "window_start_reached" if reached_start else None
                    )
                    job["next_retry_at"] = None
                    job["updated_at"] = now_utc().isoformat()
                    _save_backfill_state(state_path, state)
                    break
                job["error"] = "historical page limit reached before source exhaustion"
                job["terminal"] = True
                job["terminal_reason"] = "page_limit_reached"
                job["updated_at"] = now_utc().isoformat()
                break
            current_cursor = job.get("cursor")
            request_pages = min(
                batch_pages,
                remaining,
                SOURCE_BACKFILL_BATCH_CAP.get(source, batch_pages),
            )
            if parallel_mode:
                parallel_attempts.append(
                    {
                        "spec": spec,
                        "query": job["query"],
                        "current_cursor": current_cursor,
                        "max_pages": request_pages,
                    }
                )
                break
            result = collect_source(
                session,
                source,
                query=job["query"],
                since=since,
                until=until,
                settings=settings,
                max_pages=request_pages,
                topic_id=topic.id if topic else None,
                start_cursor=current_cursor,
                allow_partial=True,
            )
            _apply_backfill_job_result(
                job,
                spec,
                result,
                current_cursor=current_cursor,
                source_retry_at=source_retry_at,
                unavailable_sources=unavailable_sources,
                max_retries=max_retries,
            )
            _save_backfill_state(state_path, state)
            if progress:
                progress({**job, "batch": result})

            if job.get("done"):
                break
            if job.get("error"):
                # A degraded source already carries a durable retry timestamp.
                # Do not let drain mode loop inside the same invocation and
                # defeat that cooldown (especially for expired sessions or
                # upstream rate limits). Partial strategy results stay local to
                # their query; hard failures stop every remaining job that uses
                # the same source for this bounded run.
                if not job.get("partial"):
                    unavailable_sources.add(source)
                break
            if one_batch_per_job:
                if job.get("error"):
                    unavailable_sources.add(source)
                # Fair scheduling: persist one bounded page batch and move to
                # the next source/topic. A later invocation resumes the cursor.
                break
            retry_multiplier = min(max(int(job["retries"]), 1), 4)
            time.sleep(max(0.0, cooldown_seconds) * retry_multiplier)
        _save_backfill_state(state_path, state)

    if parallel_attempts:
        parallel_results = _collect_backfill_attempts(
            session,
            parallel_attempts,
            since=since,
            until=until,
            settings=settings,
            source_concurrency=source_concurrency,
        )
        for attempt, result in zip(parallel_attempts, parallel_results, strict=True):
            spec = attempt["spec"]
            job = jobs[spec["key"]]
            _apply_backfill_job_result(
                job,
                spec,
                result,
                current_cursor=attempt["current_cursor"],
                source_retry_at=source_retry_at,
                unavailable_sources=unavailable_sources,
                max_retries=max_retries,
            )
            record_collection_checkpoint(
                session,
                source_name=spec["source"],
                query=attempt["query"],
                topic=None,
                until=until,
                result=result,
                explicit_window=True,
            )
            _save_backfill_state(state_path, state)
            if progress:
                progress({**job, "batch": result})

    rows = [jobs[spec["key"]] for spec in specs]
    query_coverage = [
        {
            "query": row.get("query"),
            "sampling_mode": row.get("sampling_mode"),
            "pages": int(row.get("pages") or 0),
            "page_limit": int(row.get("page_limit") or 0),
            "daily_samples": dict(
                (row.get("diagnostics") or {}).get("retained_publication_days") or {}
            ),
            "reached_window_start": bool(
                (row.get("diagnostics") or {}).get("reached_window_start")
            ),
            "completion_reason": row.get("completion_reason"),
            "terminal_reason": row.get("terminal_reason"),
            "partial": bool(row.get("partial")),
        }
        for row in rows
        if row.get("source") == "xiaohongshu"
    ]
    return {
        "since": since,
        "until": until,
        "state_file": str(state_path),
        "completed": all(row.get("done") or row.get("terminal") for row in rows),
        "coverage_complete": all(row.get("done") and not row.get("partial") for row in rows),
        "attempted_jobs": attempted_jobs,
        "pending_jobs": sum(not (row.get("done") or row.get("terminal")) for row in rows),
        "terminal_jobs": [row for row in rows if row.get("terminal")],
        "deferred_jobs": [
            row
            for row in rows
            if row.get("deferred_reason") and not (row.get("done") or row.get("terminal"))
        ],
        "degraded": [row for row in rows if row.get("error") and not row.get("partial")],
        "partial": [row for row in rows if row.get("partial")],
        "query_coverage": query_coverage,
        "jobs": rows,
    }


def run_core_pipeline(
    session: Session,
    *,
    limit: int = 5000,
    bucket_sizes: tuple[str, ...] = ("1h", "1d"),
    settings: Settings | None = None,
    analysis_since: datetime | None = None,
    analysis_until: datetime | None = None,
) -> dict[str, Any]:
    settings = settings or get_settings()
    finish_timer = timer("job_duration_seconds")
    pipeline_started = time.monotonic()
    logger.info(
        "event=pipeline_started limit=%d llm_provider=%s analysis_model=%s",
        limit,
        settings.llm_provider,
        settings.analysis_model,
    )
    stage_started = time.monotonic()
    normalized = normalize_pending(session, limit=limit, settings=settings)
    logger.info(
        "event=pipeline_stage_completed stage=normalize count=%d elapsed_seconds=%.3f",
        normalized,
        time.monotonic() - stage_started,
    )
    stage_started = time.monotonic()
    resolved = resolve_pending_entities(session, limit=limit)
    logger.info(
        "event=pipeline_stage_completed stage=resolve count=%d elapsed_seconds=%.3f",
        resolved,
        time.monotonic() - stage_started,
    )
    result: dict[str, Any] = {"normalized": normalized, "resolved": resolved}
    llm_status = llm_config_status(settings)
    stage_started = time.monotonic()
    if settings.llm_provider in {"openai", "openai-compatible"} and not llm_status["configured"]:
        # Collection and normalization are durable even when the optional LLM
        # credentials are absent. The analysis task table remains pending for
        # a later retry after setup is completed.
        result["analyzed"] = 0
        result["analysis_pending"] = enqueue_pending_analysis_tasks(
            session,
            settings=settings,
            limit=limit,
            since=analysis_since,
            until=analysis_until,
        )
        result["analysis_deferred"] = {
            "reason": "llm configuration is incomplete",
            "missing": llm_status["missing"],
        }
    else:
        result["analyzed"] = analyze_pending(
            session,
            limit=limit,
            settings=settings,
            since=analysis_since,
            until=analysis_until,
        )
    result["analysis_tasks"] = analysis_task_summary(
        session,
        model=settings.analysis_model,
        since=analysis_since,
        until=analysis_until,
    )
    logger.info(
        "event=pipeline_stage_completed stage=analysis completed=%d failed=%d pending=%d "
        "untracked=%d retry_ready=%d retry_deferred=%d elapsed_seconds=%.3f",
        int(result["analyzed"]),
        int(result["analysis_tasks"]["failed"]),
        int(result["analysis_tasks"]["pending"]),
        int(result["analysis_tasks"]["untracked"]),
        int(result["analysis_tasks"]["retry_ready"]),
        int(result["analysis_tasks"]["retry_deferred"]),
        time.monotonic() - stage_started,
    )
    stage_started = time.monotonic()
    result["trend_signals"] = aggregate_trend_signals(session, settings=settings)
    logger.info(
        "event=pipeline_stage_completed stage=trends count=%d elapsed_seconds=%.3f",
        result["trend_signals"],
        time.monotonic() - stage_started,
    )
    for bucket in bucket_sizes:
        stage_started = time.monotonic()
        result[f"metrics_{bucket}"] = aggregate_metrics(
            session, bucket_size=bucket, settings=settings
        )
        logger.info(
            "event=pipeline_stage_completed stage=metrics bucket=%s count=%d elapsed_seconds=%.3f",
            bucket,
            result[f"metrics_{bucket}"],
            time.monotonic() - stage_started,
        )
    stage_started = time.monotonic()
    result["events"] = detect_events(session, settings=settings)
    logger.info(
        "event=pipeline_stage_completed stage=events count=%d elapsed_seconds=%.3f",
        result["events"],
        time.monotonic() - stage_started,
    )
    stage_started = time.monotonic()
    result["returns"] = evaluate_events(session, settings=settings)
    logger.info(
        "event=pipeline_stage_completed stage=returns count=%d elapsed_seconds=%.3f",
        result["returns"],
        time.monotonic() - stage_started,
    )
    stage_started = time.monotonic()
    result["quality"] = refresh_source_quality(session)
    logger.info(
        "event=pipeline_stage_completed stage=quality count=%d elapsed_seconds=%.3f",
        result["quality"],
        time.monotonic() - stage_started,
    )
    duration = finish_timer()
    logger.info(
        "event=pipeline_completed normalized=%d resolved=%d analyzed=%d events=%d "
        "elapsed_seconds=%.3f",
        normalized,
        resolved,
        int(result["analyzed"]),
        int(result["events"]),
        max(duration, time.monotonic() - pipeline_started),
    )
    return result
