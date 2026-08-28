from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import and_, delete, exists, select, update
from sqlalchemy.orm import Session

from ..config import Settings, get_settings
from ..db import make_engine, session_factory
from ..models import (
    AnalysisTask,
    ArchiveLookupState,
    ArchiveSnapshot,
    Content,
    ContentAnalysis,
    ContentAnalysisReview,
    ContentCluster,
    ContentClusterMember,
    ContentEntity,
    DiffusionEvent,
    EventMetricLink,
    EventReturn,
    MetricSignal,
    PlatformMetric,
    SignalEvent,
    Source,
    Topic,
)
from ..models import RawObservation as StoredRawObservation
from ..schemas import RawObservation
from ..time import as_utc
from .normalize import insert_raw_observation, link_raw_observation_topic

_VERIFIABLE_SOURCE_TIME = {
    "guba": {
        "timestamp_semantics": "published",
        "source_timestamp_field": "post_publish_time",
        "source_timezone": "Asia/Shanghai",
    },
    "taoguba": {
        "timestamp_semantics": "published",
        "source_timestamp_field": "postDate",
        "source_timezone": "Asia/Shanghai_or_unix_epoch",
    },
    "xiaohongshu": {
        "timestamp_semantics": "published",
        "source_timestamp_field": "note.time",
        "source_timezone": "unix_epoch_utc",
    },
}


def remove_misnormalized_zhihu_snapshots(session: Session) -> dict[str, int]:
    """Remove only invalid derived Content while preserving immutable raw snapshots."""

    content_ids = list(
        session.scalars(
            select(Content.id)
            .join(
                StoredRawObservation,
                and_(
                    StoredRawObservation.source_id == Content.source_id,
                    StoredRawObservation.source_item_id == Content.source_item_id,
                ),
            )
            .where(StoredRawObservation.observation_kind == "zhihu_answer_snapshot")
            .distinct()
        )
    )
    if not content_ids:
        return {"content": 0, "analyses": 0, "tasks": 0, "entities": 0, "clusters": 0}

    analysis_ids = select(ContentAnalysis.id).where(ContentAnalysis.content_id.in_(content_ids))
    session.execute(
        delete(ContentAnalysisReview).where(
            ContentAnalysisReview.content_analysis_id.in_(analysis_ids)
        )
    )
    analyses = session.execute(
        delete(ContentAnalysis).where(ContentAnalysis.content_id.in_(content_ids))
    ).rowcount
    tasks = session.execute(
        delete(AnalysisTask).where(AnalysisTask.content_id.in_(content_ids))
    ).rowcount
    entities = session.execute(
        delete(ContentEntity).where(ContentEntity.content_id.in_(content_ids))
    ).rowcount
    session.execute(
        delete(ArchiveSnapshot).where(ArchiveSnapshot.content_id.in_(content_ids))
    )
    session.execute(
        delete(ArchiveLookupState).where(ArchiveLookupState.content_id.in_(content_ids))
    )
    session.execute(
        update(Content)
        .where(Content.parent_content_id.in_(content_ids))
        .values(parent_content_id=None)
    )
    cluster_members = session.execute(
        delete(ContentClusterMember).where(ContentClusterMember.content_id.in_(content_ids))
    ).rowcount
    session.execute(delete(Content).where(Content.id.in_(content_ids)))
    session.execute(
        delete(ContentCluster).where(
            ~exists().where(ContentClusterMember.cluster_id == ContentCluster.id)
        )
    )
    session.commit()
    return {
        "content": len(content_ids),
        "analyses": int(analyses or 0),
        "tasks": int(tasks or 0),
        "entities": int(entities or 0),
        "clusters": int(cluster_members or 0),
    }


def reset_metric_event_derivatives(session: Session) -> dict[str, int]:
    """Clear rebuildable metric/event rows so removed content cannot leave stale signals."""

    counts = {}
    for name, model in (
        ("event_returns", EventReturn),
        ("event_metric_links", EventMetricLink),
        ("signal_events", SignalEvent),
        ("diffusion_events", DiffusionEvent),
        ("metric_signals", MetricSignal),
        ("platform_metrics", PlatformMetric),
    ):
        result = session.execute(delete(model))
        counts[name] = int(result.rowcount or 0)
    session.commit()
    return counts


def _sqlite_url(value: str | Path) -> str:
    text = str(value)
    if "://" in text:
        return text
    return f"sqlite:///{Path(text).resolve()}"


def import_verified_raw_history(
    target: Session,
    *,
    source_database: str | Path,
    since: datetime,
    until: datetime,
    settings: Settings | None = None,
    source_names: set[str] | None = None,
) -> dict[str, Any]:
    """Import only source timestamps whose adapter contract means publication.

    This intentionally excludes Zhihu search rows because its EditTime is not
    proof of creation time. Analyses and derived values are never copied.
    """

    settings = settings or get_settings()
    since = as_utc(since) or since
    until = as_utc(until) or until
    if since >= until:
        raise ValueError("history import requires since before until")
    allowed = set(_VERIFIABLE_SOURCE_TIME)
    if source_names:
        unknown = source_names - allowed
        if unknown:
            raise ValueError(f"source time cannot be certified: {', '.join(sorted(unknown))}")
        allowed &= source_names

    source_engine = make_engine(url=_sqlite_url(source_database))
    source_session = session_factory(source_engine)()
    try:
        source_rows = {
            row.id: row.name for row in source_session.scalars(select(Source)).all()
        }
        topic_rows = {
            row.id: row.slug for row in source_session.scalars(select(Topic)).all()
        }
        raw_rows = source_session.scalars(
            select(StoredRawObservation).order_by(StoredRawObservation.id)
        ).all()
        latest: dict[tuple[int, str], StoredRawObservation] = {}
        topic_links: dict[tuple[int, str], dict[str, str]] = defaultdict(dict)
        for raw in raw_rows:
            source_name = source_rows.get(raw.source_id)
            if source_name not in allowed:
                continue
            published_at = as_utc(raw.published_at)
            if published_at is None or not since <= published_at <= until:
                continue
            key = (raw.source_id, raw.source_item_id)
            current = latest.get(key)
            truncated = (raw.payload or {}).get("body_truncated") is True
            current_truncated = (
                (current.payload or {}).get("body_truncated") is True if current else True
            )
            if current is None or not truncated or current_truncated:
                latest[key] = raw
            for match in raw.topic_matches:
                slug = topic_rows.get(match.topic_id)
                if slug:
                    topic_links[key][slug] = match.collection_query

        target_sources = {
            row.name: row for row in target.scalars(select(Source)).all()
        }
        target_topics = {
            row.slug: row for row in target.scalars(select(Topic)).all()
        }
        inserted = duplicates = links_added = 0
        by_source: dict[str, dict[str, int]] = defaultdict(
            lambda: {"items": 0, "inserted": 0, "duplicates": 0, "topic_links_added": 0}
        )
        for index, (key, raw) in enumerate(latest.items(), start=1):
            source_name = source_rows[raw.source_id]
            target_source = target_sources.get(source_name)
            if target_source is None:
                raise ValueError(f"target registry has no source {source_name}")
            payload = dict(raw.payload or {})
            payload.update(_VERIFIABLE_SOURCE_TIME[source_name])
            payload["timestamp_verification_method"] = "immutable_source_adapter_contract_v1"
            payload["source_raw_observation_id"] = raw.id
            published_at = as_utc(raw.published_at)
            assert published_at is not None
            payload["published_at"] = published_at.isoformat()
            observation = RawObservation(
                source=source_name,
                source_item_id=raw.source_item_id,
                observation_kind=raw.observation_kind,
                published_at=published_at,
                observed_at=as_utc(raw.observed_at) or published_at,
                payload=payload,
            )
            stored, did_insert = insert_raw_observation(
                target,
                target_source.id,
                observation,
                collector_version="timestamp-recertification-v1",
                collected_at=as_utc(raw.collected_at),
            )
            inserted += int(did_insert)
            duplicates += int(not did_insert)
            source_stats = by_source[source_name]
            source_stats["items"] += 1
            source_stats["inserted"] += int(did_insert)
            source_stats["duplicates"] += int(not did_insert)
            for slug, query in topic_links.get(key, {}).items():
                topic = target_topics.get(slug)
                if topic is None:
                    continue
                linked = link_raw_observation_topic(
                    target,
                    stored,
                    topic_id=topic.id,
                    collection_query=query,
                )
                links_added += int(linked)
                source_stats["topic_links_added"] += int(linked)
            if index % 500 == 0:
                target.commit()
        target.commit()
        return {
            "since": since,
            "until": until,
            "candidate_items": len(latest),
            "inserted": inserted,
            "duplicates": duplicates,
            "topic_links_added": links_added,
            "excluded_sources": ["zhihu"],
            "sources": dict(sorted(by_source.items())),
        }
    finally:
        source_session.close()
        source_engine.dispose()
