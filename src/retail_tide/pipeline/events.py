from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import Settings, get_settings
from ..models import (
    DiffusionEvent,
    EventMetricLink,
    MetricSignal,
    PlatformMetric,
    SignalEvent,
)
from ..time import bucket_delta, now_utc

EVENT_METRIC_MAP = {
    "post_count": "attention_spike",
    "unique_author_count": "attention_spike",
    "novice_ratio": "novice_spike",
    "fomo_ratio": "fomo_spike",
    "panic_ratio": "panic_spike",
    "buy_intent_ratio": "buy_intent_spike",
}


def _qualifying(signal: MetricSignal) -> bool:
    return (
        signal.robust_z is not None
        and signal.percentile is not None
        and signal.robust_z >= 3
        and signal.percentile >= 0.95
    )


def detect_events(
    session: Session,
    *,
    settings: Settings | None = None,
    rule_version: str | None = None,
    event_date=None,
) -> int:
    settings = settings or get_settings()
    rule_version = rule_version or settings.event_rule_version
    query = (
        select(MetricSignal)
        .join(PlatformMetric, MetricSignal.platform_metric_id == PlatformMetric.id)
        .where(MetricSignal.metric_version == settings.metric_version)
    )
    if event_date is not None:
        from datetime import datetime, time, timezone

        query = query.where(
            PlatformMetric.bucket_at <= datetime.combine(event_date, time.max, tzinfo=timezone.utc)
        )
    signals = session.scalars(query).all()
    metric_by_id = {metric.id: metric for metric in session.scalars(select(PlatformMetric)).all()}
    grouped: dict[tuple, list[tuple[MetricSignal, PlatformMetric]]] = defaultdict(list)
    for signal in signals:
        if not _qualifying(signal) or signal.metric_name not in EVENT_METRIC_MAP:
            continue
        metric = metric_by_id.get(signal.platform_metric_id)
        if metric is None:
            continue
        event_type = EVENT_METRIC_MAP[signal.metric_name]
        grouped[
            (event_type, metric.source_id, metric.topic_id, metric.asset_id, metric.bucket_size)
        ].append((signal, metric))

    discovered: list[SignalEvent] = []
    for (event_type, source_id, topic_id, asset_id, bucket_size), rows in grouped.items():
        rows.sort(key=lambda pair: pair[1].bucket_at)
        cluster: list[tuple[MetricSignal, PlatformMetric]] = []
        max_gap = bucket_delta(bucket_size)
        for pair in rows + [None]:
            if pair is not None and (
                not cluster or pair[1].bucket_at <= cluster[-1][1].bucket_at + max_gap
            ):
                cluster.append(pair)
                continue
            if cluster:
                discovered.append(
                    _upsert_event(
                        session,
                        cluster,
                        event_type=event_type,
                        source_id=source_id,
                        topic_id=topic_id,
                        asset_id=asset_id,
                        bucket_size=bucket_size,
                        rule_version=rule_version,
                    )
                )
            cluster = [pair] if pair is not None else []
    session.commit()
    detect_diffusion(session, settings=settings, rule_version=rule_version)
    return len(discovered)


def _upsert_event(
    session: Session,
    cluster: list[tuple[MetricSignal, PlatformMetric]],
    *,
    event_type: str,
    source_id: int,
    topic_id: int | None,
    asset_id: int | None,
    bucket_size: str,
    rule_version: str,
) -> SignalEvent:
    start_metric = cluster[0][1]
    end_metric = cluster[-1][1]
    peak_signal, peak_metric = max(
        cluster,
        key=lambda pair: (
            pair[0].robust_z if pair[0].robust_z is not None else float("-inf"),
            pair[0].percentile if pair[0].percentile is not None else float("-inf"),
        ),
    )
    existing = session.scalar(
        select(SignalEvent).where(
            SignalEvent.source_id == source_id,
            SignalEvent.topic_id.is_(None)
            if topic_id is None
            else SignalEvent.topic_id == topic_id,
            SignalEvent.asset_id.is_(None)
            if asset_id is None
            else SignalEvent.asset_id == asset_id,
            SignalEvent.event_type == event_type,
            SignalEvent.started_at == start_metric.bucket_at,
            SignalEvent.rule_version == rule_version,
        )
    )
    values = {
        "source_id": source_id,
        "topic_id": topic_id,
        "asset_id": asset_id,
        "event_type": event_type,
        "started_at": start_metric.bucket_at,
        "peaked_at": peak_metric.bucket_at,
        "ended_at": end_metric.bucket_at,
        "peak_value": peak_signal.raw_value,
        "peak_zscore": peak_signal.robust_z,
        "peak_percentile": peak_signal.percentile,
        "rule_version": rule_version,
        "status": "discovered",
        "trigger_metric_id": peak_metric.id,
    }
    if existing is None:
        existing = SignalEvent(created_at=now_utc(), **values)
        session.add(existing)
        session.flush()
    else:
        for key, value in values.items():
            setattr(existing, key, value)
    for signal, _metric in cluster:
        link = session.scalar(
            select(EventMetricLink).where(
                EventMetricLink.event_id == existing.id,
                EventMetricLink.metric_signal_id == signal.id,
            )
        )
        if link is None:
            session.add(EventMetricLink(event_id=existing.id, metric_signal_id=signal.id))
    return existing


def detect_diffusion(
    session: Session, *, settings: Settings | None = None, rule_version: str = "diffusion-v1"
) -> int:
    settings = settings or get_settings()
    events = session.scalars(
        select(SignalEvent).where(
            SignalEvent.event_type.in_(list(EVENT_METRIC_MAP.values())),
            SignalEvent.rule_version == settings.event_rule_version,
        )
    ).all()
    groups: dict[tuple[int | None, int | None], list[SignalEvent]] = defaultdict(list)
    for event in events:
        groups[(event.topic_id, event.asset_id)].append(event)
    created = 0
    for (topic_id, asset_id), group in groups.items():
        group.sort(key=lambda item: item.started_at)
        current: list[SignalEvent] = []
        for event in group + [None]:
            if event is not None and (
                not current
                or event.started_at
                <= (current[-1].ended_at or current[-1].peaked_at) + timedelta(days=1)
            ):
                current.append(event)
                continue
            if current:
                source_sequence = []
                seen_sources = set()
                for item in current:
                    if item.source_id not in seen_sources:
                        source_sequence.append(
                            {"source_id": item.source_id, "started_at": item.started_at.isoformat()}
                        )
                        seen_sources.add(item.source_id)
                if len(seen_sources) >= 2:
                    start = current[0].started_at
                    end = max(item.ended_at or item.peaked_at for item in current)
                    signature = hashlib.sha256(
                        json.dumps(
                            {
                                "topic": topic_id,
                                "asset": asset_id,
                                "start": start.isoformat(),
                                "end": end.isoformat(),
                                "sources": sorted(seen_sources),
                            },
                            sort_keys=True,
                        ).encode()
                    ).hexdigest()
                    existing = session.scalar(
                        select(DiffusionEvent).where(DiffusionEvent.signature_hash == signature)
                    )
                    if existing is None:
                        session.add(
                            DiffusionEvent(
                                topic_id=topic_id,
                                asset_id=asset_id,
                                started_at=start,
                                ended_at=end,
                                first_source_id=source_sequence[0]["source_id"],
                                platform_count=len(seen_sources),
                                source_sequence=source_sequence,
                                rule_version=rule_version,
                                signature_hash=signature,
                            )
                        )
                        created += 1
                    cross_event = session.scalar(
                        select(SignalEvent).where(
                            SignalEvent.source_id.is_(None),
                            SignalEvent.topic_id.is_(None)
                            if topic_id is None
                            else SignalEvent.topic_id == topic_id,
                            SignalEvent.asset_id.is_(None)
                            if asset_id is None
                            else SignalEvent.asset_id == asset_id,
                            SignalEvent.event_type == "cross_platform_spike",
                            SignalEvent.started_at == start,
                            SignalEvent.rule_version == settings.event_rule_version,
                        )
                    )
                    if cross_event is None:
                        session.add(
                            SignalEvent(
                                source_id=None,
                                topic_id=topic_id,
                                asset_id=asset_id,
                                event_type="cross_platform_spike",
                                started_at=start,
                                peaked_at=start,
                                ended_at=end,
                                peak_value=float(len(seen_sources)),
                                peak_zscore=None,
                                peak_percentile=1.0,
                                rule_version=settings.event_rule_version,
                                status="discovered",
                                created_at=now_utc(),
                            )
                        )
            current = [event] if event is not None else []
    session.commit()
    return created
