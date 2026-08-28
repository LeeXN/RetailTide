from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from statistics import mean, median, pstdev

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import Settings, get_settings
from ..models import (
    AssetTopic,
    Content,
    MetricSignal,
    PlatformMetric,
)
from ..time import as_utc, floor_bucket, now_utc
from .analysis import analysis_precedence_key, fomo_score

METRIC_NAMES = (
    "post_count",
    "unique_author_count",
    "retail_ratio",
    "novice_ratio",
    "fomo_ratio",
    "panic_ratio",
    "buy_intent_ratio",
    "sell_intent_ratio",
    "engagement",
)


def _entity_keys(session: Session, content: Content) -> set[tuple[int | None, int | None]]:
    topics = [e.entity_id for e in content.entities if e.entity_type == "topic"]
    assets = [e.entity_id for e in content.entities if e.entity_type == "asset"]
    keys: set[tuple[int | None, int | None]] = set()
    if topics and assets:
        keys.update((topic_id, asset_id) for topic_id in topics for asset_id in assets)
    elif topics:
        keys.update((topic_id, None) for topic_id in topics)
    elif assets:
        linked_topics = session.scalars(
            select(AssetTopic.topic_id).where(AssetTopic.asset_id.in_(assets))
        ).all()
        keys.update(
            (topic_id, asset_id) for asset_id in assets for topic_id in (linked_topics or [None])
        )
    else:
        keys.add((None, None))
    return keys


def _metric_key_query(session, bucket_at, bucket_size, source_id, topic_id, asset_id):
    query = select(PlatformMetric).where(
        PlatformMetric.bucket_at == bucket_at,
        PlatformMetric.bucket_size == bucket_size,
        PlatformMetric.source_id == source_id,
    )
    query = query.where(
        PlatformMetric.topic_id.is_(None)
        if topic_id is None
        else PlatformMetric.topic_id == topic_id
    )
    query = query.where(
        PlatformMetric.asset_id.is_(None)
        if asset_id is None
        else PlatformMetric.asset_id == asset_id
    )
    return query


def _empty_counts():
    return {
        "post_count": 0,
        "comment_count": 0,
        "authors": set(),
        "retail_count": 0,
        "novice_count": 0,
        "bullish_count": 0,
        "bearish_count": 0,
        "buy_intent_count": 0,
        "sell_intent_count": 0,
        "fomo_count": 0,
        "panic_count": 0,
        "engagement_sum": 0.0,
    }


def _preferred_analysis(content: Content, topic_id: int | None):
    """Prefer the analysis made for this topic, with legacy fallback rows."""

    if topic_id is None and content.analyses:
        return max(content.analyses, key=analysis_precedence_key)
    topic_rows = [row for row in content.analyses if row.topic_id == topic_id]
    if topic_rows:
        return max(topic_rows, key=analysis_precedence_key)
    legacy = [row for row in content.analyses if row.topic_id is None]
    return max(legacy, key=analysis_precedence_key) if legacy else None


def aggregate_metrics(
    session: Session,
    *,
    bucket_size: str = "1h",
    settings: Settings | None = None,
    since=None,
    until=None,
) -> int:
    if bucket_size not in {"1h", "1d"}:
        raise ValueError("bucket_size must be 1h or 1d")
    settings = settings or get_settings()
    contents_query = select(Content).order_by(Content.published_at)
    if since is not None:
        contents_query = contents_query.where(Content.published_at >= as_utc(since))
    if until is not None:
        contents_query = contents_query.where(Content.published_at <= as_utc(until))
    contents = session.scalars(contents_query).all()
    grouped: dict[tuple, dict] = defaultdict(_empty_counts)
    for content in contents:
        bucket_at = floor_bucket(content.published_at, bucket_size)
        for topic_id, asset_id in _entity_keys(session, content):
            key = (bucket_at, content.source_id, topic_id, asset_id)
            counts = grouped[key]
            if str(content.kind).lower() in {"comment", "answer_comment", "reply"}:
                counts["comment_count"] += 1
            else:
                counts["post_count"] += 1
            if content.author_id is not None:
                counts["authors"].add(content.author_id)
            analysis = _preferred_analysis(content, topic_id)
            if analysis:
                is_promotional = bool(getattr(analysis, "promotion", False))
                is_spam = bool(getattr(analysis, "spam", False))
                is_retail = (
                    analysis.actor_type == "retail" and not is_promotional and not is_spam
                )
                counts["retail_count"] += is_retail
                # KOL/media/commercial/bot observations remain in exposure and
                # engagement, while promotion and spam are excluded from all
                # retail sentiment counters.
                counts["novice_count"] += is_retail and analysis.investor_level == "novice"
                counts["bullish_count"] += (
                    not is_promotional and not is_spam and analysis.direction == "bullish"
                )
                counts["bearish_count"] += (
                    not is_promotional and not is_spam and analysis.direction == "bearish"
                )
                counts["buy_intent_count"] += is_retail and analysis.intent == "buy"
                counts["sell_intent_count"] += is_retail and analysis.intent == "sell"
                counts["fomo_count"] += is_retail and fomo_score(analysis) >= 0.5
                counts["panic_count"] += is_retail and bool(
                    (analysis.emotion_signals or {}).get("panic", False)
                )
            counts["engagement_sum"] += sum(
                float(value or 0)
                for value in (
                    content.likes,
                    content.favorites,
                    content.comments,
                    content.shares,
                    content.views,
                )
            )

    metric_rows: list[PlatformMetric] = []
    for (bucket_at, source_id, topic_id, asset_id), counts in grouped.items():
        metric = session.scalar(
            _metric_key_query(session, bucket_at, bucket_size, source_id, topic_id, asset_id)
        )
        values = {
            "post_count": counts["post_count"],
            "comment_count": counts["comment_count"],
            "unique_author_count": len(counts["authors"]),
            "retail_count": int(counts["retail_count"]),
            "novice_count": int(counts["novice_count"]),
            "bullish_count": int(counts["bullish_count"]),
            "bearish_count": int(counts["bearish_count"]),
            "buy_intent_count": int(counts["buy_intent_count"]),
            "sell_intent_count": int(counts["sell_intent_count"]),
            "fomo_count": int(counts["fomo_count"]),
            "panic_count": int(counts["panic_count"]),
            "engagement_sum": counts["engagement_sum"],
        }
        if metric is None:
            metric = PlatformMetric(
                bucket_at=bucket_at,
                bucket_size=bucket_size,
                source_id=source_id,
                topic_id=topic_id,
                asset_id=asset_id,
                created_at=now_utc(),
                **values,
            )
            session.add(metric)
        else:
            for key, value in values.items():
                setattr(metric, key, value)
        metric_rows.append(metric)
    session.flush()
    for metric in metric_rows:
        _upsert_metric_signals(session, metric, metric_version=settings.metric_version)
    session.commit()
    return len(metric_rows)


def metric_raw_value(metric: PlatformMetric, name: str) -> float:
    denominator = metric.post_count + metric.comment_count
    retail_denominator = metric.retail_count
    if name == "post_count":
        return float(metric.post_count)
    if name == "unique_author_count":
        return float(metric.unique_author_count)
    if name == "retail_ratio":
        return _ratio(metric.retail_count, denominator)
    if name == "novice_ratio":
        return _ratio(metric.novice_count, denominator)
    if name == "fomo_ratio":
        return _ratio(metric.fomo_count, denominator)
    if name == "panic_ratio":
        return _ratio(metric.panic_count, denominator)
    if name == "buy_intent_ratio":
        return _ratio(metric.buy_intent_count, retail_denominator)
    if name == "sell_intent_ratio":
        return _ratio(metric.sell_intent_count, retail_denominator)
    if name == "engagement":
        return float(metric.engagement_sum)
    raise ValueError(f"unknown metric name: {name}")


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def compute_signal(
    values: list[float], current: float, *, min_samples: int = 14
) -> dict[str, float | None]:
    """Compute version-independent signal values using only prior observations."""
    if len(values) < min_samples:
        return {"zscore": None, "robust_z": None, "percentile": None}
    average = mean(values)
    std = pstdev(values)
    zscore = (current - average) / std if std > 0 else None
    med = median(values)
    mad = median([abs(value - med) for value in values])
    robust_z = (current - med) / (1.4826 * mad) if mad > 0 else None
    percentile = (sum(value <= current for value in values) + 1) / (len(values) + 1)
    return {"zscore": zscore, "robust_z": robust_z, "percentile": percentile}


def _upsert_metric_signals(
    session: Session, metric: PlatformMetric, *, metric_version: str
) -> None:
    baseline_cutoff = metric.bucket_at - timedelta(days=30)
    grouping = (metric.bucket_size, metric.source_id, metric.topic_id, metric.asset_id)
    prior_metrics = session.scalars(
        select(PlatformMetric)
        .where(
            PlatformMetric.bucket_size == grouping[0],
            PlatformMetric.source_id == grouping[1],
            PlatformMetric.bucket_at < metric.bucket_at,
            PlatformMetric.bucket_at >= baseline_cutoff,
            PlatformMetric.topic_id.is_(None)
            if grouping[2] is None
            else PlatformMetric.topic_id == grouping[2],
            PlatformMetric.asset_id.is_(None)
            if grouping[3] is None
            else PlatformMetric.asset_id == grouping[3],
        )
        .order_by(PlatformMetric.bucket_at)
    ).all()
    for metric_name in METRIC_NAMES:
        current = metric_raw_value(metric, metric_name)
        values = [metric_raw_value(row, metric_name) for row in prior_metrics]
        signal_values = compute_signal(values, current)
        existing = session.scalar(
            select(MetricSignal).where(
                MetricSignal.platform_metric_id == metric.id,
                MetricSignal.metric_name == metric_name,
                MetricSignal.metric_version == metric_version,
            )
        )
        if existing is None:
            session.add(
                MetricSignal(
                    platform_metric_id=metric.id,
                    metric_name=metric_name,
                    raw_value=current,
                    zscore=signal_values["zscore"],
                    robust_z=signal_values["robust_z"],
                    percentile=signal_values["percentile"],
                    baseline_window="30d",
                    metric_version=metric_version,
                    created_at=now_utc(),
                )
            )
        else:
            existing.raw_value = current
            existing.zscore = signal_values["zscore"]
            existing.robust_z = signal_values["robust_z"]
            existing.percentile = signal_values["percentile"]


def metric_snapshot(metric: PlatformMetric) -> dict[str, float | int]:
    denominator = metric.post_count + metric.comment_count
    retail_denominator = metric.retail_count
    return {
        "post_count": metric.post_count,
        "comment_count": metric.comment_count,
        "unique_author_count": metric.unique_author_count,
        "novice_ratio": _ratio(metric.novice_count, denominator),
        "fomo_ratio": _ratio(metric.fomo_count, denominator),
        "panic_ratio": _ratio(metric.panic_count, denominator),
        "buy_intent_ratio": _ratio(metric.buy_intent_count, retail_denominator),
        "sell_intent_ratio": _ratio(metric.sell_intent_count, retail_denominator),
        "engagement": metric.engagement_sum,
    }
