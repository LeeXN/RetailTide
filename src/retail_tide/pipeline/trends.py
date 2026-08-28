from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import Settings, get_settings
from ..models import Source, TrendObservation, TrendSignal
from ..time import as_utc, now_utc
from .metrics import compute_signal


def aggregate_trend_signals(
    session: Session,
    *,
    settings: Settings | None = None,
    since=None,
    until=None,
) -> int:
    """Derive attention changes from independent trend observations.

    This intentionally does not join Content or ContentAnalysis: Wikimedia
    pageviews are aggregate attention evidence, not investor-language samples.
    """

    settings = settings or get_settings()
    query = (
        select(TrendObservation)
        .join(Source, Source.id == TrendObservation.source_id)
        .where(Source.name == "wikimedia-pageviews")
        .order_by(
            TrendObservation.topic_id,
            TrendObservation.keyword,
            TrendObservation.observed_at,
        )
    )
    observations = session.scalars(query).all()
    groups: dict[tuple[int | None, str], list[TrendObservation]] = defaultdict(list)
    for observation in observations:
        groups[(observation.topic_id, observation.keyword)].append(observation)

    inserted = 0
    start = as_utc(since)
    end = as_utc(until)
    for rows in groups.values():
        for index, observation in enumerate(rows):
            observed_at = as_utc(observation.observed_at)
            if observed_at is None:
                continue
            if start is not None and observed_at < start:
                continue
            if end is not None and observed_at >= end:
                continue
            prior_rows = [
                row
                for row in rows[:index]
                if (as_utc(row.observed_at) or observed_at)
                >= observed_at - timedelta(days=30)
            ]
            prior_values = [float(row.value) for row in prior_rows]
            previous = prior_rows[-1].value if prior_rows else None
            change_ratio = (
                (float(observation.value) - float(previous)) / float(previous)
                if previous not in (None, 0)
                else None
            )
            signals = compute_signal(prior_values, float(observation.value))
            metric_name = "pageviews"
            existing = session.scalar(
                select(TrendSignal).where(
                    TrendSignal.trend_observation_id == observation.id,
                    TrendSignal.metric_name == metric_name,
                    TrendSignal.metric_version == settings.metric_version,
                )
            )
            values = {
                "raw_value": float(observation.value),
                "change_ratio": change_ratio,
                "zscore": signals["zscore"],
                "robust_z": signals["robust_z"],
                "percentile": signals["percentile"],
                "baseline_window": "30d",
            }
            if existing is None:
                session.add(
                    TrendSignal(
                        trend_observation_id=observation.id,
                        metric_name=metric_name,
                        metric_version=settings.metric_version,
                        created_at=now_utc(),
                        **values,
                    )
                )
                inserted += 1
            else:
                for key, value in values.items():
                    setattr(existing, key, value)
    session.commit()
    return inserted


def trend_snapshot(
    session: Session,
    *,
    topic_id: int | None = None,
    keyword: str | None = None,
    limit: int = 200,
    all_topics: bool = False,
) -> list[dict]:
    """Return pageview points and derived signals for the API/dashboard."""

    query = (
        select(TrendObservation, TrendSignal, Source.name)
        .join(TrendSignal, TrendSignal.trend_observation_id == TrendObservation.id)
        .join(Source, Source.id == TrendObservation.source_id)
        .where(Source.name == "wikimedia-pageviews")
    )
    if topic_id is None and not all_topics:
        query = query.where(TrendObservation.topic_id.is_(None))
    elif topic_id is not None:
        query = query.where(TrendObservation.topic_id == topic_id)
    if keyword:
        query = query.where(TrendObservation.keyword == keyword)
    rows = session.execute(
        query.order_by(TrendObservation.observed_at.desc()).limit(limit)
    ).all()
    return [
        {
            "source": source_name,
            "topic_id": observation.topic_id,
            "keyword": observation.keyword,
            "observed_at": as_utc(observation.observed_at),
            "value": observation.value,
            "unit": observation.unit,
            "change_ratio": signal.change_ratio,
            "zscore": signal.zscore,
            "robust_z": signal.robust_z,
            "percentile": signal.percentile,
            "metric_version": signal.metric_version,
        }
        for observation, signal, source_name in rows
    ]
