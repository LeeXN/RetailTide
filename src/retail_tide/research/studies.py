from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from ..config import Settings, get_settings
from ..models import (
    EventMetricLink,
    EventReturn,
    MetricSignal,
    ResearchRun,
    SignalEvent,
    Topic,
)
from ..time import now_utc
from .stats import spearman, summarize

HORIZONS = ("1d", "3d", "5d", "10d", "20d")


def _topic(session: Session, slug: str) -> Topic:
    topic = session.scalar(select(Topic).where(Topic.slug == slug))
    if topic is None:
        raise ValueError(f"unknown topic: {slug}")
    return topic


def _persist_run(
    session: Session,
    result: dict,
    *,
    study_type: str,
    topic_id: int | None,
    event_type=None,
    metric_name=None,
    settings: Settings,
):
    data_until = None
    dates = result.get("observation_period") or {}
    if dates.get("until"):
        try:
            data_until = datetime.fromisoformat(dates["until"])
        except ValueError:
            data_until = None
    query = select(ResearchRun).where(
        ResearchRun.study_type == study_type,
        ResearchRun.topic_id.is_(None) if topic_id is None else ResearchRun.topic_id == topic_id,
        ResearchRun.metric_version == settings.metric_version,
        ResearchRun.prompt_version == settings.prompt_version,
        ResearchRun.schema_version == settings.analysis_schema_version,
        ResearchRun.event_rule_version == settings.event_rule_version,
        ResearchRun.market_provider == settings.market_provider,
        ResearchRun.analysis_model == settings.analysis_model,
    )
    query = query.where(
        ResearchRun.event_type.is_(None)
        if event_type is None
        else ResearchRun.event_type == event_type
    )
    query = query.where(
        ResearchRun.metric_name.is_(None)
        if metric_name is None
        else ResearchRun.metric_name == metric_name
    )
    row = session.scalar(query)
    if row is None:
        row = ResearchRun(
            study_type=study_type,
            topic_id=topic_id,
            event_type=event_type,
            metric_name=metric_name,
            data_until=data_until,
            metric_version=settings.metric_version,
            prompt_version=settings.prompt_version,
            schema_version=settings.analysis_schema_version,
            event_rule_version=settings.event_rule_version,
            market_provider=settings.market_provider,
            analysis_model=settings.analysis_model,
            result=result,
            created_at=now_utc(),
        )
        session.add(row)
    else:
        row.data_until = data_until
        row.result = result
        row.created_at = now_utc()
    session.commit()
    return row


def event_study(
    session: Session,
    *,
    topic_slug: str,
    event_type: str,
    settings: Settings | None = None,
    persist: bool = False,
) -> dict:
    settings = settings or get_settings()
    topic = _topic(session, topic_slug)
    events = session.scalars(
        select(SignalEvent).where(
            SignalEvent.topic_id == topic.id,
            SignalEvent.event_type == event_type,
        )
    ).all()
    by_horizon: dict[str, dict[str, dict]] = {}
    period = {"from": None, "until": None}
    for event in events:
        candidate = event.started_at.isoformat()
        period["from"] = candidate if period["from"] is None else min(period["from"], candidate)
        candidate_end = (event.ended_at or event.peaked_at).isoformat()
        period["until"] = (
            candidate_end if period["until"] is None else max(period["until"], candidate_end)
        )

    event_ids = [event.id for event in events]
    entry_event_ids: set[int] = set()
    return_row_count = 0
    mature_return_count = 0
    horizon_readiness: dict[str, dict[str, int]] = {}
    for horizon in HORIZONS:
        rows = (
            session.scalars(
                select(EventReturn).where(
                    EventReturn.event_id.in_(event_ids), EventReturn.horizon == horizon
                )
            ).all()
            if event_ids
            else []
        )
        entry_event_ids.update(item.event_id for item in rows)
        return_row_count += len(rows)
        raw_mature = sum(item.raw_return is not None for item in rows)
        abnormal_mature = sum(item.market_abnormal_return is not None for item in rows)
        mature_return_count += raw_mature
        horizon_readiness[horizon] = {
            "rows": len(rows),
            "mature": raw_mature,
            "abnormal_mature": abnormal_mature,
            "pending": len(rows) - raw_mature,
        }
        by_horizon.setdefault(horizon, {})["raw_return"] = summarize(
            [item.raw_return for item in rows]
        )
        by_horizon[horizon]["market_abnormal_return"] = summarize(
            [item.market_abnormal_return for item in rows]
        )
        by_horizon[horizon]["sector_abnormal_return"] = summarize(
            [item.sector_abnormal_return for item in rows]
        )

    pending_entry_events = len(events) - len(entry_event_ids)
    pending_horizon_rows = sum(row["pending"] for row in horizon_readiness.values())
    if not events:
        readiness_status = "no_events"
        readiness_note = "当前阈值下尚未检测到这类事件。"
    elif not return_row_count:
        readiness_status = "awaiting_entry"
        readiness_note = "事件已经检测到，等待事件后的下一交易日形成入场价。"
    elif not mature_return_count:
        readiness_status = "awaiting_maturity"
        readiness_note = "入场记录已经建立，尚未到达所选事件后的收益期限。"
    elif pending_entry_events or pending_horizon_rows:
        readiness_status = "partially_mature"
        readiness_note = "已有部分期限形成真实收益，其余事件或期限仍在等待交易日成熟。"
    else:
        readiness_status = "ready"
        readiness_note = "所有已建立的事件回报期限均已成熟。"
    result = {
        "study": "event-study",
        "topic": topic.slug,
        "event": event_type,
        "observation_period": period,
        "events": len(events),
        "horizons": by_horizon,
        "readiness": {
            "status": readiness_status,
            "note": readiness_note,
            "event_count": len(events),
            "entry_event_count": len(entry_event_ids),
            "pending_entry_events": pending_entry_events,
            "return_rows": return_row_count,
            "mature_return_rows": mature_return_count,
            "horizons": horizon_readiness,
        },
        "controls": [
            "prior_return_1d",
            "prior_return_5d",
            "prior_return_20d",
            "volume_zscore",
            "volatility",
        ],
        "versions": {
            "metric_version": settings.metric_version,
            "prompt_version": settings.prompt_version,
            "schema_version": settings.analysis_schema_version,
            "event_rule_version": settings.event_rule_version,
            "market_provider": settings.market_provider,
            "analysis_model": settings.analysis_model,
        },
    }
    if persist:
        _persist_run(
            session,
            result,
            study_type="event-study",
            topic_id=topic.id,
            event_type=event_type,
            settings=settings,
        )
    return result


def quantile_study(
    session: Session,
    *,
    topic_slug: str,
    metric_name: str,
    settings: Settings | None = None,
    horizon: str = "5d",
    persist: bool = False,
) -> dict:
    settings = settings or get_settings()
    if horizon not in HORIZONS:
        raise ValueError(f"unsupported horizon: {horizon}")
    topic = _topic(session, topic_slug)
    rows = []
    linked_event_ids: set[int] = set()
    entry_event_ids: set[int] = set()
    return_row_count = 0
    raw_mature_count = 0
    linked_rows = session.execute(
        select(
            SignalEvent.id.label("event_id"),
            MetricSignal.raw_value,
            EventReturn,
        )
        .select_from(EventMetricLink)
        .join(MetricSignal, MetricSignal.id == EventMetricLink.metric_signal_id)
        .join(SignalEvent, SignalEvent.id == EventMetricLink.event_id)
        .outerjoin(
            EventReturn,
            and_(
                EventReturn.event_id == SignalEvent.id,
                EventReturn.horizon == horizon,
            ),
        )
        .where(
            SignalEvent.topic_id == topic.id,
            MetricSignal.metric_name == metric_name,
        )
    ).all()
    for event_id, raw_value, event_return in linked_rows:
        linked_event_ids.add(event_id)
        if event_return is None:
            continue
        return_row_count += 1
        entry_event_ids.add(event_return.event_id)
        if event_return.raw_return is not None:
            raw_mature_count += 1
        if event_return.market_abnormal_return is not None:
            rows.append((raw_value, event_return.market_abnormal_return, event_id))
    rows.sort(key=lambda item: item[0])
    buckets: dict[str, list[float]] = {f"Q{index}": [] for index in range(1, 6)}
    if rows:
        for index, (_metric, value, _event_id) in enumerate(rows):
            bucket = min(4, int(index * 5 / len(rows))) + 1
            buckets[f"Q{bucket}"].append(value)
    if not linked_event_ids:
        readiness_status = "no_signals"
        readiness_note = "当前赛道尚无由这个指标触发的事件。"
    elif not return_row_count:
        readiness_status = "awaiting_entry"
        readiness_note = "指标事件已经出现，等待事件后的下一交易日形成入场价。"
    elif not raw_mature_count:
        readiness_status = "awaiting_maturity"
        readiness_note = f"已建立入场记录，尚未到达 T+{horizon[:-1]}。"
    elif not rows:
        readiness_status = "benchmark_unavailable"
        readiness_note = "原始收益已经成熟，但基准行情尚未对齐，暂不能计算异常收益。"
    else:
        readiness_status = "ready"
        readiness_note = "异常收益样本已经形成；小样本结果仅作观察。"
    result = {
        "study": "quantile-study",
        "topic": topic.slug,
        "metric": metric_name,
        "horizon": horizon,
        "N": len(rows),
        "quantiles": {key: summarize(values) for key, values in sorted(buckets.items())},
        "spearman": spearman([row[0] for row in rows], [row[1] for row in rows]),
        "readiness": {
            "status": readiness_status,
            "note": readiness_note,
            "linked_events": len(linked_event_ids),
            "entry_events": len(entry_event_ids),
            "return_rows": return_row_count,
            "raw_mature": raw_mature_count,
            "abnormal_mature": len(rows),
        },
        "versions": {
            "metric_version": settings.metric_version,
            "prompt_version": settings.prompt_version,
            "schema_version": settings.analysis_schema_version,
            "event_rule_version": settings.event_rule_version,
            "market_provider": settings.market_provider,
            "analysis_model": settings.analysis_model,
        },
    }
    if persist:
        _persist_run(
            session,
            result,
            study_type="quantile-study",
            topic_id=topic.id,
            metric_name=metric_name,
            settings=settings,
        )
    return result
