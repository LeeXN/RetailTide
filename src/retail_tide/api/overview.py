from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
from statistics import mean
from typing import Any

from sqlalchemy import and_, case, func, select
from sqlalchemy.orm import Session, aliased

from ..models import (
    Asset,
    AssetTopic,
    CollectionTask,
    Content,
    ContentAnalysis,
    ContentAnalysisReview,
    ContentEntity,
    MarketBar,
    Source,
    Topic,
    TrendObservation,
    TrendSignal,
)
from ..pipeline.analysis import analysis_precedence_key, fomo_score
from ..time import SHANGHAI, UTC, as_utc, bucket_delta, floor_bucket, now_utc

CONTENT_FILTERS = (
    "all",
    "retail",
    "buy",
    "sell",
    "hold",
    "wait",
    "fomo",
    "panic",
    "promotion",
)

_DAILY_HEAT_WEIGHTS = {
    "retail_share": 0.35,
    "retail_volume": 0.25,
    "intent_expression": 0.20,
    "emotion_activation": 0.15,
    "directional_conviction": 0.05,
}
_DAILY_VOLUME_HALF_SATURATION = 20.0


def _empty_period() -> dict[str, Any]:
    return {
        "post_count": 0,
        "comment_count": 0,
        "authors": set(),
        "sources": set(),
        "engagement_sum": 0.0,
        "analyzed_count": 0,
        "retail_count": 0,
        "novice_count": 0,
        "buy_intent_count": 0,
        "sell_intent_count": 0,
        "fomo_count": 0,
        "panic_count": 0,
    }


def _add_content(
    stats: dict[str, Any],
    content: Any,
    analysis: Any | None,
) -> None:
    if str(content.kind).lower() in {"comment", "answer_comment", "reply"}:
        stats["comment_count"] += 1
    else:
        stats["post_count"] += 1
    if content.author_id is not None:
        stats["authors"].add(content.author_id)
    stats["sources"].add(content.source_id)
    stats["engagement_sum"] += (
        float(content.likes or 0)
        + float(content.favorites or 0)
        + float(content.comments or 0)
        + float(content.shares or 0)
        + float(content.views or 0)
    )
    if analysis is None:
        return
    stats["analyzed_count"] += 1
    if analysis.actor_type != "retail" or analysis.promotion or analysis.spam:
        return
    stats["retail_count"] += 1
    stats["novice_count"] += analysis.investor_level == "novice"
    stats["buy_intent_count"] += analysis.intent == "buy"
    stats["sell_intent_count"] += analysis.intent == "sell"
    stats["fomo_count"] += bool(analysis.fomo_active)
    stats["panic_count"] += bool(analysis.panic_active)


def _stats_json(stats: dict[str, Any]) -> dict[str, Any]:
    attention = stats["post_count"] + stats["comment_count"]
    denominator = attention or 1
    retail_denominator = stats["retail_count"] or 1
    return {
        "attention": attention,
        "post_count": stats["post_count"],
        "comment_count": stats["comment_count"],
        "unique_author_count": len(stats["authors"]),
        "source_count": len(stats["sources"]),
        "source_ids": sorted(stats["sources"]),
        "engagement_sum": stats["engagement_sum"],
        "analyzed_count": stats["analyzed_count"],
        "retail_count": stats["retail_count"],
        "novice_count": stats["novice_count"],
        "buy_intent_count": stats["buy_intent_count"],
        "sell_intent_count": stats["sell_intent_count"],
        "fomo_count": stats["fomo_count"],
        "panic_count": stats["panic_count"],
        "analysis_coverage": stats["analyzed_count"] / denominator,
        "retail_ratio": stats["retail_count"] / denominator,
        "novice_ratio": stats["novice_count"] / denominator,
        "buy_intent_ratio": stats["buy_intent_count"] / retail_denominator,
        "sell_intent_ratio": stats["sell_intent_count"] / retail_denominator,
        "fomo_ratio": stats["fomo_count"] / denominator,
        "panic_ratio": stats["panic_count"] / denominator,
    }


def _daily_heat_index(row: dict[str, Any]) -> float | None:
    """Return a same-day 0-100 retail emotion/heat score.

    The index is deliberately computable from the first analyzed natural day.
    Historical context is reported separately as a percentile.  A saturating
    volume term prevents one highly expressive post from looking as hot as a
    broad conversation, while the ratios retain comparability across topics.
    """

    analyzed = int(row.get("analyzed_count") or 0)
    attention = int(row.get("attention") or 0)
    if analyzed <= 0 or attention <= 0:
        return None
    retail = max(0, int(row.get("retail_count") or 0))
    retail_share = min(1.0, retail / attention)
    retail_volume = retail / (retail + _DAILY_VOLUME_HALF_SATURATION) if retail else 0.0
    retail_denominator = retail or 1
    intent_expression = min(
        1.0,
        (int(row.get("buy_intent_count") or 0) + int(row.get("sell_intent_count") or 0))
        / retail_denominator,
    )
    emotion_activation = min(
        1.0,
        (int(row.get("fomo_count") or 0) + int(row.get("panic_count") or 0))
        / retail_denominator,
    )
    directional_conviction = min(
        1.0,
        abs(
            int(row.get("buy_intent_count") or 0)
            - int(row.get("sell_intent_count") or 0)
        )
        / retail_denominator,
    )
    components = {
        "retail_share": retail_share,
        "retail_volume": retail_volume,
        "intent_expression": intent_expression,
        "emotion_activation": emotion_activation,
        "directional_conviction": directional_conviction,
    }
    score = 100 * sum(
        components[name] * weight for name, weight in _DAILY_HEAT_WEIGHTS.items()
    )
    return round(max(0.0, min(100.0, score)), 1)


def _daily_index_confidence(row: dict[str, Any]) -> dict[str, str]:
    analyzed = int(row.get("analyzed_count") or 0)
    attention = int(row.get("attention") or 0)
    coverage = analyzed / attention if attention else 0.0
    if analyzed >= 30 and coverage >= 0.8:
        return {"value": "high", "label": "高置信度"}
    if analyzed >= 10 and coverage >= 0.5:
        return {"value": "medium", "label": "中置信度"}
    if analyzed > 0:
        return {"value": "low", "label": "低置信度"}
    return {"value": "insufficient", "label": "无已分析样本"}


def _with_trend(
    current: dict[str, Any],
    previous: dict[str, Any],
) -> dict[str, Any]:
    current_json = _stats_json(current)
    previous_json = _stats_json(previous)
    attention = current_json["attention"]
    previous_attention = previous_json["attention"]
    change_ratio = (
        (attention - previous_attention) / previous_attention if previous_attention else None
    )
    if attention == 0 and previous_attention == 0:
        trend = "no_data"
    elif previous_attention == 0:
        trend = "new"
    elif change_ratio is not None and change_ratio >= 0.1:
        trend = "rising"
    elif change_ratio is not None and change_ratio <= -0.1:
        trend = "falling"
    else:
        trend = "stable"
    return {
        **current_json,
        "previous_attention": previous_attention,
        "change_ratio": change_ratio,
        "trend": trend,
    }


def _historical_percentile(values: list[float], current: float) -> float | None:
    """Return a mid-rank percentile, only when there is enough observed history."""

    if len(values) < 5:
        return None
    below = sum(value < current for value in values)
    equal = sum(value == current for value in values)
    return round((below + equal / 2) / len(values) * 100, 1)


def _window_summary(
    history: list[dict[str, Any]],
    *,
    start_at: Any,
    end_at: Any,
) -> dict[str, Any]:
    rows = [
        row
        for row in history
        if start_at <= row["bucket_at"] <= end_at and row["analyzed_count"] > 0
    ]
    indexes = [float(row["heat_index"]) for row in rows if row["heat_index"] is not None]
    return {
        "index": round(mean(indexes), 1) if indexes else None,
        "retail_daily_average": (
            round(mean(float(row["retail_count"]) for row in rows), 1) if rows else None
        ),
        "sample_days": len(rows),
        "index_sample_days": len(indexes),
    }


def _trend_profile(score: float | None) -> dict[str, Any]:
    if score is None:
        return {
            "direction": "insufficient",
            "label": "样本不足",
            "score": None,
            "note": "需要更多连续有效日",
        }
    if score >= 12:
        return {
            "direction": "accelerating",
            "label": "快速升温",
            "score": round(score, 1),
            "note": "短中期热度同步向上",
        }
    if score >= 4:
        return {
            "direction": "rising",
            "label": "温和升温",
            "score": round(score, 1),
            "note": "热度高于近期均值",
        }
    if score <= -12:
        return {
            "direction": "cooling_fast",
            "label": "快速降温",
            "score": round(score, 1),
            "note": "短中期热度同步向下",
        }
    if score <= -4:
        return {
            "direction": "cooling",
            "label": "温和降温",
            "score": round(score, 1),
            "note": "热度低于近期均值",
        }
    return {
        "direction": "stable",
        "label": "震荡持平",
        "score": round(score, 1),
        "note": "各观察窗口差异不大",
    }


def _history_metadata(
    history: list[dict[str, Any]],
    *,
    current_bucket_at: Any,
    delta: Any,
    display_start_at: Any | None = None,
    display_window_days: int = 30,
) -> dict[str, Any]:
    if current_bucket_at is None:
        empty_trend = _trend_profile(None)
        empty_trend.update(
            {
                "confidence": "insufficient",
                "confidence_label": "无可比窗口",
                "components": {
                    "today_vs_yesterday": None,
                    "current_7d_vs_previous_7d": None,
                    "current_30d_vs_previous_30d": None,
                },
            }
        )
        return {
            "heat_score": None,
            "daily_index": None,
            "daily_index_confidence": _daily_index_confidence({}),
            "historical_percentile": None,
            "heat_sample_days": 0,
            "heat_window_days": display_window_days,
            "heat_basis": "daily_retail_emotion_heat_v1",
            "historical_percentile_basis": "daily_index_prior_30_valid_days",
            "history_coverage": {
                "window_days": display_window_days,
                "observed_days": 0,
                "analyzed_days": 0,
                "index_days": 0,
                "percentile_days": 0,
                "warming_up_days": 0,
                "missing_observation_days": display_window_days,
                "minimum_baseline_days": 5,
                "current_baseline_days": 0,
                "first_observed_at": None,
                "first_index_at": None,
                "first_percentile_at": None,
                "latest_observed_at": None,
                "status": "empty",
            },
            "trend_windows": {
                "today": {"index": None, "retail_count": 0, "sample_days": 0},
                "yesterday": {
                    "index": None,
                    "retail_count": 0,
                    "sample_days": 0,
                    "change_points": None,
                },
                "7d": {
                    "index": None,
                    "retail_daily_average": None,
                    "sample_days": 0,
                    "index_sample_days": 0,
                    "previous_index": None,
                    "change_points": None,
                },
                "30d": {
                    "index": None,
                    "retail_daily_average": None,
                    "sample_days": 0,
                    "index_sample_days": 0,
                    "previous_index": None,
                    "change_points": None,
                },
            },
            "trend_summary": empty_trend,
            "history": [],
        }
    enriched: list[dict[str, Any]] = []
    for row in sorted(history, key=lambda item: item["bucket_at"]):
        baseline_start = row["bucket_at"] - delta * 30
        prior = [
            float(item["daily_index"])
            for item in enriched
            if baseline_start <= item["bucket_at"] < row["bucket_at"]
            and item["daily_index"] is not None
        ]
        daily_index = _daily_heat_index(row)
        historical_percentile = (
            _historical_percentile(prior, daily_index) if daily_index is not None else None
        )
        enriched.append(
            {
                **row,
                # ``heat_index`` remains as a compatibility alias for clients
                # while its meaning is now the same-day index, not a percentile.
                "heat_index": daily_index,
                "daily_index": daily_index,
                "daily_index_confidence": _daily_index_confidence(row),
                "historical_percentile": historical_percentile,
                "baseline_sample_days": len(prior),
                "heat_status": (
                    "ready"
                    if historical_percentile is not None
                    else "warming_up"
                    if daily_index is not None
                    else "unanalyzed"
                ),
            }
        )

    current = next(
        (row for row in enriched if row["bucket_at"] == current_bucket_at),
        None,
    )
    yesterday = next(
        (row for row in enriched if row["bucket_at"] == current_bucket_at - delta),
        None,
    )
    current_7d = _window_summary(
        enriched,
        start_at=current_bucket_at - delta * 6,
        end_at=current_bucket_at,
    )
    previous_7d = _window_summary(
        enriched,
        start_at=current_bucket_at - delta * 13,
        end_at=current_bucket_at - delta * 7,
    )
    current_30d = _window_summary(
        enriched,
        start_at=current_bucket_at - delta * 29,
        end_at=current_bucket_at,
    )
    previous_30d = _window_summary(
        enriched,
        start_at=current_bucket_at - delta * 59,
        end_at=current_bucket_at - delta * 30,
    )

    def index_delta(current_window: dict[str, Any], previous_window: dict[str, Any], minimum: int):
        if (
            current_window["index"] is None
            or previous_window["index"] is None
            or current_window["index_sample_days"] < minimum
            or previous_window["index_sample_days"] < minimum
        ):
            return None
        return round(current_window["index"] - previous_window["index"], 1)

    day_delta = (
        round(float(current["heat_index"]) - float(yesterday["heat_index"]), 1)
        if current is not None
        and yesterday is not None
        and current["heat_index"] is not None
        and yesterday["heat_index"] is not None
        else None
    )
    seven_delta = index_delta(current_7d, previous_7d, 2)
    thirty_delta = index_delta(current_30d, previous_30d, 5)
    weighted_components = [
        (day_delta, 0.5),
        (seven_delta, 0.3),
        (thirty_delta, 0.2),
    ]
    available = [(value, weight) for value, weight in weighted_components if value is not None]
    trend_score = (
        sum(float(value) * weight for value, weight in available)
        / sum(weight for _value, weight in available)
        if available
        else None
    )
    trend_summary = _trend_profile(trend_score)
    component_count = len(available)
    trend_summary.update(
        {
            "confidence": (
                "high"
                if component_count == 3
                else "medium"
                if component_count == 2
                else "low"
                if component_count == 1
                else "insufficient"
            ),
            "confidence_label": (
                "3/3窗口完整"
                if component_count == 3
                else "2/3窗口可比"
                if component_count == 2
                else "1/3窗口可比"
                if component_count == 1
                else "无可比窗口"
            ),
            "components": {
                "today_vs_yesterday": day_delta,
                "current_7d_vs_previous_7d": seven_delta,
                "current_30d_vs_previous_30d": thirty_delta,
            },
        }
    )
    display_start_at = display_start_at or current_bucket_at - delta * 29
    displayed = [row for row in enriched if row["bucket_at"] >= display_start_at]
    analyzed = [row for row in displayed if row["analyzed_count"] > 0]
    indexed = [row for row in displayed if row["heat_index"] is not None]
    percentiled = [row for row in displayed if row["historical_percentile"] is not None]
    warming_up = [
        row
        for row in displayed
        if row["daily_index"] is not None and row["historical_percentile"] is None
    ]
    coverage_status = (
        "empty"
        if not displayed
        else "warming_up"
        if not percentiled
        else "partial"
        if len(percentiled) < display_window_days
        else "complete"
    )
    return {
        "heat_score": current["heat_index"] if current is not None else None,
        "daily_index": current["daily_index"] if current is not None else None,
        "daily_index_confidence": (
            current["daily_index_confidence"]
            if current is not None
            else _daily_index_confidence({})
        ),
        "historical_percentile": (
            current["historical_percentile"] if current is not None else None
        ),
        "heat_sample_days": len(analyzed),
        "heat_window_days": display_window_days,
        "heat_basis": "daily_retail_emotion_heat_v1",
        "historical_percentile_basis": "daily_index_prior_30_valid_days",
        "history_coverage": {
            "window_days": display_window_days,
            "observed_days": len(displayed),
            "analyzed_days": len(analyzed),
            "index_days": len(indexed),
            "percentile_days": len(percentiled),
            "warming_up_days": len(warming_up),
            "missing_observation_days": max(0, display_window_days - len(displayed)),
            "minimum_baseline_days": 5,
            "current_baseline_days": (
                int(current["baseline_sample_days"]) if current is not None else 0
            ),
            "first_observed_at": displayed[0]["bucket_at"] if displayed else None,
            "first_index_at": indexed[0]["bucket_at"] if indexed else None,
            "first_percentile_at": percentiled[0]["bucket_at"] if percentiled else None,
            "latest_observed_at": displayed[-1]["bucket_at"] if displayed else None,
            "status": coverage_status,
        },
        "trend_windows": {
            "today": {
                "index": current["heat_index"] if current is not None else None,
                "retail_count": current["retail_count"] if current is not None else 0,
                "sample_days": int(current is not None and current["analyzed_count"] > 0),
            },
            "yesterday": {
                "index": yesterday["heat_index"] if yesterday is not None else None,
                "retail_count": yesterday["retail_count"] if yesterday is not None else 0,
                "sample_days": int(yesterday is not None and yesterday["analyzed_count"] > 0),
                "change_points": day_delta,
            },
            "7d": {
                **current_7d,
                "previous_index": previous_7d["index"],
                "change_points": seven_delta,
            },
            "30d": {
                **current_30d,
                "previous_index": previous_30d["index"],
                "change_points": thirty_delta,
            },
        },
        "trend_summary": trend_summary,
        "history": displayed,
    }


def _fomo_signal_count_expression(analysis: Any) -> Any:
    return sum(
        (
            case((analysis.emotion_signals[key].as_boolean().is_(True), 1), else_=0)
            for key in ("urgency", "fear_of_missing", "social_proof", "price_chasing", "regret")
        ),
        start=0,
    )


def _preferred_analyses_by_topic(
    session: Session,
    content_ids: set[int],
    topic_ids: list[int],
) -> tuple[dict[tuple[int, int], Any], dict[int, Any]]:
    """Load every overview analysis in one query and apply the existing precedence rules."""

    if not content_ids:
        return {}, {}
    rows = session.execute(
        select(
            ContentAnalysis.id,
            ContentAnalysis.content_id,
            ContentAnalysis.topic_id,
            ContentAnalysis.model,
            ContentAnalysis.prompt_version,
            ContentAnalysis.actor_type,
            ContentAnalysis.investor_level,
            ContentAnalysis.intent,
            (_fomo_signal_count_expression(ContentAnalysis) >= 3).label("fomo_active"),
            ContentAnalysis.emotion_signals["panic"].as_boolean().label("panic_active"),
            ContentAnalysis.spam,
            ContentAnalysis.promotion,
        ).where(
            ContentAnalysis.content_id.in_(content_ids),
            ContentAnalysis.topic_id.in_(topic_ids) | ContentAnalysis.topic_id.is_(None),
        )
    ).all()
    scoped: dict[tuple[int, int], Any] = {}
    legacy: dict[int, Any] = {}
    for analysis in rows:
        if analysis.topic_id is None:
            current = legacy.get(analysis.content_id)
            if current is None or analysis_precedence_key(analysis) > analysis_precedence_key(
                current
            ):
                legacy[analysis.content_id] = analysis
            continue
        key = (analysis.content_id, analysis.topic_id)
        current = scoped.get(key)
        if current is None or analysis_precedence_key(analysis) > analysis_precedence_key(current):
            scoped[key] = analysis
    return scoped, legacy


def _analysis_priority_expression(analysis: Any) -> Any:
    """SQL equivalent of ``analysis_precedence_key`` for paginated content queries."""

    model = func.lower(analysis.model)
    prompt = func.lower(analysis.prompt_version)
    return case(
        (
            and_(
                model.contains("gpt-5.6-sol-via-codex-cli"),
                prompt.startswith("codex-content-review-v4:"),
            ),
            475,
        ),
        (
            and_(
                model.contains("gpt-5.6-sol-via-codex-cli"),
                prompt.startswith("codex-content-review-v3:"),
            ),
            450,
        ),
        (
            and_(
                model.contains("gpt-5.6-sol-via-codex-cli"),
                prompt.startswith("codex-content-review-v2:"),
            ),
            400,
        ),
        (
            and_(
                model.contains("via-openai-compatible"),
                prompt.startswith("evidence-content-review-v3:"),
            ),
            460,
        ),
        (
            and_(
                model.contains("via-openai-compatible"),
                prompt.startswith("evidence-content-review-v2:"),
            ),
            350,
        ),
        (model.contains("gpt-5.6-terra-via-codex-cli"), 300),
        (model.contains("via-codex-cli"), 250),
        (model.startswith("rule-based"), 0),
        else_=200,
    )


def _preferred_analysis_join(topic_id: int | None) -> tuple[Any, Any]:
    """Return an analysis alias and correlated preferred-analysis ID expression."""

    preferred = aliased(ContentAnalysis)
    candidate = aliased(ContentAnalysis)
    conditions = [candidate.content_id == Content.id]
    ordering: list[Any] = []
    if topic_id is not None:
        conditions.append(candidate.topic_id.in_([topic_id]) | candidate.topic_id.is_(None))
        ordering.append(case((candidate.topic_id == topic_id, 1), else_=0).desc())
    ordering.extend([_analysis_priority_expression(candidate).desc(), candidate.id.desc()])
    preferred_id = (
        select(candidate.id)
        .where(*conditions)
        .order_by(*ordering)
        .limit(1)
        .correlate(Content)
        .scalar_subquery()
    )
    return preferred, preferred_id


def _content_filter_condition(analysis: Any, name: str) -> Any:
    retail = and_(
        analysis.id.is_not(None),
        analysis.actor_type == "retail",
        analysis.promotion.is_(False),
        analysis.spam.is_(False),
    )
    fomo_signals = _fomo_signal_count_expression(analysis)
    conditions = {
        "all": Content.id.is_not(None),
        "retail": retail,
        "buy": and_(retail, analysis.intent == "buy"),
        "sell": and_(retail, analysis.intent == "sell"),
        "hold": and_(retail, analysis.intent == "hold"),
        "wait": and_(retail, analysis.intent == "wait"),
        "fomo": and_(retail, fomo_signals >= 3),
        "panic": and_(
            retail,
            analysis.emotion_signals["panic"].as_boolean().is_(True),
        ),
        "promotion": analysis.promotion.is_(True),
    }
    return conditions[name]


def _topic_asset_payloads(
    session: Session,
    topic_ids: list[int],
    *,
    start_at: Any,
    end_at: Any,
) -> dict[int, list[dict[str, Any]]]:
    if not topic_ids or start_at is None or end_at is None:
        return {}
    links = session.execute(
        select(AssetTopic.topic_id, Asset)
        .join(Asset, Asset.id == AssetTopic.asset_id)
        .where(AssetTopic.topic_id.in_(topic_ids))
        .order_by(AssetTopic.id)
    ).all()
    assets_by_topic: dict[int, list[Asset]] = defaultdict(list)
    for topic_id, asset in links:
        assets_by_topic[topic_id].append(asset)
    asset_ids = {asset.id for topic_assets in assets_by_topic.values() for asset in topic_assets}
    bars_by_asset: dict[int, dict[Any, MarketBar]] = defaultdict(dict)
    if asset_ids:
        bars = session.scalars(
            select(MarketBar)
            .where(
                MarketBar.asset_id.in_(asset_ids),
                MarketBar.interval == "1d",
                MarketBar.ts >= start_at,
                MarketBar.ts < end_at,
            )
            .order_by(MarketBar.ts, MarketBar.id)
        ).all()
        for bar in bars:
            bars_by_asset[bar.asset_id][bar.ts] = bar
    result: dict[int, list[dict[str, Any]]] = {}
    for topic_id, topic_assets in assets_by_topic.items():
        payloads = []
        for asset in topic_assets:
            price_history = [
                {
                    "ts": as_utc(bar.ts),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "provider": bar.provider,
                }
                for bar in bars_by_asset[asset.id].values()
            ]
            payloads.append(
                {
                    "id": asset.id,
                    "symbol": asset.symbol,
                    "name": asset.name,
                    "asset_type": asset.asset_type,
                    "market": asset.market,
                    "currency": asset.currency,
                    "price_history": price_history,
                    "has_price_data": bool(price_history),
                }
            )
        result[topic_id] = payloads
    return result


def _calendar_day_bounds(day: date) -> tuple[datetime, datetime]:
    start = as_utc(datetime.combine(day, time.min, tzinfo=SHANGHAI))
    assert start is not None
    return start, start + timedelta(days=1)


def _wikimedia_histories(
    session: Session,
    *,
    topic_ids: list[int],
    start_at: datetime | None,
    end_at: datetime | None,
) -> tuple[dict[int, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Return per-topic and cross-topic Wikimedia reference histories."""

    if not topic_ids or start_at is None or end_at is None:
        return {}, []
    rows = session.execute(
        select(TrendObservation, TrendSignal)
        .join(TrendSignal, TrendSignal.trend_observation_id == TrendObservation.id)
        .join(Source, Source.id == TrendObservation.source_id)
        .where(
            Source.name == "wikimedia-pageviews",
            TrendObservation.topic_id.in_(topic_ids),
            TrendObservation.observed_at >= start_at,
            TrendObservation.observed_at < end_at,
        )
        .order_by(TrendObservation.observed_at, TrendObservation.id, TrendSignal.id)
    ).all()
    latest: dict[
        tuple[int, str, date], tuple[TrendObservation, TrendSignal]
    ] = {}
    for observation, signal in rows:
        observed_at = as_utc(observation.observed_at)
        if observed_at is None or observation.topic_id is None:
            continue
        latest[(observation.topic_id, observation.keyword, observed_at.date())] = (
            observation,
            signal,
        )

    topic_by_date: dict[tuple[int, date], list[dict[str, Any]]] = defaultdict(list)
    market_by_date: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for (topic_id, _keyword, day), (observation, signal) in sorted(
        latest.items(), key=lambda item: (item[0][0], item[0][2], item[0][1])
    ):
        payload = {
            "topic_id": topic_id,
            "bucket_at": as_utc(observation.observed_at),
            "keyword": observation.keyword,
            "value": observation.value,
            "change_ratio": signal.change_ratio,
            "percentile": signal.percentile,
        }
        topic_by_date[(topic_id, day)].append(payload)
        market_by_date[day].append(payload)

    topic_histories: dict[int, list[dict[str, Any]]] = defaultdict(list)
    previous_topic_value: dict[int, float] = {}
    for (topic_id, _day), payloads in sorted(
        topic_by_date.items(), key=lambda item: (item[0][0], item[0][1])
    ):
        total_value = sum(float(payload["value"] or 0) for payload in payloads)
        percentiles = [
            float(payload["percentile"])
            for payload in payloads
            if payload["percentile"] is not None
        ]
        previous_value = previous_topic_value.get(topic_id)
        topic_histories[topic_id].append(
            {
                "bucket_at": payloads[0]["bucket_at"],
                "keyword": " / ".join(
                    sorted(str(payload["keyword"]) for payload in payloads)
                ),
                "keywords": [
                    {
                        "keyword": payload["keyword"],
                        "value": payload["value"],
                        "change_ratio": payload["change_ratio"],
                        "percentile": payload["percentile"],
                    }
                    for payload in sorted(payloads, key=lambda item: item["keyword"])
                ],
                "value": total_value,
                "change_ratio": (
                    (total_value - previous_value) / previous_value
                    if previous_value not in (None, 0)
                    else None
                ),
                "percentile": mean(percentiles) if percentiles else None,
            }
        )
        previous_topic_value[topic_id] = total_value

    market_history = []
    previous_value = None
    for day, payloads in sorted(market_by_date.items()):
        total_value = sum(float(payload["value"] or 0) for payload in payloads)
        percentiles = [
            float(payload["percentile"])
            for payload in payloads
            if payload["percentile"] is not None
        ]
        market_history.append(
            {
                "bucket_at": as_utc(
                    datetime.combine(day, time.min, tzinfo=UTC)
                ),
                "keyword": "全部赛道",
                "value": total_value,
                "change_ratio": (
                    (total_value - previous_value) / previous_value
                    if previous_value not in (None, 0)
                    else None
                ),
                "percentile": mean(percentiles) if percentiles else None,
                "topic_count": len({payload["topic_id"] for payload in payloads}),
                "keyword_count": len(payloads),
            }
        )
        previous_value = total_value
    return dict(topic_histories), market_history


def _daily_data_coverage(
    session: Session,
    *,
    topic_ids: list[int],
    start_at: datetime,
    end_at: datetime,
    selected_date: date,
    expected_sources: tuple[str, ...],
) -> dict[str, Any]:
    """Describe what is actually present for one natural day.

    Collection checkpoints are evidence about search coverage, not a claim that
    an upstream platform exposed every possible result.  Keeping this separate
    from the heat index prevents a partial collection from looking complete.
    """

    contents = session.scalars(
        select(Content).where(
            Content.published_at >= start_at,
            Content.published_at < end_at,
            Content.is_deleted.is_(False),
        )
    ).all()
    content_ids = {content.id for content in contents}
    source_rows = session.scalars(select(Source)).all()
    source_by_id = {source.id: source for source in source_rows}
    content_counts: dict[str, int] = defaultdict(int)
    for content in contents:
        source = source_by_id.get(content.source_id)
        content_counts[source.name if source else "unknown"] += 1

    indexed_content_ids = set(
        session.scalars(
            select(ContentEntity.content_id)
            .where(
                ContentEntity.entity_type == "topic",
                ContentEntity.entity_id.in_(topic_ids),
                ContentEntity.content_id.in_(content_ids),
            )
            .distinct()
        ).all()
    ) if topic_ids and content_ids else set()
    analyzed_content_ids = set(
        session.scalars(
            select(ContentAnalysis.content_id)
            .where(ContentAnalysis.content_id.in_(indexed_content_ids))
            .distinct()
        ).all()
    ) if indexed_content_ids else set()

    normalized_expected = tuple(
        dict.fromkeys(name.lower().replace("_", "-") for name in expected_sources)
    )
    expected_source_ids = {
        source.name: source.id
        for source in source_rows
        if source.name in normalized_expected
    }
    # Load every task that overlaps the selected natural day.  A task may be
    # successful for a shorter, frozen window (for example 00:00-12:00) while
    # still not proving coverage for the full 00:00-24:00 day.  Previously
    # those tasks were filtered out here and the UI incorrectly said that no
    # task record existed at all.
    tasks = (
        session.scalars(
            select(CollectionTask).where(
                CollectionTask.source_id.in_(expected_source_ids.values()),
                CollectionTask.window_start < end_at,
                CollectionTask.window_end > start_at,
            )
        ).all()
        if expected_source_ids
        else []
    )
    tasks_by_source_topic: dict[tuple[int, int], list[CollectionTask]] = defaultdict(list)
    for task in tasks:
        tasks_by_source_topic[(task.source_id, task.topic_id)].append(task)

    source_coverage = []
    for source_name in sorted(set(normalized_expected) | set(content_counts)):
        source_id = expected_source_ids.get(source_name)
        expected = source_name in normalized_expected
        topic_states: list[str] = []
        next_retry_at = None
        recorded_window_start = None
        recorded_window_end = None
        if expected and source_id is not None:
            for topic_id in topic_ids:
                candidates = tasks_by_source_topic.get((source_id, topic_id), [])
                full_window_candidates = [
                    task
                    for task in candidates
                    if (as_utc(task.window_start) or start_at) <= start_at
                    and (as_utc(task.window_end) or end_at) >= end_at
                ]
                for task in candidates:
                    task_start = as_utc(task.window_start)
                    task_end = as_utc(task.window_end)
                    if task_start is not None and (
                        recorded_window_start is None or task_start < recorded_window_start
                    ):
                        recorded_window_start = task_start
                    if task_end is not None and (
                        recorded_window_end is None or task_end > recorded_window_end
                    ):
                        recorded_window_end = task_end
                if any(task.status == "complete" for task in full_window_candidates):
                    topic_states.append("complete")
                    continue
                states = {task.status for task in full_window_candidates}
                if "degraded" in states:
                    topic_states.append("degraded")
                elif "partial" in states:
                    topic_states.append("partial")
                elif "running" in states:
                    topic_states.append("running")
                elif states:
                    topic_states.append("pending")
                elif candidates:
                    # There is real collection evidence, but it does not span
                    # the complete selected day.  Do not call it absent and do
                    # not promote it to full-day completeness.
                    topic_states.append("window_partial")
                else:
                    topic_states.append("not_recorded")
                for task in candidates:
                    retry_at = as_utc(task.next_retry_at)
                    if retry_at is not None and (
                        next_retry_at is None or retry_at > next_retry_at
                    ):
                        next_retry_at = retry_at
        elif expected:
            topic_states = ["not_recorded"] * len(topic_ids)

        if not expected:
            status = "observed"
        elif topic_states and all(state == "complete" for state in topic_states):
            status = "complete"
        elif "degraded" in topic_states:
            status = "degraded"
        elif "running" in topic_states:
            status = "running"
        elif any(state in {"complete", "partial"} for state in topic_states):
            status = "partial"
        elif "window_partial" in topic_states:
            status = "window_partial"
        elif "pending" in topic_states:
            status = "pending"
        else:
            status = "not_recorded"
        if status == "not_recorded" and content_counts.get(source_name, 0) > 0:
            status = "observed_untracked"
        source_coverage.append(
            {
                "name": source_name,
                "expected": expected,
                "status": status,
                "content_count": content_counts.get(source_name, 0),
                "complete_topics": topic_states.count("complete"),
                "recorded_topics": sum(state != "not_recorded" for state in topic_states),
                "expected_topics": len(topic_ids) if expected else 0,
                "recorded_window_start": recorded_window_start,
                "recorded_window_end": recorded_window_end,
                "next_retry_at": next_retry_at,
            }
        )

    linked_asset_ids = set(
        session.scalars(
            select(AssetTopic.asset_id)
            .where(AssetTopic.topic_id.in_(topic_ids))
            .distinct()
        ).all()
    ) if topic_ids else set()
    exact_bar_asset_ids = set(
        session.scalars(
            select(MarketBar.asset_id)
            .where(
                MarketBar.asset_id.in_(linked_asset_ids),
                MarketBar.interval == "1d",
                MarketBar.ts >= start_at,
                MarketBar.ts < end_at,
            )
            .distinct()
        ).all()
    ) if linked_asset_ids else set()

    expected_rows = [row for row in source_coverage if row["expected"]]
    collection_status = (
        "complete"
        if expected_rows and all(row["status"] == "complete" for row in expected_rows)
        else "untracked"
        if not expected_rows
        else "partial"
    )
    today = now_utc().astimezone(SHANGHAI).date()
    is_collecting = selected_date == today
    analysis_pending = max(0, len(indexed_content_ids) - len(analyzed_content_ids))
    analysis_complete = bool(indexed_content_ids) and analysis_pending == 0
    is_complete = (
        not is_collecting
        and collection_status == "complete"
        and analysis_complete
    )
    return {
        "selected_date": selected_date.isoformat(),
        "window_start": start_at,
        "window_end": end_at,
        "status": "collecting" if is_collecting else "complete" if is_complete else "partial",
        "is_collecting": is_collecting,
        "is_complete": is_complete,
        "analysis_complete": analysis_complete,
        "analysis_status": (
            "complete"
            if analysis_complete
            else "pending"
            if indexed_content_ids
            else "empty"
        ),
        "collection_status": collection_status,
        "content_count": len(content_ids),
        "indexed_content_count": len(indexed_content_ids),
        "analyzed_content_count": len(analyzed_content_ids),
        "analysis_pending_count": analysis_pending,
        "sources": source_coverage,
        "market": {
            "linked_asset_count": len(linked_asset_ids),
            "exact_day_asset_count": len(exact_bar_asset_ids),
            "missing_asset_count": max(0, len(linked_asset_ids) - len(exact_bar_asset_ids)),
        },
    }


def topic_overview(
    session: Session,
    *,
    bucket_size: str = "1d",
    selected_date: date | None = None,
    history_start_date: date | None = None,
    expected_sources: tuple[str, ...] = ("guba", "taoguba"),
) -> dict[str, Any]:
    """Build a deduplicated row for every active topic on a selected day."""

    if bucket_size not in {"1h", "1d"}:
        raise ValueError("bucket_size must be 1h or 1d")
    if selected_date is not None and bucket_size != "1d":
        raise ValueError("selected_date is only supported for 1d overview")
    if history_start_date is not None and bucket_size != "1d":
        raise ValueError("history_start_date is only supported for 1d overview")
    if history_start_date is not None and selected_date is None:
        raise ValueError("history_start_date requires selected_date")
    today = now_utc().astimezone(SHANGHAI).date()
    if selected_date is not None and selected_date > today:
        raise ValueError("selected_date cannot be in the future")
    if history_start_date is not None and selected_date is not None:
        if history_start_date > selected_date:
            raise ValueError("history_start_date cannot be after selected_date")
        if (selected_date - history_start_date).days >= 366:
            raise ValueError("history range cannot exceed 366 calendar days")
    display_window_days = (
        (selected_date - history_start_date).days + 1
        if selected_date is not None and history_start_date is not None
        else 30
    )
    topics = session.scalars(select(Topic).where(Topic.status == "active").order_by(Topic.id)).all()
    topic_ids = [topic.id for topic in topics]
    if not topic_ids:
        empty_history = _history_metadata(
            [],
            current_bucket_at=None,
            delta=bucket_delta(bucket_size),
            display_window_days=display_window_days,
        )
        return {
            "bucket_size": bucket_size,
            "bucket_at": None,
            "previous_bucket_at": None,
            "generated_at": now_utc(),
            "market": {
                **empty_history,
                **_with_trend(_empty_period(), _empty_period()),
                "latest_activity_at": None,
            },
            "topics": [],
        }

    selected_start = selected_end = None
    if selected_date is not None:
        selected_start, selected_end = _calendar_day_bounds(selected_date)
    latest_query = (
        select(func.max(Content.published_at))
        .join(ContentEntity, ContentEntity.content_id == Content.id)
        .where(
            ContentEntity.entity_type == "topic",
            ContentEntity.entity_id.in_(topic_ids),
            Content.is_deleted.is_(False),
        )
    )
    if selected_start is not None and selected_end is not None:
        latest_query = latest_query.where(
            Content.published_at >= selected_start,
            Content.published_at < selected_end,
        )
    latest_at = session.scalar(latest_query)
    latest_activity_query = (
        select(ContentEntity.entity_id, func.max(Content.published_at))
        .join(Content, Content.id == ContentEntity.content_id)
        .where(
            ContentEntity.entity_type == "topic",
            ContentEntity.entity_id.in_(topic_ids),
            Content.is_deleted.is_(False),
        )
    )
    if selected_start is not None and selected_end is not None:
        latest_activity_query = latest_activity_query.where(
            Content.published_at >= selected_start,
            Content.published_at < selected_end,
        )
    latest_activity_rows = session.execute(
        latest_activity_query.group_by(ContentEntity.entity_id)
    ).all()
    latest_activity = {topic_id: value for topic_id, value in latest_activity_rows}

    bucket_at = (
        selected_start
        if selected_start is not None
        else floor_bucket(latest_at, bucket_size)
        if latest_at is not None
        else None
    )
    delta = bucket_delta(bucket_size)
    previous_bucket_at = bucket_at - delta if bucket_at is not None else None
    display_start = (
        _calendar_day_bounds(history_start_date)[0]
        if history_start_date is not None
        else bucket_at - delta * 29
        if bucket_at is not None
        else None
    )
    history_start = (
        min(bucket_at - delta * 89, display_start - delta * 30)
        if bucket_at is not None and display_start is not None
        else None
    )
    query_start = history_start
    topic_heat_stats: dict[tuple[int, Any], dict[str, Any]] = defaultdict(_empty_period)
    market_heat_stats: dict[Any, dict[str, Any]] = defaultdict(_empty_period)
    if bucket_at is not None and previous_bucket_at is not None:
        pairs = session.execute(
            select(
                ContentEntity.entity_id.label("topic_id"),
                Content.id.label("content_id"),
                Content.source_id,
                Content.kind,
                Content.author_id,
                Content.published_at,
                Content.likes,
                Content.favorites,
                Content.comments,
                Content.shares,
                Content.views,
            )
            .join(Content, Content.id == ContentEntity.content_id)
            .where(
                ContentEntity.entity_type == "topic",
                ContentEntity.entity_id.in_(topic_ids),
                Content.published_at >= query_start,
                Content.published_at < bucket_at + delta,
                Content.is_deleted.is_(False),
            )
        ).all()
        scoped_analyses, legacy_analyses = _preferred_analyses_by_topic(
            session,
            {pair.content_id for pair in pairs},
            topic_ids,
        )
        market_heat_seen: dict[Any, set[int]] = defaultdict(set)
        for content in pairs:
            analysis = scoped_analyses.get(
                (content.content_id, content.topic_id)
            ) or legacy_analyses.get(content.content_id)
            heat_bucket_at = floor_bucket(content.published_at, bucket_size)
            if history_start <= heat_bucket_at <= bucket_at:
                _add_content(
                    topic_heat_stats[(content.topic_id, heat_bucket_at)],
                    content,
                    analysis,
                )
                if content.content_id not in market_heat_seen[heat_bucket_at]:
                    _add_content(market_heat_stats[heat_bucket_at], content, analysis)
                    market_heat_seen[heat_bucket_at].add(content.content_id)

    def history_rows(topic_id: int | None) -> list[dict[str, Any]]:
        if history_start is None or bucket_at is None:
            return []
        result = []
        history_bucket_count = int((bucket_at - history_start) / delta) + 1
        for index in range(history_bucket_count):
            row_bucket_at = history_start + delta * index
            stats = (
                market_heat_stats[row_bucket_at]
                if topic_id is None
                else topic_heat_stats[(topic_id, row_bucket_at)]
            )
            row = {
                "bucket_at": row_bucket_at,
                **_stats_json(stats),
            }
            if row["attention"] > 0:
                result.append(row)
        return result

    rows = []
    wikimedia_histories, market_wikimedia_history = _wikimedia_histories(
        session,
        topic_ids=topic_ids,
        start_at=display_start,
        end_at=bucket_at + delta if bucket_at is not None else None,
    )
    asset_payloads = _topic_asset_payloads(
        session,
        topic_ids,
        start_at=display_start,
        end_at=bucket_at + delta if bucket_at is not None else None,
    )
    for topic in topics:
        summary = _with_trend(
            topic_heat_stats[(topic.id, bucket_at)],
            topic_heat_stats[(topic.id, previous_bucket_at)],
        )
        history = history_rows(topic.id)
        topic_assets = asset_payloads.get(topic.id, [])
        for asset in topic_assets:
            asset["selected_day_bar"] = next(
                (
                    bar
                    for bar in reversed(asset["price_history"])
                    if bucket_at is not None
                    and bucket_at <= bar["ts"] < bucket_at + delta
                ),
                None,
            )
        rows.append(
            {
                "id": topic.id,
                "slug": topic.slug,
                "name": topic.name,
                "status": topic.status,
                "latest_activity_at": as_utc(latest_activity.get(topic.id)),
                "asset": topic_assets[0] if topic_assets else None,
                "assets": topic_assets,
                "wikimedia_history": wikimedia_histories.get(topic.id, []),
                **_history_metadata(
                    history,
                    current_bucket_at=bucket_at,
                    delta=delta,
                    display_start_at=display_start,
                    display_window_days=display_window_days,
                ),
                **summary,
            }
        )
    market_history = history_rows(None)
    coverage = (
        _daily_data_coverage(
            session,
            topic_ids=topic_ids,
            start_at=bucket_at,
            end_at=bucket_at + delta,
            selected_date=bucket_at.astimezone(SHANGHAI).date(),
            expected_sources=expected_sources,
        )
        if bucket_size == "1d" and bucket_at is not None
        else None
    )
    return {
        "bucket_size": bucket_size,
        "bucket_at": bucket_at,
        "previous_bucket_at": previous_bucket_at,
        "selected_date": (
            bucket_at.astimezone(SHANGHAI).date().isoformat()
            if bucket_size == "1d" and bucket_at is not None
            else None
        ),
        "generated_at": now_utc(),
        "data_cutoff_at": as_utc(latest_at),
        "comparison_mode": (
            "calendar_day_asia_shanghai" if bucket_size == "1d" else "rolling_1h"
        ),
        "market": {
            **_history_metadata(
                market_history,
                current_bucket_at=bucket_at,
                delta=delta,
                display_start_at=display_start,
                display_window_days=display_window_days,
            ),
            **_with_trend(
                market_heat_stats[bucket_at],
                market_heat_stats[previous_bucket_at],
            ),
            "wikimedia_history": market_wikimedia_history,
            "latest_activity_at": as_utc(latest_at),
        },
        "coverage": coverage,
        "topics": rows,
    }


def topic_contents(
    session: Session,
    *,
    topic_id: int | None,
    bucket_size: str = "1d",
    content_filter: str = "all",
    source_name: str = "all",
    period: str = "latest",
    from_at: Any | None = None,
    to_at: Any | None = None,
    limit: int = 30,
    offset: int = 0,
) -> dict[str, Any]:
    """Return traceable topic content with SQL-side filtering and pagination."""

    if bucket_size not in {"1h", "1d"}:
        raise ValueError("bucket_size must be 1h or 1d")
    if content_filter not in CONTENT_FILTERS:
        raise ValueError(f"content_filter must be one of {', '.join(CONTENT_FILTERS)}")
    if source_name != "all" and (
        not source_name
        or len(source_name) > 50
        or not all(character.isalnum() or character in {"-", "_"} for character in source_name)
    ):
        raise ValueError("source_name must be all or a valid source name")
    if period not in {"latest", "24h", "7d", "30d", "all", "custom"}:
        raise ValueError("period must be latest, 24h, 7d, 30d, all, or custom")
    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")
    if offset < 0:
        raise ValueError("offset must not be negative")

    if topic_id is None:
        latest_at = session.scalar(
            select(func.max(Content.published_at)).where(Content.is_deleted.is_(False))
        )
    else:
        active_topic_ids = session.scalars(select(Topic.id).where(Topic.status == "active")).all()
        latest_at = session.scalar(
            select(func.max(Content.published_at))
            .join(ContentEntity, ContentEntity.content_id == Content.id)
            .where(
                ContentEntity.entity_type == "topic",
                ContentEntity.entity_id.in_(active_topic_ids),
                Content.is_deleted.is_(False),
            )
        )
    bucket_at = floor_bucket(latest_at, bucket_size) if latest_at is not None else None
    if bucket_at is None:
        return {
            "topic_id": topic_id,
            "bucket_size": bucket_size,
            "bucket_at": None,
            "filter": content_filter,
            "source": source_name,
            "period": period,
            "from_at": None,
            "to_at": None,
            "total": 0,
            "facets": {name: 0 for name in CONTENT_FILTERS},
            "source_facets": {},
            "limit": limit,
            "offset": offset,
            "items": [],
        }

    delta = bucket_delta(bucket_size)
    range_end = to_at or latest_at
    if period == "custom":
        range_start = from_at
    elif period == "latest":
        range_start = bucket_at
    elif period == "24h":
        range_start = range_end - delta
    elif period == "7d":
        range_start = range_end - delta * 7
    elif period == "30d":
        range_start = range_end - delta * 30
    else:
        range_start = None
    if from_at is not None:
        range_start = from_at

    def apply_scope(statement: Any) -> Any:
        statement = statement.select_from(Content)
        if topic_id is not None:
            statement = statement.join(
                ContentEntity, ContentEntity.content_id == Content.id
            ).where(
                ContentEntity.entity_type == "topic",
                ContentEntity.entity_id == topic_id,
            )
        statement = statement.where(Content.is_deleted.is_(False))
        if range_start is not None:
            statement = statement.where(Content.published_at >= range_start)
        if range_end is not None:
            statement = statement.where(Content.published_at <= range_end)
        return statement

    source_facets = dict(
        session.execute(
            apply_scope(
                select(Source.name, func.count(Content.id)).join(
                    Source, Source.id == Content.source_id
                )
            ).group_by(Source.name)
        ).all()
    )
    source_id = None
    if source_name != "all":
        source_id = session.scalar(select(Source.id).where(Source.name == source_name))

    preferred, preferred_id = _preferred_analysis_join(topic_id)

    def apply_selected_source(statement: Any) -> Any:
        if source_name == "all":
            return statement
        if source_id is None:
            return statement.where(Content.id.is_(None))
        return statement.where(Content.source_id == source_id)

    facet_columns = [
        func.sum(case((_content_filter_condition(preferred, name), 1), else_=0)).label(name)
        for name in CONTENT_FILTERS
    ]
    facet_row = session.execute(
        apply_selected_source(
            apply_scope(select(*facet_columns)).outerjoin(
                preferred, preferred.id == preferred_id
            )
        )
    ).one()
    facets = {name: int(getattr(facet_row, name) or 0) for name in CONTENT_FILTERS}
    total = facets[content_filter]

    item_rows = session.execute(
        apply_selected_source(
            apply_scope(select(Content, preferred)).outerjoin(
                preferred, preferred.id == preferred_id
            )
        )
        .where(_content_filter_condition(preferred, content_filter))
        .order_by(Content.published_at.desc(), Content.id.desc())
        .limit(limit)
        .offset(offset)
    ).all()
    analysis_ids = {analysis.id for _content, analysis in item_rows if analysis is not None}
    reviews = (
        {
            review.content_analysis_id: review
            for review in session.scalars(
                select(ContentAnalysisReview).where(
                    ContentAnalysisReview.content_analysis_id.in_(analysis_ids)
                )
            ).all()
        }
        if analysis_ids
        else {}
    )
    sources = {
        source.id: source.name for source in session.scalars(select(Source)).all()
    }

    items = []
    for content, analysis in item_rows:
        emotion_signals = analysis.emotion_signals or {} if analysis else {}
        score = fomo_score(analysis) if analysis else 0.0
        review = reviews.get(analysis.id) if analysis else None
        items.append(
            {
                "id": content.id,
                "source_id": content.source_id,
                "source_name": sources.get(content.source_id),
                "source_item_id": content.source_item_id,
                "kind": content.kind,
                "published_at": as_utc(content.published_at),
                "time_semantics": (
                    "market_session_reference"
                    if content.kind == "reference_answer"
                    else "published"
                ),
                "reference_date": (
                    as_utc(content.published_at).astimezone(SHANGHAI).date()
                    if content.kind == "reference_answer"
                    and as_utc(content.published_at) is not None
                    else None
                ),
                "title": content.title,
                "body": content.body,
                "url": content.url,
                "likes": content.likes,
                "favorites": content.favorites,
                "comments": content.comments,
                "shares": content.shares,
                "views": content.views,
                "engagement_sum": sum(
                    int(value or 0)
                    for value in (
                        content.likes,
                        content.favorites,
                        content.comments,
                        content.shares,
                        content.views,
                    )
                ),
                "analysis": {
                    "model": analysis.model,
                    "prompt_version": analysis.prompt_version,
                    "schema_version": analysis.schema_version,
                    "topic_id": analysis.topic_id,
                    "input_hash": analysis.input_hash,
                    "actor_type": analysis.actor_type,
                    "actor_confidence": analysis.actor_confidence,
                    "investor_level": analysis.investor_level,
                    "direction": analysis.direction,
                    "direction_confidence": analysis.direction_confidence,
                    "intent": analysis.intent,
                    "intent_confidence": analysis.intent_confidence,
                    "position": analysis.position,
                    "novice_signals": analysis.novice_signals,
                    "emotion_signals": emotion_signals,
                    "spam": analysis.spam,
                    "spam_confidence": analysis.spam_confidence,
                    "promotion": analysis.promotion,
                    "promotion_confidence": analysis.promotion_confidence,
                    "fomo_score": score,
                    "fomo": (
                        analysis.actor_type == "retail"
                        and not analysis.promotion
                        and not analysis.spam
                        and score >= 0.5
                    ),
                    "panic": (
                        analysis.actor_type == "retail"
                        and not analysis.promotion
                        and not analysis.spam
                        and bool(emotion_signals.get("panic", False))
                    ),
                    "review": {
                        "reviewer": review.reviewer,
                        "intent_basis": review.intent_basis,
                        "intent_evidence": review.intent_evidence,
                        "rationale": review.rationale,
                    }
                    if review
                    else None,
                }
                if analysis
                else None,
            }
        )
    return {
        "topic_id": topic_id,
        "bucket_size": bucket_size,
        "bucket_at": bucket_at,
        "filter": content_filter,
        "source": source_name,
        "period": period,
        "from_at": as_utc(range_start),
        "to_at": as_utc(range_end),
        "total": total,
        "facets": facets,
        "source_facets": dict(sorted(source_facets.items())),
        "limit": limit,
        "offset": offset,
        "items": items,
    }


def topic_series(
    session: Session,
    *,
    topic_id: int,
    bucket_size: str = "1d",
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Return deduplicated topic totals by bucket for detail charts."""

    if bucket_size not in {"1h", "1d"}:
        raise ValueError("bucket_size must be 1h or 1d")
    contents = session.execute(
        select(
            Content.id.label("content_id"),
            Content.source_id,
            Content.kind,
            Content.author_id,
            Content.published_at,
            Content.likes,
            Content.favorites,
            Content.comments,
            Content.shares,
            Content.views,
        )
        .join(ContentEntity, ContentEntity.content_id == Content.id)
        .where(
            ContentEntity.entity_type == "topic",
            ContentEntity.entity_id == topic_id,
            Content.is_deleted.is_(False),
        )
        .order_by(Content.published_at)
    ).all()
    scoped_analyses, legacy_analyses = _preferred_analyses_by_topic(
        session,
        {content.content_id for content in contents},
        [topic_id],
    )
    grouped: dict[Any, dict[str, Any]] = defaultdict(_empty_period)
    for content in contents:
        bucket_at = floor_bucket(content.published_at, bucket_size)
        analysis = scoped_analyses.get(
            (content.content_id, topic_id)
        ) or legacy_analyses.get(content.content_id)
        _add_content(grouped[bucket_at], content, analysis)
    rows = [
        {
            "bucket_at": bucket_at,
            "bucket_size": bucket_size,
            "topic_id": topic_id,
            **_stats_json(stats),
        }
        for bucket_at, stats in grouped.items()
    ]
    return sorted(rows, key=lambda row: row["bucket_at"], reverse=True)[:limit]
