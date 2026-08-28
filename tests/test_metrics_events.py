from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select

from retail_tide.config import Settings
from retail_tide.models import MetricSignal, PlatformMetric, SignalEvent, Source, Topic
from retail_tide.pipeline.analysis import save_content_analysis
from retail_tide.pipeline.events import detect_events
from retail_tide.pipeline.metrics import (
    aggregate_metrics,
    compute_signal,
    metric_raw_value,
    metric_snapshot,
)
from retail_tide.pipeline.normalize import insert_raw_observation, normalize_raw_observation
from retail_tide.schemas import RawObservation
from retail_tide.time import UTC


def test_ratio_snapshot_uses_explicit_denominator():
    metric = PlatformMetric(
        bucket_at=datetime(2026, 8, 1, tzinfo=UTC),
        bucket_size="1d",
        source_id=1,
        post_count=20,
        comment_count=0,
        unique_author_count=10,
        retail_count=10,
        novice_count=6,
        bullish_count=6,
        bearish_count=0,
        buy_intent_count=4,
        sell_intent_count=2,
        fomo_count=4,
        panic_count=0,
        engagement_sum=100,
        created_at=datetime(2026, 8, 1, tzinfo=UTC),
    )
    snapshot = metric_snapshot(metric)
    assert snapshot["novice_ratio"] == 0.3
    assert snapshot["fomo_ratio"] == 0.2
    assert snapshot["buy_intent_ratio"] == 0.4
    assert snapshot["sell_intent_ratio"] == 0.2
    assert metric_raw_value(metric, "buy_intent_ratio") == 0.4
    assert metric_raw_value(metric, "sell_intent_ratio") == 0.2


def test_signal_baseline_never_uses_current_observation():
    result = compute_signal([1.0] * 14, 100.0)
    assert result["percentile"] == 1.0
    # MAD is zero for this constant baseline, so the undefined robust z is not
    # invented as a trading signal.
    assert result["robust_z"] is None


def test_aggregate_buy_sell_counts_only_retail_author_intent(session, settings):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    base_result = {
        "investor": {"level": "unknown", "confidence": 0.8},
        "novice_signals": [],
        "direction": {"value": "unknown", "confidence": 0.8},
        "intent": {"value": "buy", "confidence": 0.99},
        "position": {"value": "owned", "confidence": 0.9},
        "emotion": {
            "urgency": False,
            "fear_of_missing": False,
            "social_proof": False,
            "price_chasing": False,
            "regret": False,
            "panic": False,
        },
        "spam": {"value": False, "confidence": 0.99},
    }
    for index, actor in enumerate(("retail", "media")):
        observation = RawObservation(
            source="guba",
            source_item_id=f"retail-only-intent-{index}",
            observation_kind="forum_post",
            published_at=f"2026-08-01T0{index + 1}:00:00Z",
            observed_at="2026-08-01T04:00:00Z",
            payload={
                "body": "测试内容",
                "timestamp_semantics": "published",
                "updated_at": f"2026-08-01T0{index + 1}:00:00Z",
            },
        )
        raw, _ = insert_raw_observation(session, source.id, observation)
        content = normalize_raw_observation(session, raw)
        save_content_analysis(
            session,
            content,
            {**base_result, "actor": {"type": actor, "confidence": 0.99}},
            model="test-model",
            prompt_version="test-prompt",
            schema_version="content-analysis-v1",
        )
    session.commit()

    aggregate_metrics(session, bucket_size="1d", settings=settings)
    metric = session.scalar(
        select(PlatformMetric).where(
            PlatformMetric.source_id == source.id,
            PlatformMetric.topic_id.is_(None),
            PlatformMetric.asset_id.is_(None),
        )
    )
    assert metric.post_count == 2
    assert metric.retail_count == 1
    assert metric.buy_intent_count == 1


def test_promotional_content_is_excluded_from_retail_sentiment_metrics(session, settings):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    observation = RawObservation(
        source="guba",
        source_item_id="promotion-metrics",
        observation_kind="forum_post",
        published_at="2026-08-01T01:00:00Z",
        observed_at="2026-08-01T02:00:00Z",
        payload={"body": "广告推广，扫码领取课程，马上上车"},
    )
    raw, _ = insert_raw_observation(session, source.id, observation)
    content = normalize_raw_observation(session, raw)
    save_content_analysis(
        session,
        content,
        {
            "actor": {"type": "commercial", "confidence": 0.99},
            "investor": {"level": "unknown", "confidence": 0.8},
            "novice_signals": [],
            "direction": {"value": "bullish", "confidence": 0.9},
            "intent": {"value": "buy", "confidence": 0.9},
            "position": {"value": "unknown", "confidence": 0.8},
            "emotion": {
                "urgency": True,
                "fear_of_missing": True,
                "social_proof": False,
                "price_chasing": True,
                "regret": False,
                "panic": False,
            },
            "spam": {"value": False, "confidence": 0.8},
            "promotion": {"value": True, "confidence": 0.99},
        },
        model="test-promotion",
        prompt_version="test-promotion",
        schema_version="content-analysis-v1",
    )
    session.commit()
    aggregate_metrics(session, bucket_size="1d", settings=settings)
    metric = session.scalar(
        select(PlatformMetric).where(
            PlatformMetric.source_id == source.id,
            PlatformMetric.topic_id.is_(None),
            PlatformMetric.asset_id.is_(None),
        )
    )
    assert metric.retail_count == 0
    assert metric.buy_intent_count == 0
    assert metric.fomo_count == 0


def test_contiguous_discovery_signals_merge_to_one_event(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    topic = session.scalar(select(Topic).where(Topic.slug == "gold"))
    asset_id = 1
    start = datetime(2026, 7, 1, tzinfo=UTC)
    for index in range(3):
        metric = PlatformMetric(
            bucket_at=start + timedelta(days=index),
            bucket_size="1d",
            source_id=source.id,
            topic_id=topic.id,
            asset_id=asset_id,
            post_count=10,
            comment_count=0,
            unique_author_count=10,
            retail_count=10,
            novice_count=8,
            bullish_count=8,
            bearish_count=0,
            buy_intent_count=8,
            sell_intent_count=0,
            fomo_count=8,
            panic_count=0,
            engagement_sum=10,
            created_at=start,
        )
        session.add(metric)
        session.flush()
        session.add(
            MetricSignal(
                platform_metric_id=metric.id,
                metric_name="fomo_ratio",
                raw_value=0.8,
                zscore=4,
                robust_z=4,
                percentile=0.99,
                baseline_window="30d",
                metric_version="metric-v1",
                created_at=start,
            )
        )
    session.commit()
    settings = Settings(database_url="sqlite://", config_dir="config")
    count = detect_events(session, settings=settings)
    assert count == 1
    events = session.scalars(
        select(SignalEvent).where(SignalEvent.event_type == "fomo_spike")
    ).all()
    assert len(events) == 1
    assert events[0].ended_at - events[0].started_at == timedelta(days=2)
