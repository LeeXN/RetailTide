from datetime import datetime

from sqlalchemy import func, select

from retail_tide.models import (
    Asset,
    EventMetricLink,
    EventReturn,
    MetricSignal,
    PlatformMetric,
    ResearchRun,
    SignalEvent,
    Source,
    Topic,
)
from retail_tide.research.studies import event_study, quantile_study
from retail_tide.time import UTC


def test_research_readiness_distinguishes_entry_and_maturity(session, settings):
    topic = session.scalar(select(Topic).where(Topic.slug == "ai"))
    source = session.scalar(select(Source).where(Source.name == "guba"))
    asset = session.scalar(select(Asset).where(Asset.symbol == "159869"))
    started_at = datetime(2026, 8, 14, tzinfo=UTC)
    event = SignalEvent(
        source_id=source.id,
        topic_id=topic.id,
        event_type="buy_intent_spike",
        started_at=started_at,
        peaked_at=started_at,
        peak_value=0.8,
        peak_zscore=3.2,
        peak_percentile=0.98,
        rule_version=settings.event_rule_version,
        status="discovered",
        created_at=started_at,
    )
    session.add(event)
    session.commit()

    waiting = event_study(
        session,
        topic_slug="ai",
        event_type="buy_intent_spike",
        settings=settings,
    )
    assert waiting["readiness"]["status"] == "awaiting_entry"
    assert waiting["readiness"]["pending_entry_events"] == 1

    metric = PlatformMetric(
        bucket_at=started_at,
        bucket_size="1d",
        source_id=source.id,
        topic_id=topic.id,
        post_count=10,
        buy_intent_count=8,
        created_at=started_at,
    )
    session.add(metric)
    session.flush()
    signal = MetricSignal(
        platform_metric_id=metric.id,
        metric_name="buy_intent_ratio",
        raw_value=0.8,
        zscore=3.2,
        percentile=0.98,
        baseline_window="30d",
        metric_version=settings.metric_version,
        created_at=started_at,
    )
    session.add(signal)
    session.flush()
    session.add(EventMetricLink(event_id=event.id, metric_signal_id=signal.id))
    session.add_all(
        [
            EventReturn(
                event_id=event.id,
                asset_id=asset.id,
                entry_at=datetime(2026, 8, 15, tzinfo=UTC),
                entry_price=1.0,
                horizon="1d",
                exit_at=datetime(2026, 8, 15, tzinfo=UTC),
                exit_price=1.05,
                raw_return=0.05,
                market_return=0.02,
                market_abnormal_return=0.03,
                calculated_at=started_at,
            ),
            EventReturn(
                event_id=event.id,
                asset_id=asset.id,
                entry_at=datetime(2026, 8, 15, tzinfo=UTC),
                entry_price=1.0,
                horizon="3d",
                calculated_at=started_at,
            ),
        ]
    )
    session.commit()

    partial = event_study(
        session,
        topic_slug="ai",
        event_type="buy_intent_spike",
        settings=settings,
    )
    assert partial["readiness"]["status"] == "partially_mature"
    assert partial["readiness"]["horizons"]["1d"] == {
        "rows": 1,
        "mature": 1,
        "abnormal_mature": 1,
        "pending": 0,
    }
    assert partial["readiness"]["horizons"]["3d"]["pending"] == 1

    one_day = quantile_study(
        session,
        topic_slug="ai",
        metric_name="buy_intent_ratio",
        horizon="1d",
        settings=settings,
    )
    three_day = quantile_study(
        session,
        topic_slug="ai",
        metric_name="buy_intent_ratio",
        horizon="3d",
        settings=settings,
    )
    assert one_day["readiness"]["status"] == "ready"
    assert one_day["N"] == 1
    assert three_day["readiness"]["status"] == "awaiting_maturity"
    assert three_day["N"] == 0
    assert session.scalar(select(func.count(ResearchRun.id))) == 0

    event_study(
        session,
        topic_slug="ai",
        event_type="buy_intent_spike",
        settings=settings,
        persist=True,
    )
    quantile_study(
        session,
        topic_slug="ai",
        metric_name="buy_intent_ratio",
        horizon="1d",
        settings=settings,
        persist=True,
    )
    assert session.scalar(select(func.count(ResearchRun.id))) == 2
