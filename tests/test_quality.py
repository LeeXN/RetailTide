from __future__ import annotations

from sqlalchemy import select

from retail_tide.models import Source, SourceQualityMetric
from retail_tide.pipeline.analysis import save_content_analysis
from retail_tide.pipeline.normalize import insert_raw_observation, normalize_raw_observation
from retail_tide.pipeline.quality import refresh_source_quality
from retail_tide.schemas import RawObservation


def _analysis(*, actor: str, spam: bool) -> dict:
    return {
        "actor": {"type": actor, "confidence": 0.99},
        "investor": {"level": "unknown", "confidence": 0.8},
        "novice_signals": [],
        "direction": {"value": "unknown", "confidence": 0.8},
        "intent": {"value": "unknown", "confidence": 0.99},
        "position": {"value": "unknown", "confidence": 0.8},
        "emotion": {
            "urgency": False,
            "fear_of_missing": False,
            "social_proof": False,
            "price_chasing": False,
            "regret": False,
            "panic": False,
        },
        "spam": {"value": spam, "confidence": 0.99},
    }


def test_source_quality_uses_one_preferred_analysis_per_content(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    raw, _ = insert_raw_observation(
        session,
        source.id,
        RawObservation(
            source="guba",
            source_item_id="quality-precedence",
            observation_kind="forum_post",
            published_at="2026-08-01T01:00:00Z",
            observed_at="2026-08-01T02:00:00Z",
            payload={
                "body": "仅用于质量统计优先级测试",
                "timestamp_semantics": "published",
                "updated_at": "2026-08-01T01:00:00Z",
            },
        ),
    )
    content = normalize_raw_observation(session, raw)
    save_content_analysis(
        session,
        content,
        _analysis(actor="unknown", spam=True),
        model="rule-based-v0",
        prompt_version="content-analysis-v1",
        schema_version="content-analysis-v1",
    )
    save_content_analysis(
        session,
        content,
        _analysis(actor="retail", spam=False),
        model="gpt-5.6-sol-via-codex-cli",
        prompt_version="codex-content-review-v3:test",
        schema_version="content-analysis-v1",
    )
    session.commit()

    refresh_source_quality(session)
    values = {
        row.metric_name: row.metric_value
        for row in session.scalars(
            select(SourceQualityMetric).where(SourceQualityMetric.source_id == source.id)
        )
    }
    assert values["analysis_failure_ratio"] == 0
    assert values["unknown_actor_ratio"] == 0
    assert values["spam_ratio"] == 0


def test_trend_source_is_not_reported_as_a_content_parse_failure(session):
    source = session.scalar(select(Source).where(Source.name == "wikimedia-pageviews"))
    insert_raw_observation(
        session,
        source.id,
        RawObservation(
            source="wikimedia-pageviews",
            source_item_id="zh.wikipedia.org:黄金:2026083100:user",
            observation_kind="pageviews",
            published_at="2026-08-31T00:00:00Z",
            observed_at="2026-09-01T04:00:00Z",
            payload={"keyword": "黄金", "value": 10, "unit": "views"},
        ),
    )
    session.commit()

    refresh_source_quality(session)

    metric = session.scalar(
        select(SourceQualityMetric).where(
            SourceQualityMetric.source_id == source.id,
            SourceQualityMetric.metric_name == "parse_failure_ratio",
        )
    )
    assert metric.metric_value == 0
