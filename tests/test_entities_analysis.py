from __future__ import annotations

import logging
from datetime import datetime

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from retail_tide.models import (
    AnalysisTask,
    Content,
    ContentAnalysis,
    ContentAnalysisReview,
    ContentEntity,
    Source,
    Topic,
)
from retail_tide.pipeline.analysis import (
    FailoverAnalysisProvider,
    RuleBasedAnalysisProvider,
    analysis_precedence_key,
    analysis_task_summary,
    analyze_pending,
    save_content_analysis,
    validate_analysis,
)
from retail_tide.pipeline.codex_review import (
    CompatibleLLMTransportError,
    _coerce_compatible_item,
    _degrade_unverifiable_intent,
    _parse_json_object,
    _run_codex_batch_resilient,
    _run_compatible_batch_resilient,
    _validate_review_item,
    review_contents_with_codex,
    review_contents_with_compatible_llm,
)
from retail_tide.pipeline.entities import EntityResolver
from retail_tide.pipeline.normalize import normalize_raw_observation
from retail_tide.pipeline.timestamps import publication_time_audit
from retail_tide.schemas import AnalysisContract, RawObservation
from retail_tide.time import UTC


def test_registry_resolution_cases(session):
    resolver = EntityResolver(session)
    assert resolver.resolve_asset("600519").label == "贵州茅台"
    assert resolver.resolve_asset("贵州茅台").label == "贵州茅台"
    assert resolver.resolve_asset("茅台").label == "贵州茅台"
    assert resolver.resolve_asset("苹果").label == "Apple"
    assert resolver.resolve_asset("黄金").label == "黄金ETF"
    assert resolver.resolve_topic("机器人").label == "人形机器人"


def test_ambiguous_alias_is_left_unresolved(session):
    from retail_tide.models import Asset, AssetAlias

    other = Asset(
        market="CN",
        symbol="999999",
        name="测试资产",
        asset_type="stock",
        currency="CNY",
        timezone="Asia/Shanghai",
    )
    session.add(other)
    session.flush()
    session.add(AssetAlias(asset_id=other.id, alias="苹果", alias_type="name", priority=10))
    session.commit()
    assert EntityResolver(session).resolve_asset("苹果") is None


def test_normalized_content_entity_and_strict_analysis(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    raw = RawObservation(
        source="guba",
        source_item_id="analysis-1",
        observation_kind="forum_post",
        published_at="2026-08-01T01:00:00Z",
        observed_at="2026-08-01T02:00:00Z",
        payload={
            "title": "黄金新手",
            "body": "第一次怎么买黄金，怕错过，准备追涨",
            "author_id": "a",
        },
    )
    # Normalize through the public model path to exercise UTC conversion.
    from retail_tide.pipeline.normalize import insert_raw_observation

    stored, _ = insert_raw_observation(session, source.id, raw)
    content = normalize_raw_observation(session, stored)
    session.commit()
    resolver = EntityResolver(session)
    matches = resolver.resolve_content(content)
    session.commit()
    assert any(match.entity_type == "topic" for match in matches)
    result = RuleBasedAnalysisProvider().analyze(title=content.title, body=content.body)
    assert result.investor.level == "novice"
    assert result.emotion.fomo_score > 0
    row = save_content_analysis(
        session,
        content,
        result,
        model="rule-based-v0",
        prompt_version="content-analysis-v1",
        schema_version="content-analysis-v1",
    )
    session.commit()
    assert row.prompt_version and row.schema_version
    with pytest.raises(ValidationError):
        value = RuleBasedAnalysisProvider().analyze(title=None, body="").model_dump()
        value["unexpected"] = True
        AnalysisContract.model_validate(value)


def test_analysis_contract_corrects_unambiguous_provider_aliases():
    payload = RuleBasedAnalysisProvider().analyze(title=None, body="市场震荡").model_dump()
    payload["actor"] = {"value": "media", "confidence": 0.85}
    payload["investor"] = {"value": "unknown", "confidence": 0.7}

    result = validate_analysis(payload)

    assert result.actor.type == "media"
    assert result.investor.level == "unknown"


def test_compatible_json_parser_accepts_reasoning_wrappers_but_keeps_json_strict():
    payload = '{"items":[{"id":1}]}'

    assert _parse_json_object(f"<think>内部推理</think>\n{payload}")["items"][0]["id"] == 1
    assert _parse_json_object(f"分析完成：\n{payload}\n以上")["items"][0]["id"] == 1
    with pytest.raises(ValueError, match="not valid JSON"):
        _parse_json_object("只有解释，没有结构化结果")


def test_review_accepts_directional_tendency_without_claiming_self_action():
    row = {
        "id": 42,
        "source": "guba",
        "published_at": "2026-08-21T01:00:00+00:00",
        "title": "风险提醒",
        "body": "这个板块后面还要跌，大家千万小心。",
    }
    item = {
        "id": 42,
        "actor": {"type": "retail", "confidence": 0.9},
        "investor": {"level": "unknown", "confidence": 0.8},
        "novice_signals": [],
        "direction": {"value": "bearish", "confidence": 0.9},
        "intent": {"value": "sell", "confidence": 0.85},
        "position": {"value": "unknown", "confidence": 0.8},
        "emotion": {
            "urgency": True,
            "fear_of_missing": False,
            "social_proof": False,
            "price_chasing": False,
            "regret": False,
            "panic": False,
        },
        "spam": {"value": False, "confidence": 0.95},
        "promotion": {"value": False, "confidence": 0.95},
        "intent_basis": "risk_warning",
        "intent_evidence": "后面还要跌，大家千万小心",
        "rationale": "作者提醒他人下跌风险，属于偏卖倾向，不代表本人已经卖出。",
    }

    contract = _validate_review_item(item, row)

    assert contract.intent.value == "sell"
    assert contract.direction.value == "bearish"


def test_review_accepts_advice_but_rejects_misaligned_directional_view():
    row = {
        "id": 43,
        "source": "guba",
        "published_at": "2026-08-21T01:00:00+00:00",
        "title": "操作建议",
        "body": "洗盘结束，回调可以买。",
    }
    item = {
        "id": 43,
        "actor": {"type": "retail", "confidence": 0.9},
        "investor": {"level": "unknown", "confidence": 0.8},
        "novice_signals": [],
        "direction": {"value": "bullish", "confidence": 0.9},
        "intent": {"value": "buy", "confidence": 0.85},
        "position": {"value": "unknown", "confidence": 0.8},
        "emotion": {
            "urgency": False,
            "fear_of_missing": False,
            "social_proof": False,
            "price_chasing": False,
            "regret": False,
            "panic": False,
        },
        "spam": {"value": False, "confidence": 0.95},
        "promotion": {"value": False, "confidence": 0.95},
        "intent_basis": "advice_or_recommendation",
        "intent_evidence": "回调可以买",
        "rationale": "作者给读者建议买入，属于偏买倾向，不代表本人已经买入。",
    }

    assert _validate_review_item(item, row).intent.value == "buy"
    with pytest.raises(ValueError, match="directional tendency"):
        _validate_review_item(
            {
                **item,
                "direction": {"value": "bearish", "confidence": 0.9},
                "intent_basis": "market_directional_view",
            },
            row,
        )


def test_analysis_failure_is_logged_and_remains_retryable(session, settings, caplog):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    raw = RawObservation(
        source="guba",
        source_item_id="analysis-log-failure",
        observation_kind="forum_post",
        published_at="2026-08-01T01:00:00Z",
        observed_at="2026-08-01T02:00:00Z",
        payload={"title": "日志测试", "body": "散户投资记录"},
    )
    stored, _ = insert_raw_observation(session, source.id, raw)
    normalize_raw_observation(session, stored)
    session.commit()

    class FailingProvider:
        model = "rule-based-failing"

        def analyze(self, *, title, body, topic=None):
            raise RuntimeError("upstream schema failed")

    with caplog.at_level(logging.WARNING, logger="retail_tide.pipeline.analysis"):
        assert (
            analyze_pending(
                session,
                limit=1,
                provider=FailingProvider(),
                settings=settings,
            )
            == 0
        )

    task = session.scalar(select(AnalysisTask))
    assert task.status == "failed"
    assert task.next_retry_at is not None
    assert "event=analysis_failed" in caplog.text
    assert "upstream schema failed" in caplog.text
    summary = analysis_task_summary(session, model=FailingProvider.model)
    assert summary["failed"] == 1
    assert summary["retry_deferred"] == 1

    outside_window = analysis_task_summary(
        session,
        model=FailingProvider.model,
        since=datetime(2026, 8, 2, tzinfo=UTC),
        until=datetime(2026, 8, 3, tzinfo=UTC),
    )
    assert outside_window["failed"] == 0
    assert outside_window["targets"] == 0


def test_analyze_pending_advances_past_already_analyzed_content(session, settings):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    for index in range(2):
        raw = RawObservation(
            source="guba",
            source_item_id=f"pending-analysis-{index}",
            observation_kind="forum_post",
            published_at=f"2026-08-0{index + 1}T01:00:00Z",
            observed_at=f"2026-08-0{index + 1}T02:00:00Z",
            payload={"title": f"帖子 {index}", "body": "散户投资记录"},
        )
        stored, _ = insert_raw_observation(session, source.id, raw)
        normalize_raw_observation(session, stored)
    session.commit()

    provider = RuleBasedAnalysisProvider()
    assert analyze_pending(session, limit=1, provider=provider, settings=settings) == 1
    assert analyze_pending(session, limit=1, provider=provider, settings=settings) == 1
    assert len(session.scalars(select(ContentAnalysis)).all()) == 2


def test_analyze_pending_can_be_scoped_to_collection_window(session, settings):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    for index, published_at in enumerate(
        ("2026-08-23T01:00:00Z", "2026-08-24T01:00:00Z")
    ):
        raw = RawObservation(
            source="guba",
            source_item_id=f"window-analysis-{index}",
            observation_kind="forum_post",
            published_at=published_at,
            observed_at=published_at,
            payload={"title": f"窗口帖子 {index}", "body": "散户投资记录"},
        )
        stored, _ = insert_raw_observation(session, source.id, raw)
        normalize_raw_observation(session, stored)
    session.commit()

    provider = RuleBasedAnalysisProvider()
    assert (
        analyze_pending(
            session,
            limit=10,
            provider=provider,
            settings=settings,
            since=datetime(2026, 8, 24, tzinfo=UTC),
            until=datetime(2026, 8, 25, tzinfo=UTC),
        )
        == 1
    )
    analyzed = session.scalars(select(ContentAnalysis)).all()
    assert len(analyzed) == 1
    assert session.get(Content, analyzed[0].content_id).source_item_id == "window-analysis-1"


def test_analysis_failover_saves_actual_model_and_resumes_without_reprocessing(
    session, settings
):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    raw = RawObservation(
        source="guba",
        source_item_id="analysis-provider-failover",
        observation_kind="forum_post",
        published_at="2026-08-01T01:00:00Z",
        observed_at="2026-08-01T02:00:00Z",
        payload={"title": "主备切换", "body": "我继续持有黄金ETF。"},
    )
    stored, _ = insert_raw_observation(session, source.id, raw)
    content = normalize_raw_observation(session, stored)
    session.commit()

    class PrimaryProvider:
        model = "primary-model"
        calls = 0

        def analyze(self, *, title, body, topic=None):
            self.calls += 1
            raise RuntimeError("primary unavailable")

    class BackupProvider:
        model = "backup-model"
        calls = 0

        def analyze(self, *, title, body, topic=None):
            self.calls += 1
            return RuleBasedAnalysisProvider().analyze(title=title, body=body, topic=topic)

    primary = PrimaryProvider()
    backup = BackupProvider()
    provider = FailoverAnalysisProvider(primary, backup)

    assert analyze_pending(session, limit=1, provider=provider, settings=settings) == 1
    assert analyze_pending(session, limit=1, provider=provider, settings=settings) == 0
    analysis = session.scalar(
        select(ContentAnalysis).where(ContentAnalysis.content_id == content.id)
    )
    task = session.scalar(select(AnalysisTask).where(AnalysisTask.content_id == content.id))
    assert analysis.model == "backup-model"
    assert task.model == "primary-model"
    assert task.status == "completed"
    assert primary.calls == 1
    assert backup.calls == 1


def test_valid_unknown_from_primary_does_not_call_fallback():
    class PrimaryProvider:
        model = "primary-model"

        def analyze(self, *, title, body, topic=None):
            return RuleBasedAnalysisProvider().analyze(
                title="普通讨论", body="今天市场震荡。", topic=topic
            )

    class BackupProvider:
        model = "backup-model"
        calls = 0

        def analyze(self, *, title, body, topic=None):
            self.calls += 1
            raise AssertionError("valid unknown must not trigger failover")

    backup = BackupProvider()
    provider = FailoverAnalysisProvider(PrimaryProvider(), backup)

    result = provider.analyze(title="普通讨论", body="今天市场震荡。")

    assert result.intent.value == "unknown"
    assert provider.last_model == "primary-model"
    assert backup.calls == 0


def test_analysis_is_scoped_to_each_content_topic_and_promotion_is_not_spam(session, settings):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    gold = session.scalar(select(Topic).where(Topic.slug == "gold"))
    robot = session.scalar(select(Topic).where(Topic.slug == "humanoid-robot"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    raw = RawObservation(
        source="guba",
        source_item_id="topic-analysis-promotion",
        observation_kind="forum_post",
        published_at="2026-08-01T01:00:00Z",
        observed_at="2026-08-01T02:00:00Z",
        payload={
            "title": "黄金和机器人课程推广",
            "body": "广告推广：扫码加微信领取课程，马上上车。",
        },
    )
    stored, _ = insert_raw_observation(session, source.id, raw)
    content = normalize_raw_observation(session, stored)
    session.add_all(
        [
            ContentEntity(
                content_id=content.id,
                entity_type="topic",
                entity_id=gold.id,
                method="test",
                confidence=1,
                created_at=content.first_collected_at,
            ),
            ContentEntity(
                content_id=content.id,
                entity_type="topic",
                entity_id=robot.id,
                method="test",
                confidence=1,
                created_at=content.first_collected_at,
            ),
        ]
    )
    session.commit()

    assert (
        analyze_pending(session, limit=10, provider=RuleBasedAnalysisProvider(), settings=settings)
        == 2
    )
    rows = session.scalars(
        select(ContentAnalysis).where(ContentAnalysis.content_id == content.id)
    ).all()
    assert {row.topic_id for row in rows} == {gold.id, robot.id}
    assert all(row.promotion is True for row in rows)


def test_batch_analysis_calls_llm_once_for_content_with_multiple_topics(session, settings):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    gold = session.scalar(select(Topic).where(Topic.slug == "gold"))
    robot = session.scalar(select(Topic).where(Topic.slug == "humanoid-robot"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    raw = RawObservation(
        source="guba",
        source_item_id="batch-analysis-multiple-topics",
        observation_kind="forum_post",
        published_at="2026-08-01T01:00:00Z",
        observed_at="2026-08-01T02:00:00Z",
        payload={"title": "黄金与机器人", "body": "我准备买入相关ETF。"},
    )
    stored, _ = insert_raw_observation(session, source.id, raw)
    content = normalize_raw_observation(session, stored)
    session.add_all(
        [
            ContentEntity(
                content_id=content.id,
                entity_type="topic",
                entity_id=topic.id,
                method="test",
                confidence=1,
                created_at=content.first_collected_at,
            )
            for topic in (gold, robot)
        ]
    )
    session.commit()

    class BatchProvider:
        model = "batch-test"
        batch_size = 4

        def __init__(self):
            self.calls = []

        def analyze_batch(self, *, items):
            self.calls.append(items)
            rules = RuleBasedAnalysisProvider()
            return {
                item["id"]: rules.analyze(title=item["title"], body=item["body"])
                for item in items
            }

    provider = BatchProvider()
    assert analyze_pending(session, limit=10, provider=provider, settings=settings) == 2
    assert analyze_pending(session, limit=10, provider=provider, settings=settings) == 0
    assert len(provider.calls) == 1
    assert len(provider.calls[0]) == 1
    assert len(provider.calls[0][0]["topics"]) == 2
    rows = session.scalars(
        select(ContentAnalysis).where(ContentAnalysis.content_id == content.id)
    ).all()
    assert {row.topic_id for row in rows} == {gold.id, robot.id}
    assert all(row.spam is False for row in rows)
    assert session.scalars(select(AnalysisTask).where(AnalysisTask.content_id == content.id)).all()


def test_codex_review_saves_explicit_intent_evidence_and_resumes(
    session, settings, monkeypatch
):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    raw = RawObservation(
        source="guba",
        source_item_id="codex-review-1",
        observation_kind="forum_post",
        published_at="2026-08-01T01:00:00Z",
        observed_at="2026-08-01T02:00:00Z",
        payload={
            "title": "操作记录",
            "body": "我今天加仓了半导体ETF。",
            "timestamp_semantics": "published",
            "updated_at": "2026-08-01T01:00:00Z",
        },
    )
    stored, _ = insert_raw_observation(session, source.id, raw)
    normalize_raw_observation(session, stored)
    session.commit()

    def fake_batch(rows, **_kwargs):
        assert len(rows) == 1
        return [
            {
                "id": rows[0]["id"],
                "actor": {"type": "retail", "confidence": 0.99},
                "investor": {"level": "unknown", "confidence": 0.8},
                "novice_signals": [],
                "direction": {"value": "unknown", "confidence": 0.8},
                "intent": {"value": "buy", "confidence": 0.99},
                "position": {"value": "owned", "confidence": 0.99},
                "emotion": {
                    "urgency": False,
                    "fear_of_missing": False,
                    "social_proof": False,
                    "price_chasing": False,
                    "regret": False,
                    "panic": False,
                },
                "spam": {"value": False, "confidence": 0.99},
                "intent_basis": "explicit_self_executed",
                "intent_evidence": "我今天加仓了",
                "rationale": "作者明确陈述本人已完成加仓。",
            }
        ]

    monkeypatch.setattr("retail_tide.pipeline.codex_review._run_codex_batch", fake_batch)
    first = review_contents_with_codex(session, batch_size=1)
    second = review_contents_with_codex(session, batch_size=1)

    assert first["reviewed"] == 1
    assert second["reviewed"] == 0
    analysis = session.scalar(
        select(ContentAnalysis).where(ContentAnalysis.model.like("%codex-cli"))
    )
    assert analysis.intent == "buy"
    review = session.scalar(select(ContentAnalysisReview))
    assert review.intent_evidence == "我今天加仓了"
    assert review.intent_basis == "explicit_self_executed"

    class UnneededProvider:
        model = "unneeded-after-evidence-review"

        def analyze(self, **_kwargs):
            raise AssertionError("evidence-reviewed content must not be analyzed again")

    assert (
        analyze_pending(
            session,
            limit=1,
            provider=UnneededProvider(),
            settings=settings,
        )
        == 0
    )
    assert analysis_task_summary(session, model=UnneededProvider.model)["targets"] == 0


def test_codex_review_only_unscanned_skips_any_existing_external_llm(
    session, settings, monkeypatch
):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    contents = []
    for index in range(3):
        raw = RawObservation(
            source="guba",
            source_item_id=f"codex-unscanned-{index}",
            observation_kind="forum_post",
            published_at=f"2026-08-0{index + 1}T01:00:00Z",
            observed_at=f"2026-08-0{index + 1}T02:00:00Z",
            payload={"title": "行情讨论", "body": "今天市场震荡。"},
        )
        stored, _ = insert_raw_observation(session, source.id, raw)
        contents.append(normalize_raw_observation(session, stored))
    save_content_analysis(
        session,
        contents[0],
        RuleBasedAnalysisProvider().analyze(title="行情讨论", body="今天市场震荡。"),
        model="existing-external-model",
        prompt_version=settings.prompt_version,
        schema_version=settings.analysis_schema_version,
    )
    save_content_analysis(
        session,
        contents[2],
        RuleBasedAnalysisProvider().analyze(title="行情讨论", body="今天市场震荡。"),
        model="stale-external-model",
        prompt_version=settings.prompt_version,
        schema_version=settings.analysis_schema_version,
    )
    contents[2].body = "正文已经更新。"
    session.commit()

    seen = []

    def fake_batch(rows, **_kwargs):
        seen.extend(row["id"] for row in rows)
        return [
            {
                "id": row["id"],
                "actor": {"type": "retail", "confidence": 0.8},
                "investor": {"level": "unknown", "confidence": 0.8},
                "novice_signals": [],
                "direction": {"value": "neutral", "confidence": 0.8},
                "intent": {"value": "unknown", "confidence": 0.8},
                "position": {"value": "unknown", "confidence": 0.8},
                "emotion": {
                    "urgency": False,
                    "fear_of_missing": False,
                    "social_proof": False,
                    "price_chasing": False,
                    "regret": False,
                    "panic": False,
                },
                "spam": {"value": False, "confidence": 0.9},
                "intent_basis": "market_description",
                "intent_evidence": row["body"],
                "rationale": "只描述市场。",
            }
            for row in rows
        ]

    monkeypatch.setattr("retail_tide.pipeline.codex_review._run_codex_batch", fake_batch)
    result = review_contents_with_codex(
        session, batch_size=2, only_unscanned=True
    )

    assert result["reviewed"] == 2
    assert seen == [contents[1].id, contents[2].id]


def test_compatible_review_corrects_alias_saves_evidence_and_resumes(session, monkeypatch):
    source = session.scalar(select(Source).where(Source.name == "taoguba"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    raw = RawObservation(
        source="taoguba",
        source_item_id="compatible-review-1",
        observation_kind="forum_post",
        published_at="2026-08-02T01:00:00Z",
        observed_at="2026-08-02T02:00:00Z",
        payload={
            "title": "持仓记录",
            "body": "我继续拿着机器人ETF，今天不卖。",
            "timestamp_semantics": "published",
            "source_timestamp_field": "postDate",
        },
    )
    stored, _ = insert_raw_observation(session, source.id, raw)
    normalize_raw_observation(session, stored)
    session.commit()

    def fake_batch(rows, **_kwargs):
        return [
            {
                "id": rows[0]["id"],
                "actor": {"type": "retail", "confidence": 0.99},
                "investor": {"level": "unknown", "confidence": 0.8},
                "novice_signals": [],
                "direction": {"value": "unknown", "confidence": 0.8},
                "intent": {"value": "hold", "confidence": 0.99},
                "position": {"value": "owned", "confidence": 0.99},
                "emotion": {
                    "urgency": False,
                    "fear_of_missing": False,
                    "social_proof": False,
                    "price_chasing": False,
                    "regret": False,
                    "panic": False,
                },
                "spam": {"value": False, "confidence": 0.99},
                "intent_basis": "explicit_self_position",
                "intent_evidence": "我继续拿着机器人ETF",
                "rationale": "作者明确陈述本人继续持仓。",
            }
        ]

    monkeypatch.setattr(
        "retail_tide.pipeline.codex_review._run_compatible_batch_resilient",
        fake_batch,
    )
    kwargs = {
        "endpoint": "https://example.invalid/v1",
        "api_key": "test-key",
        "model": "agnes-2.0-flash",
        "batch_size": 1,
    }
    first = review_contents_with_compatible_llm(session, **kwargs)
    second = review_contents_with_compatible_llm(session, **kwargs)

    assert first["reviewed"] == 1
    assert second["reviewed"] == 0
    analysis = session.scalar(
        select(ContentAnalysis).where(
            ContentAnalysis.model == "agnes-2.0-flash-via-openai-compatible"
        )
    )
    assert analysis.intent == "hold"
    assert analysis.review.intent_evidence == "我继续拿着机器人ETF"
    assert analysis.review.reviewer == "openai-compatible"

    monkeypatch.setattr("retail_tide.pipeline.codex_review._run_codex_batch", fake_batch)
    skipped = review_contents_with_codex(
        session,
        batch_size=1,
        candidate_model="agnes-2.0-flash-via-openai-compatible",
        candidate_intents={"hold"},
        min_content_chars=1000,
    )
    assert skipped["reviewed"] == 0
    codex = review_contents_with_codex(
        session,
        batch_size=1,
        candidate_model="agnes-2.0-flash-via-openai-compatible",
        candidate_intents={"hold"},
    )
    assert codex["reviewed"] == 1


def test_compatible_review_fails_over_and_resumes_across_both_models(
    session, monkeypatch
):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    raw = RawObservation(
        source="guba",
        source_item_id="compatible-review-failover",
        observation_kind="forum_post",
        published_at="2026-08-02T01:00:00Z",
        observed_at="2026-08-02T02:00:00Z",
        payload={"title": "行情讨论", "body": "今天市场震荡。"},
    )
    stored, _ = insert_raw_observation(session, source.id, raw)
    content = normalize_raw_observation(session, stored)
    session.commit()
    calls = []

    def fake_batch(rows, *, model, **_kwargs):
        calls.append(model)
        if model == "primary-model":
            raise RuntimeError("primary invalid response")
        return [
            {
                "id": row["id"],
                "actor": {"type": "retail", "confidence": 0.8},
                "investor": {"level": "unknown", "confidence": 0.8},
                "novice_signals": [],
                "direction": {"value": "neutral", "confidence": 0.8},
                "intent": {"value": "unknown", "confidence": 0.9},
                "position": {"value": "unknown", "confidence": 0.8},
                "emotion": {
                    "urgency": False,
                    "fear_of_missing": False,
                    "social_proof": False,
                    "price_chasing": False,
                    "regret": False,
                    "panic": False,
                },
                "spam": {"value": False, "confidence": 0.9},
                "promotion": {"value": False, "confidence": 0.9},
                "intent_basis": "market_description",
                "intent_evidence": "今天市场震荡",
                "rationale": "只描述市场，没有明确交易动作。",
            }
            for row in rows
        ]

    monkeypatch.setattr(
        "retail_tide.pipeline.codex_review._run_compatible_batch_resilient",
        fake_batch,
    )
    kwargs = {
        "endpoint": "https://primary.invalid/v1",
        "api_key": "primary-key",
        "model": "primary-model",
        "fallback_endpoint": "https://backup.invalid/v1",
        "fallback_api_key": "backup-key",
        "fallback_model": "backup-model",
        "batch_size": 1,
    }

    first = review_contents_with_compatible_llm(session, **kwargs)
    second = review_contents_with_compatible_llm(session, **kwargs)

    analysis = session.scalar(
        select(ContentAnalysis).where(ContentAnalysis.content_id == content.id)
    )
    assert first["reviewed"] == 1
    assert first["provider_counts"] == {
        "primary-model-via-openai-compatible": 0,
        "backup-model-via-openai-compatible": 1,
    }
    assert second["reviewed"] == 0
    assert analysis.model == "backup-model-via-openai-compatible"
    assert calls == ["primary-model", "backup-model"]


def test_compatible_review_defers_one_invalid_item_and_continues(session, monkeypatch):
    source = session.scalar(select(Source).where(Source.name == "taoguba"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    content_ids = []
    for suffix, body in (("bad", "无法稳定审查的内容"), ("good", "今天市场震荡。")):
        raw = RawObservation(
            source="taoguba",
            source_item_id=f"compatible-defer-{suffix}",
            observation_kind="forum_post",
            published_at="2026-08-02T01:00:00Z",
            observed_at="2026-08-02T02:00:00Z",
            payload={
                "title": "行情记录",
                "body": body,
                "timestamp_semantics": "published",
                "source_timestamp_field": "postDate",
            },
        )
        stored, _ = insert_raw_observation(session, source.id, raw)
        content_ids.append(normalize_raw_observation(session, stored).id)
    session.commit()

    def fake_batch(rows, **_kwargs):
        if any("无法稳定审查" in row["body"] for row in rows):
            raise RuntimeError("evidence validation failed")
        return [
            {
                "id": row["id"],
                "actor": {"type": "unknown", "confidence": 0.8},
                "investor": {"level": "unknown", "confidence": 0.8},
                "novice_signals": [],
                "direction": {"value": "unknown", "confidence": 0.8},
                "intent": {"value": "unknown", "confidence": 0.8},
                "position": {"value": "unknown", "confidence": 0.8},
                "emotion": {
                    "urgency": False,
                    "fear_of_missing": False,
                    "social_proof": False,
                    "price_chasing": False,
                    "regret": False,
                    "panic": False,
                },
                "spam": {"value": False, "confidence": 0.8},
                "intent_basis": "insufficient",
                "intent_evidence": "",
                "rationale": "没有明确的本人交易动作。",
            }
            for row in rows
        ]

    monkeypatch.setattr(
        "retail_tide.pipeline.codex_review._run_compatible_batch_resilient",
        fake_batch,
    )
    result = review_contents_with_compatible_llm(
        session,
        endpoint="https://example.invalid/v1",
        api_key="test-key",
        model="agnes-2.0-flash",
        batch_size=2,
    )

    assert result["reviewed"] == 1
    assert result["failed"] == 1
    assert result["remaining"] == 1
    assert result["failed_items"] == [
        {"content_id": content_ids[0], "error": "evidence validation failed"}
    ]
    reviewed_ids = set(
        session.scalars(
            select(ContentAnalysis.content_id).where(
                ContentAnalysis.model == "agnes-2.0-flash-via-openai-compatible"
            )
        )
    )
    assert reviewed_ids == {content_ids[1]}


def test_compatible_review_only_corrects_unambiguous_schema_aliases():
    corrected = _coerce_compatible_item(
        {
            "actor": {"value": "retail", "confidence": 0.9},
            "investor": {"value": "novice", "confidence": 0.8},
        }
    )
    assert corrected["actor"] == {"type": "retail", "confidence": 0.9}
    assert corrected["investor"] == {"level": "novice", "confidence": 0.8}

    unknown = _coerce_compatible_item(
        {
            "intent": {"value": "unknown", "confidence": 0.7},
            "intent_basis": "market_directional_view",
        }
    )
    assert unknown["intent_basis"] == "insufficient"


def test_compatible_review_retries_transport_without_splitting(monkeypatch):
    calls = 0
    row = {
        "id": 7,
        "source": "guba",
        "published_at": "2026-08-01T00:00:00+00:00",
        "title": "行情讨论",
        "body": "今天市场震荡。",
    }
    item = {
        "id": 7,
        "actor": {"type": "unknown", "confidence": 0.8},
        "investor": {"level": "unknown", "confidence": 0.8},
        "novice_signals": [],
        "direction": {"value": "neutral", "confidence": 0.8},
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
        "spam": {"value": False, "confidence": 0.9},
        "intent_basis": "market_description",
        "intent_evidence": "今天市场震荡",
        "rationale": "只描述市场，没有作者本人交易动作。",
    }

    def fake_batch(_rows, **_kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise CompatibleLLMTransportError("temporary")
        return [item]

    monkeypatch.setattr("retail_tide.pipeline.codex_review._run_compatible_batch", fake_batch)
    monkeypatch.setattr("retail_tide.pipeline.codex_review.sleep", lambda _seconds: None)

    assert _run_compatible_batch_resilient(
        [row],
        endpoint="https://example.invalid/v1",
        api_key="test-key",
        model="test-model",
        timeout=1,
    ) == [item]
    assert calls == 3


def test_compatible_review_repairs_only_invalid_items(monkeypatch):
    rows = [
        {
            "id": content_id,
            "source": "guba",
            "published_at": "2026-08-01T00:00:00+00:00",
            "title": "行情讨论",
            "body": "今天市场震荡。",
        }
        for content_id in (7, 8)
    ]

    def item(content_id: int, *, valid: bool = True):
        return {
            "id": content_id,
            "actor": {"type": "unknown", "confidence": 0.8},
            "investor": {"level": "unknown", "confidence": 0.8},
            "novice_signals": [],
            "direction": {"value": "neutral", "confidence": 0.8},
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
            "spam": {"value": False, "confidence": 0.9},
            "intent_basis": "market_description" if valid else "explicit_self_executed",
            "intent_evidence": "今天市场震荡",
            "rationale": "只描述市场，没有明确交易动作。",
        }

    calls = []

    def fake_batch(batch_rows, **_kwargs):
        calls.append([row["id"] for row in batch_rows])
        if len(batch_rows) == 2:
            return [item(7), item(8, valid=False)]
        return [item(batch_rows[0]["id"])]

    monkeypatch.setattr("retail_tide.pipeline.codex_review._run_compatible_batch", fake_batch)

    result = _run_compatible_batch_resilient(
        rows,
        endpoint="https://example.invalid/v1",
        api_key="test-key",
        model="test-model",
        timeout=1,
    )

    assert [row["id"] for row in result] == [7, 8]
    assert calls == [[7, 8], [8]]


def test_unverifiable_intent_fallback_is_conservative():
    row = {
        "id": 9,
        "source": "guba",
        "published_at": "2026-08-01T00:00:00+00:00",
        "title": "行情讨论",
        "body": "今天市场震荡。",
    }
    item = {
        "id": 9,
        "actor": {"type": "retail", "confidence": 0.8},
        "investor": {"level": "unknown", "confidence": 0.8},
        "novice_signals": [],
        "direction": {"value": "neutral", "confidence": 0.8},
        "intent": {"value": "wait", "confidence": 0.9},
        "position": {"value": "unknown", "confidence": 0.8},
        "emotion": {
            "urgency": False,
            "fear_of_missing": False,
            "social_proof": False,
            "price_chasing": False,
            "regret": False,
            "panic": False,
        },
        "spam": {"value": False, "confidence": 0.9},
        "intent_basis": "market_description",
        "intent_evidence": "",
        "rationale": "只描述市场。",
    }

    result = _degrade_unverifiable_intent(item, row, ValueError("invalid wait"))

    assert result["intent"] == {"value": "unknown", "confidence": 0.0}
    assert result["intent_basis"] == "insufficient"
    assert result["intent_evidence"] == ""
    assert "保守降级" in result["rationale"]


def test_codex_review_degrades_invalid_intent_without_repeating_batch(monkeypatch):
    calls = 0
    row = {
        "id": 9,
        "source": "guba",
        "published_at": "2026-08-01T00:00:00+00:00",
        "title": "行情讨论",
        "body": "今天市场震荡。",
    }
    item = {
        "id": 9,
        "actor": {"type": "unknown", "confidence": 0.8},
        "investor": {"level": "unknown", "confidence": 0.8},
        "novice_signals": [],
        "direction": {"value": "neutral", "confidence": 0.8},
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
        "spam": {"value": False, "confidence": 0.9},
        "intent_basis": "market_description",
        "intent_evidence": "今天市场震荡",
        "rationale": "只描述市场，没有作者本人交易动作。",
    }

    def fake_batch(_rows, **_kwargs):
        nonlocal calls
        calls += 1
        return [{**item, "intent_basis": "explicit_self_executed"}]

    monkeypatch.setattr("retail_tide.pipeline.codex_review._run_codex_batch", fake_batch)
    monkeypatch.setattr("retail_tide.pipeline.codex_review.sleep", lambda _seconds: None)

    result = _run_codex_batch_resilient([row], model="test-model", timeout=1)
    assert result[0]["intent"] == {"value": "unknown", "confidence": 0.0}
    assert result[0]["intent_basis"] == "insufficient"
    assert result[0]["intent_evidence"] == ""
    assert calls == 1


def test_review_analysis_precedence():
    rule = ContentAnalysis(id=1, model="rule-based-v0")
    terra = ContentAnalysis(id=2, model="gpt-5.6-terra-via-codex-cli")
    agnes = ContentAnalysis(
        id=3,
        model="agnes-2.0-flash-via-openai-compatible",
        prompt_version="evidence-content-review-v2:hash",
    )
    sol = ContentAnalysis(
        id=4,
        model="gpt-5.6-sol-via-codex-cli",
        prompt_version="codex-content-review-v3:hash",
    )
    stale_sol = ContentAnalysis(
        id=5,
        model="gpt-5.6-sol-via-codex-cli",
        prompt_version="codex-content-review-v1:hash",
    )
    sol_v2 = ContentAnalysis(
        id=6,
        model="gpt-5.6-sol-via-codex-cli",
        prompt_version="codex-content-review-v2:hash",
    )
    agnes_v3 = ContentAnalysis(
        id=7,
        model="agnes-2.5-pro-via-openai-compatible",
        prompt_version="evidence-content-review-v3:hash",
    )
    sol_v4 = ContentAnalysis(
        id=8,
        model="gpt-5.6-sol-via-codex-cli",
        prompt_version="codex-content-review-v4:hash",
    )

    assert max([rule, terra, agnes, sol], key=analysis_precedence_key) is sol
    assert max([agnes, sol_v2], key=analysis_precedence_key) is sol_v2
    assert max([rule, terra, agnes, stale_sol], key=analysis_precedence_key) is agnes
    assert max([sol, agnes_v3], key=analysis_precedence_key) is agnes_v3
    assert max([agnes_v3, sol_v4], key=analysis_precedence_key) is sol_v4


def test_verified_publication_time_overrides_older_ambiguous_timestamp(session):
    source = session.scalar(select(Source).where(Source.name == "zhihu"))
    from retail_tide.pipeline.normalize import insert_raw_observation

    ambiguous = RawObservation(
        source="zhihu",
        source_item_id="answer:timestamp-correction",
        observation_kind="zhihu_answer",
        published_at="2026-08-01T01:00:00Z",
        observed_at="2026-08-03T01:00:00Z",
        payload={
            "body": "同一条回答",
            "published_at": "2026-08-01T01:00:00Z",
            "updated_at": "2026-08-03T00:00:00Z",
            "timestamp_semantics": "published_or_edited",
        },
    )
    raw, _ = insert_raw_observation(session, source.id, ambiguous)
    content = normalize_raw_observation(session, raw)
    assert content.published_at.isoformat().startswith("2026-08-01")

    verified = RawObservation(
        source="zhihu",
        source_item_id="answer:timestamp-correction",
        observation_kind="zhihu_answer",
        published_at="2026-08-02T01:00:00Z",
        observed_at="2026-08-03T02:00:00Z",
        payload={
            "body": "同一条回答",
            "published_at": "2026-08-02T01:00:00Z",
            "updated_at": "2026-08-03T00:00:00Z",
            "timestamp_semantics": "created",
            "source_timestamp_field": "answer.created_time",
        },
    )
    raw, _ = insert_raw_observation(session, source.id, verified)
    content = normalize_raw_observation(session, raw)
    session.commit()

    assert content.published_at.isoformat().startswith("2026-08-02")
    audit = publication_time_audit(session)
    zhihu = next(row for row in audit["sources"] if row["source"] == "zhihu")
    assert zhihu["verified"] == 1
    assert zhihu["content_time_mismatch"] == 0
