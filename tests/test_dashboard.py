from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy import select, text

from retail_tide.api.app import _event_json, create_app
from retail_tide.api.dashboard_pages import dashboard_html
from retail_tide.api.overview import (
    _history_metadata,
    topic_contents,
    topic_overview,
    topic_series,
)
from retail_tide.models import (
    Asset,
    CollectionTask,
    Content,
    ContentAnalysis,
    ContentEntity,
    EventMetricLink,
    MarketBar,
    MetricSignal,
    PlatformMetric,
    SignalEvent,
    Source,
    Topic,
)
from retail_tide.time import UTC


def test_builtin_dashboard_has_analysis_drilldown_and_refresh_paths():
    html = dashboard_html("overview")
    trends = dashboard_html("trends")
    posts = dashboard_html("posts")
    research = dashboard_html("research")

    assert "散户潮汐" in html
    assert "/dashboard" in html
    assert "/trends" in html
    assert "/posts" in html
    assert "/research" in html
    assert "截至所选日的散户情绪/热度趋势" in html
    assert "历史分位" in html
    assert "所选自然日" in html
    assert "方向与情绪排行" in html
    assert 'id="overviewDate"' in html
    assert "今日仍在采集中" in html
    assert "历史日期仅展示已经落库的帖子数量" in html
    assert "来源尚未齐备" not in html
    assert "未找到采集任务" not in html
    assert "赛道有记录" not in html
    assert "该日去重内容" in html
    assert "有当日日线 / 关联资产" in html
    assert "selectedDayPostsUrl" in html
    assert "chart-tooltip" in trends
    assert 'id="trendFrom"' in trends
    assert 'id="trendTo"' in trends
    assert 'id="applyTrendRange"' in trends
    assert "from_date: state.trendFrom" in trends
    assert "to_date: state.trendTo" in trends
    assert "coverage.window_days || 30" in trends
    assert 'class="hover-band"' in trends
    assert '"mouseenter", "mousemove"' in trends
    assert "pointer-events: all" in trends
    assert "viewXFromEvent" in trends
    assert "shellXFromViewX" in trends
    assert "getScreenCTM" in trends
    assert "event.stopPropagation()" in trends
    assert "showNearest" in trends
    assert "代表资产" in trends
    assert "自然日样本" in trends
    assert "历史分位已计算" in trends
    assert "个基线不足日未计算分位" in trends
    assert "这些是状态，不是分位数点" in trends
    assert "未计算（更早有效日" in trends
    assert "历史分位预热" not in trends
    assert "分位预热" not in trends
    assert "heatBridges" in trends
    assert "数据间断" in trends
    assert "Wikimedia 与赛道热度对比" in trends
    assert "Wikimedia 浏览量走势（窗口归一化）" in trends
    assert "内外关注共振" in trends
    assert 'optional("/trends/attention", { limit: 5000 }, [])' in trends
    assert "历史帖子" in posts
    assert 'id="postDate"' in posts
    assert '<option value="custom">指定自然日</option>' in posts
    assert "calendarDayBounds(state.postDate)" in posts
    assert 'id="postSource"' in posts
    assert 'postSourceNames = ["all", "xiaohongshu", "guba", "taoguba", "zhihu"]' in posts
    assert "参考交易日" in posts
    assert 'data-source="${esc(item.source_name)}"' in posts
    assert 'data-filter="${name}"' in posts
    assert '["all", "retail", "buy", "sell", "hold", "wait", "fomo", "panic", "promotion"]' in posts
    assert "全部历史" in posts
    assert "position: sticky" in posts
    assert "持有 · ${pct(analysis.intent_confidence)}" in posts
    assert "等待观察 · ${pct(analysis.intent_confidence)}" in posts
    assert "偏买倾向" in posts
    assert "偏卖倾向" in posts
    assert "topicOptions(state.postTopic, true)" in posts
    assert 'const path = allTopics ? "/contents"' in posts
    assert 'analysis.model || "未知模型"' not in posts
    assert 'explicit ? " open" : ""' in posts
    assert "意图/倾向判断依据（原文证据）" in posts
    assert "/research/event-study" in research
    assert "/research/quantile-study" in research
    assert "api(`/events/${id}`)" in research
    assert "RawObservation" in research
    assert "事件收益、指标分位与原始观测" in research
    assert "已落库证据" in research
    assert "高赞回答经交易日校验后进入实体识别与 LLM" in research
    assert "补全已知帖子的归档正文" in research
    assert "研究概览" in research
    assert "暂无异常事件" in research
    assert "样本 ${num(study.N)} / 20" in research
    assert 'xiaohongshu: "小红书"' in research
    assert 'setup.enabled === false ? "未启用"' in research
    assert "当前赛道关联原始内容" in research
    assert "打开原帖" in research
    assert ".slice(0, 12)" not in research
    assert "limit: 5000" in research
    assert "显示最近" in research
    assert 'event: "fomo_spike"' not in research
    assert "等待下一交易日" in research
    assert "前一自然日" in html
    assert "较前一桶" not in html
    assert "/topics/overview" in html
    assert "/topics/${topic.id}/contents" in posts

    removed_explanations = (
        "有趋势数据，但没有异常事件",
        "不是页面没加载",
        "基线预热样本（不是热度 0）",
        "虚线跨缺样本日",
        "中间缺失日没有数值",
        "这次复盘在说什么",
        "robust-z ≥ 3",
        "暂不拆成 Q1–Q5",
    )
    for phrase in removed_explanations:
        assert phrase not in trends
        assert phrase not in research


def test_dashboard_routes_are_registered():
    app = create_app()
    paths = {route.path for route in app.routes}

    assert "/" in paths
    assert "/dashboard" in paths
    assert "/trends" in paths
    assert "/posts" in paths
    assert "/research" in paths
    assert "/config/status" in paths
    assert "/topics/overview" in paths
    assert "/contents" in paths
    assert "/topics/{topic_id}/series" in paths
    assert "/topics/{topic_id}/contents" in paths


def _content(session, source, topic, item_id, published_at, *, kind="post", comments=0):
    row = Content(
        source_id=source.id,
        source_item_id=item_id,
        kind=kind,
        published_at=published_at,
        first_collected_at=published_at,
        last_seen_at=published_at,
        title=item_id,
        body="测试内容",
        comments=comments,
        content_hash=item_id.rjust(64, "0"),
        language="zh-CN",
    )
    session.add(row)
    session.flush()
    session.add(
        ContentEntity(
            content_id=row.id,
            entity_type="topic",
            entity_id=topic.id,
            method="collection_query",
            confidence=0.9,
            created_at=published_at,
        )
    )
    session.flush()
    return row


def _analysis(session, content, topic, *, intent, promotion=False):
    row = ContentAnalysis(
        content_id=content.id,
        topic_id=topic.id,
        model="test-model",
        prompt_version="test-v1",
        schema_version="test-v1",
        actor_type="retail",
        actor_confidence=0.9,
        investor_level="experienced",
        investor_confidence=0.9,
        direction="neutral",
        direction_confidence=0.9,
        intent=intent,
        intent_confidence=0.9,
        position="unknown",
        position_confidence=0.9,
        novice_signals=[],
        emotion_signals={},
        spam=False,
        spam_confidence=0.9,
        promotion=promotion,
        promotion_confidence=0.9 if promotion else 0.0,
        created_at=content.published_at + timedelta(minutes=1),
    )
    session.add(row)
    return row


def test_topic_contents_filters_every_visible_intent_tag(session, settings):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    topic = session.scalar(select(Topic).where(Topic.slug == "ai"))
    published_at = datetime(2026, 8, 18, 2, tzinfo=UTC)
    hold = _content(session, source, topic, "hold-filter", published_at)
    wait = _content(session, source, topic, "wait-filter", published_at + timedelta(minutes=2))
    promotion = _content(
        session, source, topic, "promotion-filter", published_at + timedelta(minutes=4)
    )
    _analysis(session, hold, topic, intent="hold")
    _analysis(session, wait, topic, intent="wait")
    _analysis(session, promotion, topic, intent="unknown", promotion=True)
    session.commit()

    all_rows = topic_contents(session, topic_id=topic.id, period="all")
    hold_rows = topic_contents(session, topic_id=topic.id, content_filter="hold", period="all")
    wait_rows = topic_contents(session, topic_id=topic.id, content_filter="wait", period="all")
    promotion_rows = topic_contents(
        session, topic_id=topic.id, content_filter="promotion", period="all"
    )

    assert all_rows["facets"] == {
        "all": 3,
        "retail": 2,
        "buy": 0,
        "sell": 0,
        "hold": 1,
        "wait": 1,
        "fomo": 0,
        "panic": 0,
        "promotion": 1,
    }
    assert hold_rows["items"][0]["source_item_id"] == "hold-filter"
    assert wait_rows["items"][0]["source_item_id"] == "wait-filter"
    assert promotion_rows["items"][0]["source_item_id"] == "promotion-filter"

    app = create_app(engine=session.bind, settings=settings)
    endpoint = next(
        route.endpoint for route in app.routes if route.path == "/topics/{topic_id}/contents"
    )
    response = endpoint(
        topic.id,
        bucket_size="1d",
        content_filter="hold",
        source_filter="all",
        period="all",
        from_at=None,
        to_at=None,
        limit=30,
        offset=0,
    )
    assert response["total"] == 1


def test_overview_batches_analyses_and_invalidates_historical_cache(session, settings):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    topic = session.scalar(select(Topic).where(Topic.slug == "ai"))
    published_at = datetime(2025, 1, 15, 2, tzinfo=UTC)
    for index in range(25):
        content = _content(
            session,
            source,
            topic,
            f"overview-performance-{index}",
            published_at + timedelta(minutes=index),
        )
        _analysis(session, content, topic, intent="buy")
    session.commit()

    app = create_app(engine=session.bind, settings=settings)
    assert any(middleware.cls.__name__ == "GZipMiddleware" for middleware in app.user_middleware)
    assert session.execute(text("PRAGMA journal_mode")).scalar_one() == "wal"
    endpoint = next(route.endpoint for route in app.routes if route.path == "/topics/overview")
    statement_count = 0

    def count_statement(*_args):
        nonlocal statement_count
        statement_count += 1

    sqlalchemy_event.listen(session.bind, "before_cursor_execute", count_statement)
    try:
        first = endpoint(
            bucket_size="1d",
            date=date(2025, 1, 15),
            from_date=None,
            to_date=None,
        )
        first_statement_count = statement_count
        statement_count = 0
        second = endpoint(
            bucket_size="1d",
            date=date(2025, 1, 15),
            from_date=None,
            to_date=None,
        )
        cached_statement_count = statement_count

        content = _content(
            session,
            source,
            topic,
            "overview-cache-invalidation",
            published_at + timedelta(hours=1),
        )
        _analysis(session, content, topic, intent="sell")
        session.commit()
        statement_count = 0
        refreshed = endpoint(
            bucket_size="1d",
            date=date(2025, 1, 15),
            from_date=None,
            to_date=None,
        )
    finally:
        sqlalchemy_event.remove(session.bind, "before_cursor_execute", count_statement)

    assert first_statement_count <= 20
    assert cached_statement_count == 0
    assert second["coverage"]["content_count"] == first["coverage"]["content_count"]
    assert refreshed["coverage"]["content_count"] == first["coverage"]["content_count"] + 1
    assert statement_count <= 20


def test_topic_contents_filters_and_paginates_in_sql(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    topic = session.scalar(select(Topic).where(Topic.slug == "ai"))
    published_at = datetime(2025, 1, 16, 2, tzinfo=UTC)
    for index in range(40):
        content = _content(
            session,
            source,
            topic,
            f"content-pagination-{index}",
            published_at + timedelta(minutes=index),
        )
        _analysis(session, content, topic, intent="buy" if index % 2 else "sell")
    session.commit()

    statement_count = 0

    def count_statement(*_args):
        nonlocal statement_count
        statement_count += 1

    sqlalchemy_event.listen(session.bind, "before_cursor_execute", count_statement)
    try:
        result = topic_contents(
            session,
            topic_id=topic.id,
            content_filter="buy",
            period="all",
            limit=10,
        )
    finally:
        sqlalchemy_event.remove(session.bind, "before_cursor_execute", count_statement)

    assert result["total"] == 20
    assert len(result["items"]) == 10
    assert statement_count <= 8


def test_daily_coverage_separates_analysis_completion_from_partial_day_task(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    topic = session.scalar(select(Topic).where(Topic.slug == "ai"))
    published_at = datetime(2026, 8, 18, 2, tzinfo=UTC)
    content = _content(session, source, topic, "partial-day", published_at)
    _analysis(session, content, topic, intent="hold")
    session.add(
        CollectionTask(
            source_id=source.id,
            topic_id=topic.id,
            run_key="partial-day-run",
            query="人工智能",
            query_fingerprint="partial-day".rjust(64, "0"),
            window_start=datetime(2026, 8, 17, 16, tzinfo=UTC),
            window_end=datetime(2026, 8, 18, 4, tzinfo=UTC),
            explicit_window=True,
            page_limit=10,
            status="complete",
            attempts=1,
            pages=1,
            items_collected=1,
            duplicates=0,
            topic_links_added=1,
            created_at=published_at,
            updated_at=published_at,
        )
    )
    session.commit()

    coverage = topic_overview(
        session,
        selected_date=date(2026, 8, 18),
        expected_sources=("guba",),
    )["coverage"]

    assert coverage["analysis_complete"] is True
    assert coverage["analysis_status"] == "complete"
    assert coverage["collection_status"] == "partial"
    assert coverage["is_complete"] is False
    assert coverage["sources"][0]["status"] == "window_partial"
    assert coverage["sources"][0]["recorded_topics"] == 1
    assert coverage["sources"][0]["recorded_window_end"] == datetime(2026, 8, 18, 4, tzinfo=UTC)


def test_source_status_exposes_persisted_evidence_counts(session, settings):
    app = create_app(engine=session.bind, settings=settings)
    endpoint = next(route.endpoint for route in app.routes if route.path == "/sources/status")

    response = endpoint()

    rows = {row["name"]: row for row in response}
    assert rows["zhihu"]["evidence"]["raw_observation_count"] == 0
    assert rows["wikimedia-pageviews"]["evidence"]["trend_signal_count"] == 0
    assert rows["common-crawl"]["evidence"]["archive_snapshot_count"] == 0
    assert rows["common-crawl"]["evidence"]["archive_status_counts"] == {}


def test_topic_contents_can_filter_and_count_sources(session):
    guba = session.scalar(select(Source).where(Source.name == "guba"))
    xiaohongshu = session.scalar(select(Source).where(Source.name == "xiaohongshu"))
    topic = session.scalar(select(Topic).where(Topic.slug == "ai"))
    published_at = datetime(2026, 8, 18, 2, tzinfo=UTC)
    _content(session, guba, topic, "guba-source-filter", published_at)
    expected = _content(
        session,
        xiaohongshu,
        topic,
        "xiaohongshu-source-filter",
        published_at + timedelta(minutes=1),
    )
    session.commit()

    result = topic_contents(
        session,
        topic_id=topic.id,
        source_name="xiaohongshu",
        period="all",
    )

    assert result["source"] == "xiaohongshu"
    assert result["source_facets"] == {"guba": 1, "xiaohongshu": 1}
    assert result["facets"]["all"] == 1
    assert result["total"] == 1
    assert result["items"][0]["id"] == expected.id
    assert result["items"][0]["source_name"] == "xiaohongshu"
    assert result["items"][0]["published_at"].utcoffset() == timedelta(0)


def test_all_topic_contents_includes_every_track_without_duplicates(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    ai = session.scalar(select(Topic).where(Topic.slug == "ai"))
    liquor = session.scalar(select(Topic).where(Topic.slug == "liquor"))
    published_at = datetime(2026, 8, 18, 2, tzinfo=UTC)
    shared = _content(session, source, ai, "all-topic-shared", published_at)
    session.add(
        ContentEntity(
            content_id=shared.id,
            entity_type="topic",
            entity_id=liquor.id,
            method="alias",
            confidence=0.8,
            created_at=published_at,
        )
    )
    unclassified = Content(
        source_id=source.id,
        source_item_id="all-topic-unclassified",
        kind="post",
        published_at=published_at + timedelta(minutes=1),
        first_collected_at=published_at,
        last_seen_at=published_at,
        title="未归类内容",
        body="测试内容",
        content_hash="unclassified".rjust(64, "0"),
        language="zh-CN",
    )
    session.add(unclassified)
    session.commit()

    result = topic_contents(session, topic_id=None, period="all", limit=10)

    assert result["topic_id"] is None
    assert result["total"] == 2
    assert {item["id"] for item in result["items"]} == {shared.id, unclassified.id}


def test_event_drilldown_only_returns_contents_from_event_topic(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    topic = session.scalar(select(Topic).where(Topic.slug == "ai"))
    other_topic = session.scalar(select(Topic).where(Topic.slug == "liquor"))
    bucket_at = datetime(2026, 8, 14, tzinfo=UTC)
    expected = _content(session, source, topic, "ai-event", bucket_at + timedelta(hours=1))
    _content(session, source, other_topic, "liquor-same-bucket", bucket_at + timedelta(hours=2))
    metric = PlatformMetric(
        bucket_at=bucket_at,
        bucket_size="1d",
        source_id=source.id,
        topic_id=topic.id,
        created_at=bucket_at,
    )
    session.add(metric)
    session.flush()
    signal = MetricSignal(
        platform_metric_id=metric.id,
        metric_name="post_count",
        raw_value=1,
        baseline_window="30d",
        metric_version="test-v1",
        created_at=bucket_at,
    )
    session.add(signal)
    session.flush()
    event = SignalEvent(
        source_id=source.id,
        topic_id=topic.id,
        event_type="attention_spike",
        started_at=bucket_at,
        peaked_at=bucket_at,
        peak_value=1,
        rule_version="test-v1",
        status="discovered",
        created_at=bucket_at,
        trigger_metric_id=metric.id,
    )
    session.add(event)
    session.flush()
    session.add(EventMetricLink(event_id=event.id, metric_signal_id=signal.id))
    session.commit()

    result = _event_json(session, event, include_raw=True)

    assert [row["content"]["id"] for row in result["raw_drilldown"]] == [expected.id]
    assert result["raw_drilldown"][0]["content"]["published_at"].utcoffset() == timedelta(0)
    assert result["raw_drilldown_limit"] == 50


def test_topic_overview_compares_all_topics_in_latest_bucket(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    topic = session.scalar(select(Topic).where(Topic.slug == "ai"))
    _content(session, source, topic, "ai-previous", datetime(2026, 8, 13, 6, tzinfo=UTC))
    _content(
        session,
        source,
        topic,
        "ai-previous-after-current-cutoff",
        datetime(2026, 8, 13, 8, tzinfo=UTC),
    )
    current_post = _content(
        session,
        source,
        topic,
        "ai-current-post",
        datetime(2026, 8, 14, 6, tzinfo=UTC),
        comments=4,
    )
    _content(
        session,
        source,
        topic,
        "ai-current-comment",
        datetime(2026, 8, 14, 7, tzinfo=UTC),
        kind="comment",
    )
    session.add(
        ContentAnalysis(
            content_id=current_post.id,
            model="test-model",
            prompt_version="test-v1",
            schema_version="test-v1",
            actor_type="retail",
            actor_confidence=0.9,
            investor_level="novice",
            investor_confidence=0.9,
            direction="bullish",
            direction_confidence=0.9,
            intent="buy",
            intent_confidence=0.9,
            position="not_owned",
            position_confidence=0.9,
            novice_signals=["first_time"],
            emotion_signals={
                "urgency": True,
                "fear_of_missing": True,
                "social_proof": True,
                "price_chasing": False,
                "regret": False,
                "panic": False,
            },
            spam=False,
            spam_confidence=0.9,
            created_at=datetime(2026, 8, 14, 8, tzinfo=UTC),
        )
    )
    ai_asset = session.scalar(select(Asset).where(Asset.symbol == "159869"))
    session.add(
        MarketBar(
            asset_id=ai_asset.id,
            interval="1d",
            ts=datetime(2026, 8, 14, 6, tzinfo=UTC),
            open=1.0,
            high=1.1,
            low=0.9,
            close=1.05,
            volume=100,
            amount=105,
            adjustment="none",
            provider="test",
        )
    )
    semiconductor = session.scalar(select(Topic).where(Topic.slug == "semiconductor"))
    session.add(
        ContentEntity(
            content_id=current_post.id,
            entity_type="topic",
            entity_id=semiconductor.id,
            method="alias",
            confidence=0.8,
            created_at=datetime(2026, 8, 14, 8, tzinfo=UTC),
        )
    )
    session.commit()

    result = topic_overview(session)
    selected_result = topic_overview(session, selected_date=date(2026, 8, 13))
    ranged_result = topic_overview(
        session,
        selected_date=date(2026, 8, 14),
        history_start_date=date(2026, 8, 13),
    )
    series = topic_series(session, topic_id=topic.id)
    contents = topic_contents(session, topic_id=topic.id, limit=2)
    historical_contents = topic_contents(session, topic_id=topic.id, period="all", limit=10)
    buy_contents = topic_contents(session, topic_id=topic.id, content_filter="buy", limit=2)
    fomo_contents = topic_contents(session, topic_id=topic.id, content_filter="fomo", limit=2)
    ai = next(row for row in result["topics"] if row["slug"] == "ai")
    no_data = next(row for row in result["topics"] if row["slug"] == "liquor")
    selected_ai = next(row for row in selected_result["topics"] if row["slug"] == "ai")
    ranged_ai = next(row for row in ranged_result["topics"] if row["slug"] == "ai")

    assert len(result["topics"]) == 10
    assert result["bucket_at"] == datetime(2026, 8, 13, 16, tzinfo=UTC)
    assert result["comparison_mode"] == "calendar_day_asia_shanghai"
    assert selected_result["selected_date"] == "2026-08-13"
    assert selected_result["bucket_at"] == datetime(2026, 8, 12, 16, tzinfo=UTC)
    assert selected_result["data_cutoff_at"] == datetime(2026, 8, 13, 8, tzinfo=UTC)
    assert selected_result["coverage"]["content_count"] == 2
    assert selected_result["coverage"]["indexed_content_count"] == 2
    assert selected_result["coverage"]["analyzed_content_count"] == 0
    assert selected_result["coverage"]["is_complete"] is False
    assert selected_result["coverage"]["sources"][0]["name"] == "guba"
    assert selected_result["coverage"]["sources"][0]["content_count"] == 2
    assert selected_result["coverage"]["sources"][0]["status"] == "observed_untracked"
    assert selected_ai["attention"] == 2
    assert selected_ai["asset"]["selected_day_bar"] is None
    assert ranged_ai["history_coverage"]["window_days"] == 2
    assert ranged_ai["history_coverage"]["observed_days"] == 2
    assert [row["bucket_at"] for row in ranged_ai["history"]] == [
        datetime(2026, 8, 12, 16, tzinfo=UTC),
        datetime(2026, 8, 13, 16, tzinfo=UTC),
    ]
    with pytest.raises(ValueError, match="cannot be after"):
        topic_overview(
            session,
            selected_date=date(2026, 8, 14),
            history_start_date=date(2026, 8, 15),
        )
    assert ai["attention"] == 2
    assert ai["previous_attention"] == 2
    assert ai["change_ratio"] == 0.0
    assert ai["source_count"] == 1
    assert ai["engagement_sum"] == 4
    assert ai["retail_count"] == 1
    assert ai["retail_ratio"] == 1 / 2
    assert ai["fomo_count"] == 1
    assert ai["buy_intent_count"] == 1
    assert ai["sell_intent_count"] == 0
    assert ai["buy_intent_ratio"] == 1.0
    assert ai["analyzed_count"] == 1
    assert ai["analysis_coverage"] == 1 / 2
    assert len(ai["history"]) == 2
    assert ai["heat_sample_days"] == 1
    assert ai["heat_score"] == 58.7
    assert ai["daily_index"] == ai["heat_score"]
    assert ai["daily_index_confidence"] == {"value": "low", "label": "低置信度"}
    assert ai["historical_percentile"] is None
    assert ai["history_coverage"]["observed_days"] == 2
    assert ai["history_coverage"]["analyzed_days"] == 1
    assert ai["history_coverage"]["index_days"] == 1
    assert ai["history_coverage"]["percentile_days"] == 0
    assert ai["history_coverage"]["warming_up_days"] == 1
    assert ai["history"][-1]["heat_status"] == "warming_up"
    assert ai["history"][-1]["baseline_sample_days"] == 0
    assert ai["trend_windows"]["today"]["retail_count"] == 1
    assert ai["trend_summary"]["direction"] == "insufficient"
    assert ai["asset"]["symbol"] == "159869"
    assert ai["assets"][0]["symbol"] == "159869"
    assert ai["asset"]["price_history"][0]["close"] == 1.05
    assert result["market"]["attention"] == 2
    assert result["market"]["retail_count"] == 1
    assert len(result["market"]["history"]) == 2
    assert no_data["attention"] == 0
    assert no_data["trend"] == "no_data"
    assert series[0]["attention"] == 2
    assert series[0]["source_count"] == ai["source_count"]
    assert contents["total"] == ai["attention"]
    assert contents["period"] == "latest"
    assert contents["facets"] == {
        "all": 2,
        "retail": 1,
        "buy": 1,
        "sell": 0,
        "hold": 0,
        "wait": 0,
        "fomo": 1,
        "panic": 0,
        "promotion": 0,
    }
    assert len(contents["items"]) == 2
    analyzed = next(
        item for item in contents["items"] if item["source_item_id"] == "ai-current-post"
    )
    assert analyzed["source_name"] == "guba"
    assert analyzed["body"] == "测试内容"
    assert analyzed["analysis"]["model"] == "test-model"
    assert analyzed["analysis"]["intent_confidence"] == 0.9
    assert analyzed["analysis"]["fomo"] is True
    assert buy_contents["total"] == 1
    assert buy_contents["items"][0]["source_item_id"] == "ai-current-post"
    assert fomo_contents["total"] == 1
    assert fomo_contents["items"][0]["source_item_id"] == "ai-current-post"
    assert historical_contents["total"] == 4
    assert historical_contents["period"] == "all"


def test_multihorizon_trend_keeps_daily_index_and_historical_percentile_separate():
    start = datetime(2026, 5, 20, tzinfo=UTC)
    retail_counts = [100] * 30 + [10] * 30 + list(range(20, 50))
    history = [
        {
            "bucket_at": start + timedelta(days=index),
            "analyzed_count": 1,
            "retail_count": retail_count,
            "attention": retail_count,
        }
        for index, retail_count in enumerate(retail_counts)
    ]

    result = _history_metadata(
        history,
        current_bucket_at=history[-1]["bucket_at"],
        delta=timedelta(days=1),
    )

    assert result["heat_score"] == 52.8
    assert result["daily_index"] == 52.8
    assert result["historical_percentile"] == 100.0
    assert result["history_coverage"]["observed_days"] == 30
    assert result["history_coverage"]["index_days"] == 30
    assert result["history_coverage"]["status"] == "complete"
    assert result["trend_windows"]["today"]["index"] == 52.8
    assert result["trend_windows"]["7d"]["sample_days"] == 7
    assert result["trend_windows"]["30d"]["sample_days"] == 30
    assert result["trend_windows"]["30d"]["change_points"] > 0
    assert result["trend_summary"]["direction"] == "stable"
    assert result["trend_summary"]["confidence"] == "high"
    assert set(result["trend_summary"]["components"]) == {
        "today_vs_yesterday",
        "current_7d_vs_previous_7d",
        "current_30d_vs_previous_30d",
    }
    assert len(result["history"]) == 30
