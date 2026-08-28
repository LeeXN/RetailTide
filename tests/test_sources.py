from __future__ import annotations

import asyncio
import gzip
import json
import time
from dataclasses import replace
from datetime import datetime, timedelta

import httpx
import pytest
from sqlalchemy import select

import retail_tide.jobs.jobs as jobs_module
import retail_tide.sources.guba as guba_module
from retail_tide.config import SourceCredential
from retail_tide.jobs import collect_source_async
from retail_tide.jobs.jobs import (
    _observation_in_window,
    record_collection_checkpoint,
    resolve_incremental_window,
)
from retail_tide.models import (
    CollectionCheckpoint,
    CollectionTask,
    Content,
    ContentEntity,
    RawObservation,
    RawObservationTopic,
    Source,
    Topic,
    TrendObservation,
)
from retail_tide.pipeline import resolve_pending_entities
from retail_tide.pipeline.entities import is_market_relevant_content, is_market_relevant_text
from retail_tide.pipeline.normalize import (
    insert_raw_observation,
    link_raw_observation_topic,
    normalize_pending,
    normalize_raw_observation,
)
from retail_tide.schemas import CollectResult
from retail_tide.schemas import RawObservation as RawObservationSchema
from retail_tide.source_sessions import import_source_session
from retail_tide.sources import (
    CommonCrawlSource,
    GubaSource,
    TaogubaSource,
    WikimediaPageviewsSource,
    XiaohongshuSource,
    ZhihuSource,
    parse_paged_response,
)
from retail_tide.sources.base import RequestRateLimiter, SourceError, public_get
from retail_tide.sources.guba import parse_guba_page, select_guba_boards
from retail_tide.sources.xiaohongshu import (
    _evenly_sample_candidates,
    _newest_page_reached_since,
    _publish_time_filter,
    xiaohongshu_spider_cursor,
    xiaohongshu_strategy_cursor,
)
from retail_tide.sources.zhihu import (
    zhihu_answer_reference_eligibility,
    zhihu_market_question_query,
)
from retail_tide.time import UTC, now_utc


def test_incremental_watermark_uses_24h_then_two_hour_overlap(session):
    topic = session.scalar(select(Topic).where(Topic.slug == "gold"))
    current = datetime(2026, 8, 20, 4, 0, tzinfo=UTC)
    first_start, first_end, explicit = resolve_incremental_window(
        session,
        "guba",
        query="黄金",
        topic=topic,
        now=current,
    )
    assert explicit is False
    assert first_start == current - timedelta(hours=24)
    assert first_end == current

    record_collection_checkpoint(
        session,
        source_name="guba",
        query="黄金",
        topic=topic,
        until=current,
        result={"source_degraded": False, "source_partial": False},
        explicit_window=False,
    )
    second_start, second_end, explicit = resolve_incremental_window(
        session,
        "guba",
        query="黄金",
        topic=topic,
        now=current + timedelta(hours=1),
    )
    assert explicit is False
    assert second_start == current - timedelta(hours=2)
    assert second_end == current + timedelta(hours=1)
    checkpoint = session.scalar(select(CollectionCheckpoint))
    assert checkpoint.last_successful_until == current

    explicit_start, explicit_end, is_explicit = resolve_incremental_window(
        session,
        "guba",
        query="黄金",
        topic=topic,
        since=current - timedelta(days=30),
        until=current,
    )
    assert is_explicit is True
    assert explicit_end - explicit_start == timedelta(days=30)


def test_all_topic_retry_skips_completed_and_resumes_failed_cursor(session, settings, monkeypatch):
    calls: list[dict] = []

    def first_run(_session, _source, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return {
                "source": "guba",
                "pages": 1,
                "items_collected": 4,
                "duplicates": 0,
                "topic_links_added": 4,
                "source_degraded": False,
                "source_partial": False,
                "warnings": [],
                "exhausted": True,
                "next_cursor": None,
            }
        return {
            "source": "guba",
            "pages": 2,
            "items_collected": 6,
            "duplicates": 0,
            "topic_links_added": 6,
            "source_degraded": True,
            "source_partial": False,
            "warnings": [],
            "error": "guba returned an identity-verification page",
            "exhausted": False,
            "next_cursor": "resume-page-3",
        }

    monkeypatch.setattr(jobs_module, "collect_source", first_run)
    start = datetime(2026, 8, 19, tzinfo=UTC)
    end = datetime(2026, 8, 20, tzinfo=UTC)
    first = jobs_module.collect_active_topics(
        session,
        source_names=["guba"],
        since=start,
        until=end,
        settings=settings,
        resume_key="daily-window-2026-08-20",
    )

    assert len(calls) == 2
    assert first[0]["resume"]["status"] == "complete"
    assert first[1]["resume"]["status"] == "degraded"
    assert all(row.get("collection_skipped") for row in first[2:])

    failed = session.scalar(select(CollectionTask).where(CollectionTask.status == "degraded"))
    failed.next_retry_at = now_utc() - timedelta(seconds=1)
    session.commit()
    calls.clear()

    def resumed_run(_session, _source, **kwargs):
        calls.append(kwargs)
        return {
            "source": "guba",
            "pages": 1,
            "items_collected": 2,
            "duplicates": 0,
            "topic_links_added": 2,
            "source_degraded": False,
            "source_partial": False,
            "warnings": [],
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source", resumed_run)
    second = jobs_module.collect_active_topics(
        session,
        source_names=["guba"],
        since=start,
        until=end,
        settings=settings,
        resume_key="daily-window-2026-08-20",
    )

    assert second[0]["skip_reason"] == "checkpoint_complete"
    assert len(calls) == 9
    assert calls[0]["start_cursor"] == "resume-page-3"
    assert all(row["resume"]["status"] == "complete" for row in second)


def test_all_topic_partial_result_cools_down_remaining_source_jobs(session, settings, monkeypatch):
    calls: list[str] = []

    def partial_run(_session, _source, **kwargs):
        calls.append(kwargs["query"])
        return {
            "source": "xiaohongshu",
            "pages": 1,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_degraded": False,
            "source_partial": True,
            "warnings": ["upstream fallback returned incomplete evidence"],
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source", partial_run)
    results = jobs_module.collect_active_topics(
        session,
        source_names=["xiaohongshu"],
        since=datetime(2026, 8, 19, tzinfo=UTC),
        until=datetime(2026, 8, 20, tzinfo=UTC),
        settings=settings,
        resume_key="partial-window-2026-08-20",
    )

    assert calls == ["黄金 金价 ETF 投资"]
    assert results[0]["resume"]["status"] == "partial"
    assert all(row.get("collection_skipped") for row in results[1:])
    assert all(row.get("skip_reason") == "source_cooldown_after_failure" for row in results[1:])

    calls.clear()
    deferred = jobs_module.collect_active_topics(
        session,
        source_names=["xiaohongshu"],
        since=datetime(2026, 8, 19, tzinfo=UTC),
        until=datetime(2026, 8, 20, tzinfo=UTC),
        settings=settings,
        resume_key="partial-window-2026-08-20",
    )

    assert calls == []
    assert deferred[0]["skip_reason"] == "retry_cooldown"
    assert all(row.get("collection_skipped") for row in deferred)


def test_source_cooldown_is_shared_across_collection_windows(session, settings, monkeypatch):
    calls: list[str] = []

    def degraded_run(_session, _source, **kwargs):
        calls.append(kwargs["query"])
        return {
            "source": "guba",
            "pages": 0,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_degraded": True,
            "source_partial": False,
            "warnings": [],
            "error": "guba returned an identity-verification page",
            "exhausted": False,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source", degraded_run)
    first = jobs_module.collect_active_topics(
        session,
        source_names=["guba"],
        since=datetime(2026, 8, 19, tzinfo=UTC),
        until=datetime(2026, 8, 20, tzinfo=UTC),
        settings=settings,
        resume_key="source-cooldown-window-a",
    )
    assert calls == ["黄金"]
    assert first[0]["source_degraded"] is True

    second = jobs_module.collect_active_topics(
        session,
        source_names=["guba"],
        since=datetime(2026, 8, 20, tzinfo=UTC),
        until=datetime(2026, 8, 21, tzinfo=UTC),
        settings=settings,
        resume_key="source-cooldown-window-b",
    )

    assert calls == ["黄金"]
    assert second[0]["skip_reason"] == "source_retry_cooldown"
    assert second[0]["source_degraded"] is True
    assert second[0]["resume"]["next_retry_at"] is not None
    assert all(row.get("collection_skipped") for row in second)


def test_zhihu_all_topics_creates_only_three_market_review_jobs(session, settings, monkeypatch):
    queries: list[str] = []

    def collect_market_answer(_session, _source, **kwargs):
        queries.append(kwargs["query"])
        return {
            "source": "zhihu",
            "pages": 1,
            "items_collected": 1,
            "duplicates": 0,
            "topic_links_added": 1,
            "source_degraded": False,
            "source_partial": False,
            "warnings": [],
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source", collect_market_answer)
    results = jobs_module.collect_active_topics(
        session,
        source_names=["zhihu"],
        since=datetime(2026, 8, 19, tzinfo=UTC),
        until=datetime(2026, 8, 20, tzinfo=UTC),
        settings=settings,
        resume_key="zhihu-markets-2026-08-19",
    )

    assert len(results) == 3
    assert {row["topic_slug"] for row in results} == {
        "broad-a-share",
        "hang-seng-tech",
        "nasdaq",
    }
    assert set(queries) == {
        "如何看待2026年8月19日A股市场行情走势？",
        "如何看待2026年8月19日港股市场行情走势？",
        "如何看待2026年8月19日美股市场行情走势？",
    }


@pytest.mark.asyncio
async def test_wikimedia_pageviews_maps_daily_attention_without_text_analysis():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/daily/20260819/20260819")
        assert request.headers["user-agent"].startswith("RetailTide/")
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "timestamp": "2026081900",
                        "views": 1234,
                        "access": "all-access",
                        "agent": "user",
                        "granularity": "daily",
                    },
                    {
                        "timestamp": "2026082000",
                        "views": 9999,
                    },
                ]
            },
        )

    source = WikimediaPageviewsSource(
        user_agent="RetailTide/0.1 (ops@example.com)",
        transport=httpx.MockTransport(handler),
        min_interval=0,
    )
    result = await source.collect(
        "zh.wikipedia.org|黄金",
        datetime(2026, 8, 19, tzinfo=UTC),
        until=datetime(2026, 8, 20, tzinfo=UTC),
    )

    assert len(result.items) == 1
    item = result.items[0]
    assert item.observation_kind == "pageviews"
    assert item.payload["value"] == 1234.0
    assert item.payload["project"] == "zh.wikipedia.org"
    assert item.published_at == datetime(2026, 8, 19, tzinfo=UTC)


@pytest.mark.asyncio
async def test_wikimedia_unpublished_utc_bucket_remains_partial():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        assert "/metrics/pageviews/aggregate/" in request.url.path
        assert request.url.path.endswith("/daily/20260831/20260831")
        return httpx.Response(404)

    source = WikimediaPageviewsSource(
        user_agent="RetailTide/0.1 (ops@example.com)",
        transport=httpx.MockTransport(handler),
        min_interval=0,
    )
    result = await source.collect(
        "zh.wikipedia.org|黄金",
        datetime(2026, 8, 30, 16, tzinfo=UTC),
        until=datetime(2026, 8, 31, 16, tzinfo=UTC),
    )

    assert len(requests) == 1
    assert result.items == []
    assert result.partial is True
    assert result.exhausted is False
    assert result.diagnostics["expected_utc_dates"] == ["2026-08-31"]
    assert result.diagnostics["pending_utc_dates"] == ["2026-08-31"]
    assert result.diagnostics["availability_pending"] is True


@pytest.mark.asyncio
async def test_wikimedia_loaded_missing_article_bucket_is_recorded_as_zero():
    def handler(request: httpx.Request) -> httpx.Response:
        if "/metrics/pageviews/aggregate/" in request.url.path:
            return httpx.Response(
                200,
                json={"items": [{"timestamp": "2026083100", "views": 123456}]},
            )
        return httpx.Response(404)

    source = WikimediaPageviewsSource(
        user_agent="RetailTide/0.1 (ops@example.com)",
        transport=httpx.MockTransport(handler),
        min_interval=0,
    )
    result = await source.collect(
        "zh.wikipedia.org|低流量词条",
        datetime(2026, 8, 30, 16, tzinfo=UTC),
        until=datetime(2026, 8, 31, 16, tzinfo=UTC),
    )

    assert result.partial is False
    assert result.exhausted is True
    assert len(result.items) == 1
    assert result.items[0].published_at == datetime(2026, 8, 31, tzinfo=UTC)
    assert result.items[0].payload["value"] == 0
    assert result.items[0].payload["provider_missing_as_zero"] is True


def test_wikimedia_availability_wait_does_not_consume_retry_budget():
    job = {
        "attempts": 0,
        "pages": 0,
        "items_collected": 0,
        "duplicates": 0,
        "topic_links_added": 0,
        "diagnostics": {},
        "retries": 0,
        "done": False,
        "terminal": False,
    }
    jobs_module._apply_backfill_job_result(
        job,
        {"source": "wikimedia-pageviews", "initial_cursor": None},
        {
            "source_degraded": False,
            "source_partial": True,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "warnings": ["UTC bucket pending"],
            "diagnostics": {
                "availability_pending": True,
                "pending_utc_dates": ["2026-08-31"],
            },
        },
        current_cursor=None,
        source_retry_at={},
        unavailable_sources=set(),
        max_retries=1,
    )

    assert job["done"] is False
    assert job["terminal"] is False
    assert job["retries"] == 0
    assert job["availability_waits"] == 1
    assert job["error_code"] == "upstream_data_pending"
    assert job["next_retry_at"] is not None


def test_normalize_pending_can_limit_work_to_wikimedia(session, settings):
    wikimedia = session.scalar(select(Source).where(Source.name == "wikimedia-pageviews"))
    guba = session.scalar(select(Source).where(Source.name == "guba"))
    topic = session.scalar(select(Topic).where(Topic.slug == "gold"))
    wiki_raw, _ = insert_raw_observation(
        session,
        wikimedia.id,
        RawObservationSchema(
            source="wikimedia-pageviews",
            source_item_id="zh.wikipedia.org:黄金:2026083100:user",
            observation_kind="pageviews",
            published_at="2026-08-31T00:00:00Z",
            observed_at="2026-09-01T02:48:00Z",
            payload={"keyword": "黄金", "value": 20, "unit": "views"},
        ),
    )
    link_raw_observation_topic(
        session,
        wiki_raw,
        topic_id=topic.id,
        collection_query="黄金",
    )
    insert_raw_observation(
        session,
        guba.id,
        RawObservationSchema(
            source="guba",
            source_item_id="pending-content",
            observation_kind="forum_post",
            published_at="2026-08-31T01:00:00Z",
            observed_at="2026-08-31T02:00:00Z",
            payload={"body": "这条帖子不应由 Wikimedia timer 规范化"},
        ),
    )
    session.commit()

    assert (
        normalize_pending(
            session,
            limit=10,
            settings=settings,
            source_names={"wikimedia-pageviews"},
        )
        == 1
    )
    assert len(session.scalars(select(TrendObservation)).all()) == 1
    assert session.scalars(select(Content)).all() == []


@pytest.mark.asyncio
async def test_wikimedia_timeout_keeps_exception_type_in_error(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("", request=request)

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr("retail_tide.sources.base.asyncio.sleep", no_sleep)
    source = WikimediaPageviewsSource(
        user_agent="RetailTide/0.1 (ops@example.com)",
        transport=httpx.MockTransport(handler),
        min_interval=0,
    )

    with pytest.raises(SourceError, match=r"ReadTimeout"):
        await source.collect(
            "zh.wikipedia.org|黄金",
            datetime(2026, 8, 28, tzinfo=UTC),
            until=datetime(2026, 8, 29, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_common_crawl_lookup_and_warc_body_are_archive_only():
    html = "<html><title>Archived</title><body>黄金 股票 讨论</body></html>".encode()
    http_payload = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n" + html
    warc = (
        b"WARC/1.0\r\nWARC-Type: response\r\nContent-Length: "
        + str(len(http_payload)).encode()
        + b"\r\n\r\n"
        + http_payload
    )
    compressed = gzip.compress(warc)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "index.commoncrawl.org":
            assert request.url.params["matchType"] == "exact"
            assert request.url.params["url"] == "https://example.test/post"
            return httpx.Response(
                200,
                text=json.dumps(
                    {
                        "url": "https://example.test/post?utm_source=x",
                        "timestamp": "20260819010203",
                        "digest": "sha256:fixture",
                        "filename": "fixture.warc.gz",
                        "offset": 0,
                        "length": len(compressed),
                        "status": "200",
                        "mime": "text/html",
                    }
                ),
            )
        assert request.url.host == "data.commoncrawl.org"
        assert request.headers["range"] == f"bytes=0-{len(compressed) - 1}"
        return httpx.Response(206, content=compressed)

    source = CommonCrawlSource(
        user_agent="RetailTide/0.1 (ops@example.com)",
        transport=httpx.MockTransport(handler),
        min_interval=0,
    )
    capture = await source.lookup_url(
        "https://example.test/post?utm_source=x", crawl_id="CC-MAIN-2026-01"
    )
    assert capture is not None
    assert capture.captured_at == datetime(2026, 8, 19, 1, 2, 3, tzinfo=UTC)
    fetched = await source.fetch_body(capture)
    assert fetched.body == "Archived黄金 股票 讨论"
    assert fetched.body_truncated is False


@pytest.mark.asyncio
async def test_common_crawl_index_404_is_a_normal_missing_capture():
    source = CommonCrawlSource(
        user_agent="RetailTide/0.1 (ops@example.com)",
        transport=httpx.MockTransport(lambda _request: httpx.Response(404)),
        min_interval=0,
    )

    capture = await source.lookup_url(
        "https://example.test/not-archived", crawl_id="CC-MAIN-2026-01"
    )

    assert capture is None


@pytest.mark.asyncio
async def test_common_crawl_queue_pauses_after_first_source_failure(session, settings, monkeypatch):
    settings = replace(
        settings,
        http_user_agent="RetailTide/0.1 (ops@example.com)",
    )
    source = session.scalar(select(Source).where(Source.name == "guba"))
    published_at = datetime(2026, 8, 23, 2, 0, tzinfo=UTC)
    for index in range(2):
        session.add(
            Content(
                source_id=source.id,
                source_item_id=f"archive-candidate-{index}",
                kind="post",
                published_at=published_at,
                first_collected_at=published_at,
                last_seen_at=published_at,
                title=f"candidate {index}",
                body="",
                url=f"https://example.test/post/{index}",
                content_hash=f"{index:064d}",
                language="zh-CN",
            )
        )
    session.commit()
    lookups: list[str] = []

    async def crawl_ids(_self):
        return ["CC-MAIN-2026-01"]

    async def failed_lookup(_self, url, *, crawl_id=None):
        lookups.append(url)
        raise SourceError("archive transport disconnected")

    monkeypatch.setattr(CommonCrawlSource, "crawl_ids", crawl_ids)
    monkeypatch.setattr(CommonCrawlSource, "lookup_url", failed_lookup)

    result = await jobs_module.enrich_common_crawl_async(
        session,
        since=datetime(2026, 8, 23, tzinfo=UTC),
        until=datetime(2026, 8, 24, tzinfo=UTC),
        settings=settings,
        limit=10,
    )

    assert len(lookups) == 1
    assert result["source_degraded"] is True
    assert result["urls_checked"] == 1
    assert result["deferred"] == 1


@pytest.mark.asyncio
async def test_fixture_source_has_stable_ids_timezone_and_cursor():
    source = GubaSource(clock=lambda: datetime(2026, 8, 14, tzinfo=UTC), days=30)
    first = await source.collect("黄金", datetime(2026, 7, 1, tzinfo=UTC))
    assert first.next_cursor
    second = await source.collect("黄金", datetime(2026, 7, 1, tzinfo=UTC), first.next_cursor)
    assert first.items[0].source_item_id != second.items[0].source_item_id
    assert first.items[0].observed_at.tzinfo is not None
    assert first.items[0].published_at.tzinfo is not None


@pytest.mark.asyncio
async def test_public_get_retries_transient_status():
    requests = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if requests == 1:
            return httpx.Response(503, headers={"Retry-After": "0.001"})
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        response = await public_get(client, "https://source.example.test/items")

    assert response.json() == {"ok": True}
    assert requests == 2


def test_public_rate_limiter_can_be_reused_across_event_loops():
    limiter = RequestRateLimiter()

    async def wait_twice():
        await asyncio.gather(
            limiter.wait("guba", 0.001),
            limiter.wait("guba", 0.001),
        )

    asyncio.run(wait_twice())
    asyncio.run(wait_twice())


@pytest.mark.asyncio
async def test_live_collection_rejects_missing_configuration_before_network(session, settings):
    settings = replace(settings, enabled_sources=("wikimedia-pageviews",))
    result = await collect_source_async(
        session,
        "wikimedia-pageviews",
        query="黄金",
        since=datetime(2026, 8, 14, tzinfo=UTC),
        settings=settings,
    )

    assert result["source_degraded"] is True
    assert result["items_collected"] == 0
    assert result["missing_config"]
    assert session.scalars(select(RawObservation)).all() == []


@pytest.mark.asyncio
async def test_collection_can_resume_from_partial_page_checkpoint(session, settings):
    settings = replace(settings, data_mode="demo", enabled_sources=("guba",))
    until = now_utc()
    since = until - timedelta(days=30)

    first = await collect_source_async(
        session,
        "guba",
        query="黄金",
        since=since,
        until=until,
        settings=settings,
        max_pages=1,
        allow_partial=True,
    )
    second = await collect_source_async(
        session,
        "guba",
        query="黄金",
        since=since,
        until=until,
        settings=settings,
        max_pages=1,
        start_cursor=first["next_cursor"],
        allow_partial=True,
    )

    assert first["exhausted"] is False
    assert first["next_cursor"]
    assert second["next_cursor"] != first["next_cursor"]
    assert session.query(RawObservation).count() == 24


@pytest.mark.asyncio
async def test_collection_preserves_partial_source_health_without_retrying_as_failure(
    session, settings, monkeypatch
):
    class PartialCollector:
        async def collect(self, *_args, **_kwargs):
            return CollectResult(
                items=[],
                exhausted=True,
                warnings=["search fallback used"],
                partial=True,
            )

    monkeypatch.setattr(
        jobs_module, "source_for_name", lambda *_args, **_kwargs: PartialCollector()
    )
    settings = replace(
        settings,
        enabled_sources=("xiaohongshu",),
        source_credentials={
            "xiaohongshu": SourceCredential("xiaohongshu", endpoint="http://127.0.0.1:18060")
        },
    )
    result = await collect_source_async(
        session,
        "xiaohongshu",
        query="黄金投资",
        since=datetime(2026, 8, 14, 5, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
        settings=settings,
    )

    source = session.scalar(select(Source).where(Source.name == "xiaohongshu"))
    assert result["source_degraded"] is False
    assert result["source_partial"] is True
    assert result["warnings"] == ["search fallback used"]
    assert source.health_status == "partial"


@pytest.mark.asyncio
async def test_collection_keeps_informational_warning_without_marking_partial(
    session, settings, monkeypatch
):
    class WarningCollector:
        async def collect(self, *_args, **_kwargs):
            return CollectResult(
                items=[],
                exhausted=True,
                warnings=["excluded irrelevant reference answer"],
            )

    monkeypatch.setattr(
        jobs_module, "source_for_name", lambda *_args, **_kwargs: WarningCollector()
    )
    settings = replace(
        settings,
        enabled_sources=("zhihu",),
        source_credentials={
            "zhihu": SourceCredential("zhihu", access_token="example-access-secret")
        },
    )
    result = await collect_source_async(
        session,
        "zhihu",
        query="如何看待2026年8月28日A股市场行情走势？",
        since=datetime(2026, 8, 28, tzinfo=UTC),
        until=datetime(2026, 8, 29, tzinfo=UTC),
        settings=settings,
    )

    source = session.scalar(select(Source).where(Source.name == "zhihu"))
    assert result["warnings"] == ["excluded irrelevant reference answer"]
    assert result["source_partial"] is False
    assert source.health_status == "healthy"


@pytest.mark.asyncio
async def test_different_sources_share_one_writer_while_network_waits_concurrently(
    session, settings, monkeypatch
):
    active = 0
    maximum_active = 0

    class ConcurrentCollector:
        def __init__(self, source_name):
            self.source_name = source_name

        async def collect(self, _query, _since, _cursor=None, *, until=None):
            nonlocal active, maximum_active
            active += 1
            maximum_active = max(maximum_active, active)
            await asyncio.sleep(0.02)
            active -= 1
            published_at = datetime(2026, 8, 20, 1, tzinfo=UTC)
            return CollectResult(
                items=[
                    RawObservationSchema(
                        source=self.source_name,
                        source_item_id=f"{self.source_name}-parallel-1",
                        observation_kind="forum_post",
                        published_at=published_at,
                        observed_at=published_at + timedelta(minutes=1),
                        payload={
                            "title": self.source_name,
                            "body": "并行采集测试",
                            "timestamp_semantics": "published",
                            "published_at": published_at.isoformat(),
                        },
                    )
                ],
                exhausted=True,
            )

    monkeypatch.setattr(
        jobs_module,
        "source_for_name",
        lambda source_name, **_kwargs: ConcurrentCollector(source_name),
    )
    settings = replace(settings, enabled_sources=("guba", "taoguba"))
    writer_lock = asyncio.Lock()
    since = datetime(2026, 8, 20, 0, tzinfo=UTC)
    until = datetime(2026, 8, 21, 0, tzinfo=UTC)

    results = await asyncio.gather(
        *(
            collect_source_async(
                session,
                source_name,
                query="黄金",
                since=since,
                until=until,
                settings=settings,
                _write_lock=writer_lock,
            )
            for source_name in ("guba", "taoguba")
        )
    )

    assert maximum_active == 2
    assert [result["items_collected"] for result in results] == [1, 1]
    assert session.query(RawObservation).count() == 2


@pytest.mark.asyncio
async def test_live_collection_skips_existing_source_item_but_adds_new_topic_link(
    session, settings, monkeypatch
):
    calls = 0
    published_at = datetime(2026, 8, 20, 1, tzinfo=UTC)

    class UpdatingCollector:
        async def collect(self, _query, _since, _cursor=None, *, until=None):
            nonlocal calls
            calls += 1
            return CollectResult(
                items=[
                    RawObservationSchema(
                        source="guba",
                        source_item_id="known-post-1",
                        observation_kind="forum_post",
                        published_at=published_at,
                        observed_at=published_at + timedelta(minutes=calls),
                        payload={
                            "title": "同一帖子",
                            "body": "正文",
                            "likes": calls,
                            "timestamp_semantics": "published",
                            "published_at": published_at.isoformat(),
                        },
                    )
                ],
                exhausted=True,
            )

    collector = UpdatingCollector()
    monkeypatch.setattr(jobs_module, "source_for_name", lambda *_args, **_kwargs: collector)
    settings = replace(settings, enabled_sources=("guba",))
    gold = session.scalar(select(Topic).where(Topic.slug == "gold"))
    ai = session.scalar(select(Topic).where(Topic.slug == "ai"))
    since = datetime(2026, 8, 20, 0, tzinfo=UTC)
    until = datetime(2026, 8, 21, 0, tzinfo=UTC)

    first = await collect_source_async(
        session,
        "guba",
        query="黄金",
        since=since,
        until=until,
        settings=settings,
        topic_id=gold.id,
    )
    second = await collect_source_async(
        session,
        "guba",
        query="AI",
        since=since,
        until=until,
        settings=settings,
        topic_id=ai.id,
    )

    raw = session.scalar(select(RawObservation).where(RawObservation.source_item_id == "known-post-1"))
    assert first["items_collected"] == 1
    assert second["items_collected"] == 0
    assert second["duplicates"] == 1
    assert session.query(RawObservation).count() == 1
    assert {match.topic_id for match in raw.topic_matches} == {gold.id, ai.id}


@pytest.mark.asyncio
async def test_zhihu_keeps_same_answer_as_separate_market_session_snapshots(
    session, settings, monkeypatch
):
    class SnapshotCollector:
        async def collect(self, query, _since, _cursor=None, *, until=None):
            session_date = "2026-08-20" if "20日" in query else "2026-08-21"
            observed_at = datetime.fromisoformat(f"{session_date}T10:00:00+00:00")
            return CollectResult(
                items=[
                    RawObservationSchema(
                        source="zhihu",
                        source_item_id="answer:repeated-reference",
                        observation_kind="zhihu_answer_snapshot",
                        published_at=None,
                        observed_at=observed_at,
                        payload={
                            "title": f"如何看待{session_date}A股市场行情？",
                            "body": "A股大盘成交放大。",
                            "market_session_date": session_date,
                            "answer_edit_time": observed_at.isoformat(),
                            "timestamp_semantics": "observed_rank_snapshot",
                        },
                    )
                ],
                exhausted=True,
            )

    monkeypatch.setattr(
        jobs_module,
        "source_for_name",
        lambda *_args, **_kwargs: SnapshotCollector(),
    )
    settings = replace(
        settings,
        enabled_sources=("zhihu",),
        source_credentials={
            "zhihu": SourceCredential("zhihu", access_token="example-access-secret")
        },
    )
    topic = session.scalar(select(Topic).where(Topic.slug == "broad-a-share"))

    for day in (20, 21):
        await collect_source_async(
            session,
            "zhihu",
            query=f"如何看待2026年8月{day}日A股市场行情走势？",
            since=datetime(2026, 8, day, tzinfo=UTC),
            until=datetime(2026, 8, day + 1, tzinfo=UTC),
            settings=settings,
            topic_id=topic.id,
        )

    snapshots = session.scalars(
        select(RawObservation).where(
            RawObservation.source_item_id == "answer:repeated-reference"
        )
    ).all()
    assert len(snapshots) == 2
    assert {row.payload["market_session_date"] for row in snapshots} == {
        "2026-08-20",
        "2026-08-21",
    }


def test_guba_board_selection_prefers_exact_topic_then_relevant_board():
    records = [
        {"ShortName": "中金黄金", "OuterCode": "600489"},
        {"ShortName": "黄金", "OuterCode": "huangjin"},
        {"ShortName": "黄金", "OuterCode": "zssza0050"},
        {"ShortName": "山东黄金", "OuterCode": "600547"},
    ]

    selected = select_guba_boards(records, "黄金", limit=3)

    assert [item["code"] for item in selected] == ["huangjin", "zssza0050", "600489"]


def test_guba_board_selection_rejects_unrelated_popular_suggestions():
    selected = select_guba_boards(
        [
            {"ShortName": "上证指数", "OuterCode": "zssh000001"},
            {"ShortName": "随机热门股", "OuterCode": "600000"},
        ],
        "上证指数",
    )

    assert selected == [{"code": "zssh000001", "name": "上证指数"}]


@pytest.mark.asyncio
async def test_guba_retries_temporary_non_data_page_with_bound():
    requests = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if requests == 1:
            return httpx.Response(200, text="<html>temporary access page</html>")
        return httpx.Response(200, text='var article_list = {"re": [], "count": 0};')

    source = GubaSource(
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_public_interval=0,
        access_retry_delays=(0,),
    )
    async with httpx.AsyncClient(transport=source.transport) as client:
        payload = await source._public_page_data(
            client,
            "https://guba.eastmoney.com/list,example,f.html",
            headers={},
        )

    assert requests == 2
    assert payload["re"] == []


def test_guba_identity_verification_page_has_an_actionable_error():
    with pytest.raises(SourceError, match="identity-verification page"):
        parse_guba_page("<html><title>身份核实</title><div id='root'></div></html>")


@pytest.mark.asyncio
async def test_public_guba_maps_body_identity_counts_and_shanghai_time():
    article_list = {
        "re": [
            {
                "post_id": "example-post-1",
                "post_title": "黄金帖子",
                "post_content": "<p>不追高&nbsp;，等待确认</p>",
                "post_publish_time": "2026-08-14 15:11:45",
                "post_last_time": "2026-08-14 15:20:34",
                "post_click_count": 41,
                "post_comment_count": 2,
                "post_like_count": 3,
                "post_forward_count": 1,
                "post_top_status": 0,
                "post_type": 0,
                "post_user": {"user_id": "example-user-1", "user_nickname": "示例用户"},
                "post_guba": {"stockbar_code": "518880", "stockbar_name": "黄金ETF吧"},
            }
        ],
        "count": 1,
        "bar_info": {"OuterCode": "518880", "ShortName": "黄金ETF"},
    }
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "searchadapter.eastmoney.com":
            return httpx.Response(
                200,
                json={
                    "GubaCodeTable": {
                        "Data": [{"ShortName": "黄金ETF", "OuterCode": "518880"}],
                        "Status": 0,
                    }
                },
            )
        assert request.url.host == "gbapi.eastmoney.com"
        assert request.url.params["code"] == "518880"
        assert request.url.params["p"] == "1"
        assert request.url.params["ps"] == "40"
        assert "example-browser-session" not in request.headers.get("cookie", "")
        return httpx.Response(200, json=article_list)

    source = GubaSource(
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        max_detail_requests=0,
        clock=lambda: datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
    )
    result = await source.collect(
        "518880",
        datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
    )

    assert result.exhausted
    assert len(result.items) == 1
    item = result.items[0]
    assert item.source_item_id == "example-post-1"
    assert item.published_at == datetime(2026, 8, 14, 7, 11, 45, tzinfo=UTC)
    assert item.payload["timestamp_semantics"] == "published"
    assert item.payload["source_timestamp_field"] == "post_publish_time"
    assert item.payload["body"] == "不追高 ，等待确认"
    assert item.payload["author_id"] == "example-user-1"
    assert item.payload["views"] == 41
    assert item.payload["url"].endswith("/news,518880,example-post-1.html")


@pytest.mark.asyncio
async def test_public_guba_hot_feed_resolves_alias_and_records_sampling_contract():
    hot_records = [
        {
            "post_id": f"hot-post-{index}",
            "post_title": f"热门黄金帖子 {index}",
            "post_content": "黄金波动观察",
            "post_publish_time": f"2026-08-14 15:1{index}:45",
            "post_last_time": f"2026-08-14 15:1{index}:45",
            "post_click_count": 100 - index,
            "post_comment_count": index,
            "post_top_status": 0,
            "post_type": 0,
            "post_guba": {"stockbar_code": "fshfeaum", "stockbar_name": "沪金吧"},
        }
        for index in range(2)
    ]
    requested_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "searchadapter.eastmoney.com":
            return httpx.Response(
                200,
                json={
                    "GubaCodeTable": {
                        "Data": [{"ShortName": "黄金", "OuterCode": "huangjin"}],
                        "Status": 0,
                    }
                },
            )
        requested_paths.append(request.url.path)
        if request.url.path.endswith("/Article/WebArticleList"):
            return httpx.Response(
                200,
                json={
                    "rc": 1,
                    "re": [],
                    "count": 100,
                    "bar_info": {"OuterCode": "fshfeaum", "ShortName": "沪金"},
                },
            )
        assert request.url.path.endswith("/Hot/Articlelist")
        assert request.url.params["code"] == "fshfeaum"
        assert request.url.params["type"] == "0"
        assert request.url.params["ps"] == "2"
        return httpx.Response(200, json={"rc": 1, "re": hot_records, "count": 13701})

    source = GubaSource(
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        max_detail_requests=0,
        clock=lambda: datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
    )
    result = await source.collect(
        "黄金",
        datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
        cursor=guba_module.guba_hot_cursor("黄金", max_items=2),
        until=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
    )

    assert result.exhausted
    assert [item.source_item_id for item in result.items] == ["hot-post-0", "hot-post-1"]
    assert result.items[0].payload["sampled"] is True
    assert result.items[0].payload["sampling_mode"] == "hot"
    assert result.items[0].payload["sampling_rank"] == 1
    assert result.items[1].payload["sampling_rank"] == 2
    assert result.items[0].payload["sampling_limit"] == 2
    assert requested_paths == [
        "/webarticlelist/api/Article/WebArticleList",
        "/webarticlelist/api/Hot/Articlelist",
    ]


@pytest.mark.asyncio
async def test_public_guba_reuses_saved_browser_session_only_on_guba(tmp_path):
    input_file = tmp_path / "request.curl"
    input_file.write_text(
        "curl 'https://guba.eastmoney.com/list,518880,f.html' "
        "-H 'Cookie: eastmoney_session=example-browser-session' "
        "-H 'User-Agent: Example Browser/1.0'",
        encoding="utf-8",
    )
    session_file = tmp_path / "auth" / "guba.session.json"
    import_source_session("guba", input_file, session_file)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "searchadapter.eastmoney.com":
            assert "example-browser-session" not in request.headers.get("cookie", "")
            return httpx.Response(
                200,
                json={
                    "GubaCodeTable": {
                        "Data": [{"ShortName": "黄金ETF", "OuterCode": "518880"}],
                        "Status": 0,
                    }
                },
            )
        if request.url.host == "gbapi.eastmoney.com":
            return httpx.Response(200, json={"unexpected": []})
        cookie = request.headers.get("cookie", "")
        assert "eastmoney_session=example-browser-session" in cookie
        assert "listtype=1" in cookie
        assert request.headers["user-agent"] == "Example Browser/1.0"
        return httpx.Response(
            200,
            text='<script>var article_list={"re": [], "count": 0};</script>',
        )

    source = GubaSource(
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_public_interval=0,
        access_retry_delays=(),
        session_file=session_file,
    )

    result = await source.collect(
        "518880",
        datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
    )

    assert result.exhausted


@pytest.mark.asyncio
async def test_guba_browser_profile_transport_keeps_headers_in_memory(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_browser_get(url: str, *, headers: dict[str, str], timeout: float) -> str:
        captured.update(url=url, headers=headers, timeout=timeout)
        return '<script>var article_list={"re": [], "count": 0};</script>'

    monkeypatch.setattr(guba_module, "_next_public_list_request_at", 0.0)
    monkeypatch.setattr(guba_module, "_browser_profile_get", fake_browser_get)
    source = GubaSource(
        use_fixture=False,
        min_public_interval=0,
        access_retry_delays=(),
    )

    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: None)) as client:
        payload = await source._public_page_data(
            client,
            "https://guba.eastmoney.com/list,518880,f.html",
            headers={
                "Cookie": "eastmoney_session=example-browser-session; listtype=1",
                "User-Agent": "Example Browser/1.0",
            },
            browser_profile=True,
        )

    assert payload["re"] == []
    assert captured["url"] == "https://guba.eastmoney.com/list,518880,f.html"
    assert captured["timeout"] == 20
    assert captured["headers"] == {
        "Cookie": "eastmoney_session=example-browser-session; listtype=1",
        "User-Agent": "Example Browser/1.0",
    }


@pytest.mark.asyncio
async def test_public_guba_reports_rejected_authenticated_session(tmp_path):
    input_file = tmp_path / "cookie.txt"
    input_file.write_text("eastmoney_session=example-browser-session", encoding="utf-8")
    session_file = tmp_path / "auth" / "guba.session.json"
    import_source_session("guba", input_file, session_file)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "searchadapter.eastmoney.com":
            return httpx.Response(
                200,
                json={
                    "GubaCodeTable": {
                        "Data": [{"ShortName": "黄金ETF", "OuterCode": "518880"}],
                        "Status": 0,
                    }
                },
            )
        if request.url.host == "gbapi.eastmoney.com":
            return httpx.Response(200, json={"unexpected": []})
        return httpx.Response(200, text="<html><title>身份核实</title></html>")

    source = GubaSource(
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_public_interval=0,
        access_retry_delays=(),
        session_file=session_file,
    )

    with pytest.raises(
        SourceError,
        match="JSON list failed.*authenticated HTML fallback.*identity-verification",
    ):
        await source.collect(
            "518880",
            datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
            until=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_public_guba_expands_truncated_wealth_article_body():
    article_list = {
        "re": [
            {
                "post_id": "example-post-2",
                "post_source_id": "example-article-2",
                "post_title": "示例长文",
                "post_content": "这是一段截断摘要...",
                "post_publish_time": "2026-08-14 15:11:45",
                "post_type": 20,
                "post_user": {"user_id": "example-user-2"},
                "post_guba": {"stockbar_code": "518880", "stockbar_name": "黄金ETF吧"},
            }
        ],
        "count": 1,
    }
    detail_page = (
        "<script>var articleTxt = "
        + json.dumps("<p>这是完整正文。</p><p>第二段。</p>", ensure_ascii=False)
        + ";</script>"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "searchadapter.eastmoney.com":
            return httpx.Response(
                200,
                json={
                    "GubaCodeTable": {
                        "Data": [{"ShortName": "黄金ETF", "OuterCode": "518880"}],
                        "Status": 0,
                    }
                },
            )
        if request.url.host == "caifuhao.eastmoney.com":
            assert request.url.path == "/news/example-article-2"
            return httpx.Response(200, text=detail_page)
        assert request.url.host == "gbapi.eastmoney.com"
        return httpx.Response(200, json=article_list)

    source = GubaSource(
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        clock=lambda: datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
    )
    result = await source.collect(
        "518880",
        datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
    )

    assert result.items[0].payload["body"] == "这是完整正文。\n第二段。"
    assert result.items[0].payload["body_truncated"] is False


@pytest.mark.asyncio
async def test_public_taoguba_maps_stable_topic_and_plain_text():
    response_payload = {
        "status": True,
        "errorMessage": "",
        "dto": {
            "totalPageNum": 1,
            "topicAttr": [
                {
                    "topicID": "example-topic-1",
                    "newTopicID": "example-public-topic-1",
                    "userID": "example-user-2",
                    "userName": "示例作者",
                    "subject": "黄金行情",
                    "body": "<p>回调后观察&nbsp;成交量</p><img src='example.png'><span>[淘股吧]</span>",
                    "postDate": 1786685582000,
                    "lastReplyDate": 1786685582000,
                    "totalViewNum": 31,
                    "totalReplyNum": 4,
                    "usefulNum": 2,
                    "favoriteNum": 1,
                    "catalogName": "淘股论坛",
                }
            ],
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/search/getSearchTopicResult"
        assert request.url.params["subject"] == "黄金"
        assert request.url.params["searchDate"] == "6"
        return httpx.Response(200, json=response_payload)

    source = TaogubaSource(
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        clock=lambda: datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )
    result = await source.collect(
        "黄金",
        datetime(2026, 8, 14, 5, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )

    assert result.exhausted
    assert len(result.items) == 1
    item = result.items[0]
    assert item.source_item_id == "example-topic-1"
    assert item.published_at == datetime(2026, 8, 14, 5, 33, 2, tzinfo=UTC)
    assert item.payload["timestamp_semantics"] == "published"
    assert item.payload["source_timestamp_field"] == "postDate"
    assert item.payload["body"] == "回调后观察 成交量\n[图片]"
    assert item.payload["comments"] == 4
    assert item.payload["url"] == "https://www.tgb.cn/a/example-public-topic-1"


@pytest.mark.asyncio
async def test_public_taoguba_retries_temporary_login_gate_with_bound():
    requests = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if requests == 1:
            return httpx.Response(200, json={"status": False, "errorMessage": "请登录后操作"})
        return httpx.Response(
            200,
            json={
                "status": True,
                "errorMessage": "",
                "dto": {
                    "totalPageNum": 0,
                    "topicAttr": [],
                },
            },
        )

    source = TaogubaSource(
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_public_interval=0,
        access_retry_delays=(0,),
        clock=lambda: datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )
    result = await source.collect(
        "半导体",
        datetime(2026, 8, 14, 5, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )

    assert requests == 2
    assert result.exhausted


@pytest.mark.asyncio
async def test_public_taoguba_reuses_saved_browser_session(tmp_path):
    input_file = tmp_path / "cookie.txt"
    input_file.write_text("session=example-browser-session", encoding="utf-8")
    session_file = tmp_path / "auth" / "taoguba.session.json"
    import_source_session("taoguba", input_file, session_file)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["cookie"] == "session=example-browser-session"
        return httpx.Response(
            200,
            json={
                "status": True,
                "errorMessage": "",
                "dto": {"totalPageNum": 0, "topicAttr": []},
            },
        )

    source = TaogubaSource(
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_public_interval=0,
        access_retry_delays=(),
        session_file=session_file,
    )
    result = await source.collect(
        "半导体",
        datetime(2026, 8, 14, 5, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )

    assert result.exhausted


@pytest.mark.asyncio
async def test_public_taoguba_reports_expired_authenticated_session(tmp_path):
    input_file = tmp_path / "cookie.txt"
    input_file.write_text("session=example-browser-session", encoding="utf-8")
    session_file = tmp_path / "auth" / "taoguba.session.json"
    import_source_session("taoguba", input_file, session_file)

    source = TaogubaSource(
        use_fixture=False,
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200, json={"status": False, "errorMessage": "请登录后操作"}
            )
        ),
        min_public_interval=0,
        access_retry_delays=(),
        session_file=session_file,
    )

    with pytest.raises(SourceError, match="authenticated browser session.*login required"):
        await source.collect(
            "半导体",
            datetime(2026, 8, 14, 5, 0, tzinfo=UTC),
            until=datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_official_zhihu_search_maps_documented_contract():
    requests = 0
    response_payload = {
        "Code": 0,
        "Message": "success",
        "Data": {
            "HasMore": False,
            "SearchHashId": "example-search-1",
            "Items": [
                {
                    "Title": "黄金研究",
                    "ContentType": "Article",
                    "ContentID": "example-content-1",
                    "ContentText": "<em>黄金</em> 内容摘要",
                    "Url": "https://content.example.test/example-content-1",
                    "CommentCount": 7,
                    "VoteUpCount": 21,
                    "AuthorName": "示例作者",
                    "AuthorAvatar": "https://images.example.test/avatar.png",
                    "AuthorBadge": "",
                    "AuthorBadgeText": "示例认证",
                    "EditTime": 1786685582,
                    "CreateTime": 1786685582,
                    "CommentInfoList": [{"Content": "<b>示例评论</b>"}],
                    "AuthorityLevel": "2",
                    "RankingScore": 0.98,
                }
            ],
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        assert request.url.path == "/api/v1/content/zhihu_search"
        assert request.url.params["Query"] == "黄金"
        assert request.url.params["Count"] == "10"
        assert "query" not in request.url.params
        assert request.headers["authorization"] == "Bearer example-access-secret"
        assert request.headers["x-request-timestamp"].isdigit()
        response_payload["Data"]["Items"][0]["RankingScore"] = 0.98 - requests / 100
        return httpx.Response(200, json=response_payload)

    source = ZhihuSource(
        use_fixture=False,
        credential=SourceCredential("zhihu", access_token="example-access-secret"),
        transport=httpx.MockTransport(handler),
        clock=lambda: datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )
    result = await source.collect(
        "黄金",
        datetime(2026, 8, 14, 5, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )

    assert result.exhausted
    assert len(result.items) == 1
    item = result.items[0]
    assert item.source_item_id == "article:example-content-1"
    assert item.observation_kind == "zhihu_article"
    assert item.published_at == datetime(2026, 8, 14, 5, 33, 2, tzinfo=UTC)
    assert item.payload["body"] == "黄金 内容摘要"
    assert item.payload["author_name"] == "示例作者"
    assert "author_id" not in item.payload
    assert item.payload["comments"] == 7
    assert item.payload["likes"] == 21
    assert item.payload["featured_comments"] == ["示例评论"]
    assert item.payload["timestamp_semantics"] == "created"
    assert item.payload["source_timestamp_field"] == "CreateTime"
    assert item.payload["source_role"] == "discovery"
    assert "ranking_score" not in item.payload
    assert not _observation_in_window(
        item,
        datetime(2026, 8, 15, tzinfo=UTC),
        datetime(2026, 8, 16, tzinfo=UTC),
    )
    repeated = await source.collect("黄金", datetime(2026, 8, 14, tzinfo=UTC))
    assert repeated.items[0].payload == item.payload


@pytest.mark.asyncio
async def test_official_zhihu_answer_skips_ambiguous_edit_time_without_extra_request():
    search_payload = {
        "Code": 0,
        "Message": "success",
        "Data": {
            "HasMore": False,
            "Items": [
                {
                    "Title": "黄金还能买吗",
                    "ContentType": "Answer",
                    "ContentID": "signed-answer-id",
                    "ContentText": "回答摘要",
                    "Url": "https://www.zhihu.com/question/1/answer/123456",
                    "EditTime": 1786686000,
                }
            ],
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "developer.zhihu.com"
        return httpx.Response(200, json=search_payload)

    source = ZhihuSource(
        use_fixture=False,
        credential=SourceCredential("zhihu", access_token="example-access-secret"),
        transport=httpx.MockTransport(handler),
        clock=lambda: datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )
    result = await source.collect("黄金", datetime(2026, 8, 1, tzinfo=UTC))

    assert result.items == []
    assert result.warnings == [
        "skipped answer signed-answer-id: official search only returned EditTime"
    ]


@pytest.mark.asyncio
async def test_official_zhihu_market_question_ranks_answer_snapshots_without_fake_publish_time():
    search_payload = {
        "Code": 0,
        "Message": "success",
        "Data": {
            "HasMore": False,
            "Items": [
                {
                    "Title": "如何看待8月19日A股市场行情走势？",
                    "ContentType": "Answer",
                    "ContentID": "answer-low",
                    "ContentText": "谨慎等待确认",
                    "Url": "https://www.zhihu.com/question/1/answer/1",
                    "EditTime": 1787101200,
                    "VoteUpCount": 8,
                    "FavoriteCount": 3,
                    "CommentCount": 1,
                },
                {
                    "Title": "市场复盘文章",
                    "ContentType": "Article",
                    "ContentID": "article-ignored",
                    "ContentText": "不是问题回答",
                    "EditTime": 1787101200,
                    "VoteUpCount": 999,
                },
                {
                    "Title": "如何看待8月19日A股市场行情走势？",
                    "ContentType": "Answer",
                    "ContentID": "answer-top",
                    "ContentText": "半导体强势但不追高",
                    "Url": "https://www.zhihu.com/question/1/answer/2",
                    "EditTime": 1787101200,
                    "VoteUpCount": 21,
                    "FavoriteCount": 5,
                    "CommentCount": 4,
                },
            ],
        },
    }

    source = ZhihuSource(
        use_fixture=False,
        credential=SourceCredential("zhihu", access_token="example-access-secret"),
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=search_payload)),
        clock=lambda: datetime(2026, 8, 20, 2, 0, tzinfo=UTC),
    )
    result = await source.collect(
        "如何看待8月19日A股市场行情走势？",
        datetime(2026, 8, 18, 16, 0, tzinfo=UTC),
        until=datetime(2026, 8, 20, 3, 0, tzinfo=UTC),
    )

    assert [item.source_item_id for item in result.items] == [
        "answer:answer-top",
        "answer:answer-low",
    ]
    top = result.items[0]
    assert top.published_at is None
    assert top.payload["market_session_date"] == "2026-08-19"
    assert top.payload["answer_rank"] == 1
    assert top.payload["likes"] == 21
    assert top.payload["favorites"] == 5
    assert top.payload["timestamp_semantics"] == "observed_rank_snapshot"
    assert top.payload["publication_time_verified"] is False
    assert top.payload["reference_eligible"] is True
    assert top.payload["reference_reason"] == "recent_edit_matches_market_session"
    assert _observation_in_window(
        top,
        datetime(2026, 8, 18, 16, 0, tzinfo=UTC),
        datetime(2026, 8, 20, 3, 0, tzinfo=UTC),
    )


def test_zhihu_market_question_uses_latest_completed_weekday():
    query, session_date = zhihu_market_question_query(
        "A股", datetime(2026, 8, 21, 2, 30, tzinfo=UTC)
    )
    assert session_date.isoformat() == "2026-08-20"
    assert query == "如何看待2026年8月20日A股市场行情走势？"

    hk_query, _ = zhihu_market_question_query("港股", datetime(2026, 8, 21, 8, 0, tzinfo=UTC))
    us_query, _ = zhihu_market_question_query("美股", datetime(2026, 8, 21, 8, 0, tzinfo=UTC))
    assert hk_query == "如何看待2026年8月21日港股市场行情走势？"
    assert us_query == "如何看待2026年8月21日美股市场行情走势？"


def test_zhihu_reference_rejects_old_same_month_day_answer():
    eligible, reason = zhihu_answer_reference_eligibility(
        {
            "market_session_date": "2026-08-27",
            "title": "如何看待2025年8月27日港股市场行情走势？",
            "body": "恒生指数回落。",
            "answer_edit_time": "2025-08-28T00:00:00+00:00",
        }
    )

    assert eligible is False
    assert reason == "conflicting_market_session_year"


@pytest.mark.asyncio
async def test_official_zhihu_search_rejects_business_error_under_http_200():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"Code": 10001, "Message": "Query is required", "Data": None},
        )

    source = ZhihuSource(
        use_fixture=False,
        credential=SourceCredential("zhihu", access_token="example-access-secret"),
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(SourceError, match=r"code=10001.*Query is required"):
        await source.collect("黄金", datetime(2026, 8, 14, tzinfo=UTC))


@pytest.mark.asyncio
async def test_xiaohongshu_search_enriches_detail_time_without_storing_xsec_token():
    published_ms = 1_786_685_582_000

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/feeds/search":
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "feeds": [
                            {
                                "id": "example-note-1",
                                "xsecToken": "temporary-detail-token",
                                "noteCard": {"displayTitle": "黄金ETF还能买吗"},
                            }
                        ]
                    },
                },
            )
        assert request.url.path == "/api/v1/feeds/detail"
        body = json.loads(request.content)
        assert body["xsec_token"] == "temporary-detail-token"
        assert body["xsec_source"] == "pc_search"
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "feed_id": "example-note-1",
                    "data": {
                        "note": {
                            "noteId": "example-note-1",
                            "title": "黄金ETF还能买吗",
                            "desc": "第一次买黄金ETF，担心追高，想先观察仓位。",
                            "type": "normal",
                            "time": published_ms,
                            "user": {"userId": "example-user", "nickname": "示例用户"},
                            "interactInfo": {
                                "likedCount": "1.2万",
                                "commentCount": "31",
                                "collectedCount": "8",
                                "sharedCount": "2",
                            },
                        },
                        "comments": {
                            "list": [{"content": "我也怕踏空"}],
                        },
                    },
                },
            },
        )

    source = XiaohongshuSource(
        endpoint="http://xhs.test",
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_request_interval=0,
        clock=lambda: datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )
    result = await source.collect(
        "黄金 ETF 投资",
        datetime(2026, 8, 14, 5, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )

    assert result.warnings == []
    assert len(result.items) == 1
    item = result.items[0]
    assert item.published_at == datetime(2026, 8, 14, 5, 33, 2, tzinfo=UTC)
    assert item.payload["likes"] == 12_000
    assert item.payload["sample_comments"] == ["我也怕踏空"]
    assert item.payload["search_sort"] == "最新"
    assert item.payload["source_role"] == "discovery"
    assert item.payload["timestamp_semantics"] == "published"
    assert item.payload["source_timestamp_field"] == "note.time"
    assert "xsec" not in json.dumps(item.payload).casefold()


@pytest.mark.asyncio
async def test_xiaohongshu_backfill_cursor_selects_one_explicit_sort_strategy():
    requests: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"success": True, "data": {"feeds": []}})

    source = XiaohongshuSource(
        endpoint="http://xhs.test",
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_request_interval=0,
    )
    result = await source.collect(
        "黄金ETF 散户",
        datetime(2026, 5, 18, tzinfo=UTC),
        xiaohongshu_strategy_cursor("最多收藏"),
        until=datetime(2026, 8, 18, tzinfo=UTC),
    )

    assert result.exhausted is True
    assert requests[0]["filters"]["sort_by"] == "最多收藏"
    assert requests[0]["filters"]["publish_time"] == "半年内"
    assert set(requests[0]["filters"]) == {"sort_by", "publish_time"}


@pytest.mark.asyncio
async def test_xiaohongshu_spider_pages_and_skips_known_candidates_before_detail():
    search_bodies: list[dict] = []
    detail_ids: list[str] = []
    published_ms = int(datetime(2026, 8, 14, 5, 0, tzinfo=UTC).timestamp() * 1000)

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if request.url.path == "/api/v1/feeds/search":
            search_bodies.append(body)
            if body.get("cursor"):
                return httpx.Response(
                    200,
                    json={
                        "success": True,
                        "data": {"feeds": [], "has_more": False, "next_cursor": None},
                    },
                )
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "feeds": [
                            {"id": "known-note", "xsecToken": "known-token"},
                            {"id": "new-note", "xsecToken": "new-token"},
                            {"id": "gone-note", "xsecToken": "gone-token"},
                        ],
                        "has_more": True,
                        "next_cursor": "page-2",
                    },
                },
            )
        detail_ids.append(body["feed_id"])
        if body["feed_id"] == "gone-note":
            return httpx.Response(404, json={"detail": "笔记不存在"})
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "note": {
                        "noteId": body["feed_id"],
                        "time": published_ms,
                        "title": "我的股票投资记录",
                        "desc": "今天继续记录股票投资和持仓。",
                    },
                    "comments": {"list": []},
                },
            },
        )

    source = XiaohongshuSource(
        spider_endpoint="http://spider.test",
        use_fixture=False,
        spider_transport=httpx.MockTransport(handler),
        known_source_item_ids={"known-note"},
        min_request_interval=0,
    )
    first = await source.collect(
        "股票",
        datetime(2026, 8, 1, tzinfo=UTC),
        xiaohongshu_strategy_cursor("最新"),
        until=datetime(2026, 8, 15, tzinfo=UTC),
    )
    second = await source.collect(
        "股票",
        datetime(2026, 8, 1, tzinfo=UTC),
        first.next_cursor,
        until=datetime(2026, 8, 15, tzinfo=UTC),
    )

    assert [item.source_item_id for item in first.items] == ["new-note"]
    assert first.exhausted is False
    assert first.next_cursor
    assert second.exhausted is True
    assert search_bodies[1]["cursor"] == "page-2"
    assert set(search_bodies[0]["filters"]) == {"sort_by", "publish_time"}
    assert first.warnings == []
    assert detail_ids == ["new-note", "gone-note"]


@pytest.mark.asyncio
async def test_xiaohongshu_spider_detail_failure_uses_mcp_once():
    detail_calls: list[str] = []
    published_ms = int(datetime(2026, 8, 26, 5, 0, tzinfo=UTC).timestamp() * 1000)

    def spider_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/feeds/search":
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "feeds": [{"id": "mcp-detail-note", "xsecToken": "example-token"}],
                        "has_more": False,
                        "next_cursor": None,
                    },
                },
            )
        detail_calls.append("spider")
        return httpx.Response(
            502,
            json={
                "success": False,
                "error_code": "response_invalid",
                "message": "note detail response is empty",
                "retryable": True,
                "retry_after_seconds": 900,
                "transport": "spider",
            },
        )

    def mcp_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/feeds/detail"
        detail_calls.append("mcp")
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "note": {
                        "noteId": "mcp-detail-note",
                        "time": published_ms,
                        "title": "股票投资记录",
                        "desc": "今天记录股票投资和持仓。",
                    },
                    "comments": {"list": []},
                },
            },
        )

    source = XiaohongshuSource(
        endpoint="http://mcp.test",
        spider_endpoint="http://spider.test",
        use_fixture=False,
        transport=httpx.MockTransport(mcp_handler),
        spider_transport=httpx.MockTransport(spider_handler),
        min_request_interval=0,
    )
    result = await source.collect(
        "股票",
        datetime(2026, 8, 26, tzinfo=UTC),
        xiaohongshu_strategy_cursor("最新"),
        until=datetime(2026, 8, 27, tzinfo=UTC),
    )

    assert [item.source_item_id for item in result.items] == ["mcp-detail-note"]
    assert detail_calls == ["spider", "mcp"]
    assert result.diagnostics["fallback_details"] == 1
    assert result.diagnostics["detail_error_codes"] == {"spider:response_invalid": 1}
    assert result.warnings == []


@pytest.mark.asyncio
async def test_xiaohongshu_expired_spider_session_uses_logged_in_mcp_for_recent_day():
    mcp_calls: list[str] = []
    published_ms = int(datetime(2026, 8, 28, 5, 0, tzinfo=UTC).timestamp() * 1000)

    def spider_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            json={
                "success": False,
                "error_code": "auth_required",
                "message": "登录已过期",
                "retryable": False,
                "retry_after_seconds": None,
                "transport": "spider",
            },
        )

    def mcp_handler(request: httpx.Request) -> httpx.Response:
        mcp_calls.append(request.url.path)
        if request.url.path == "/api/v1/feeds/search":
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "feeds": [
                            {
                                "id": "recent-mcp-note",
                                "xsecToken": "temporary-detail-token",
                            }
                        ]
                    },
                },
            )
        assert request.url.path == "/api/v1/feeds/detail"
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "note": {
                        "noteId": "recent-mcp-note",
                        "time": published_ms,
                        "title": "股票投资记录",
                        "desc": "今天继续记录股票投资和持仓。",
                    },
                    "comments": {"list": []},
                },
            },
        )

    source = XiaohongshuSource(
        endpoint="http://mcp.test",
        spider_endpoint="http://spider.test",
        use_fixture=False,
        transport=httpx.MockTransport(mcp_handler),
        spider_transport=httpx.MockTransport(spider_handler),
        min_request_interval=0,
        clock=lambda: datetime(2026, 8, 31, 2, 0, tzinfo=UTC),
    )
    result = await source.collect(
        "股票",
        datetime(2026, 8, 27, 16, 0, tzinfo=UTC),
        xiaohongshu_spider_cursor("最新", "stale-page-2"),
        until=datetime(2026, 8, 28, 16, 0, tzinfo=UTC),
    )

    assert [item.source_item_id for item in result.items] == ["recent-mcp-note"]
    assert mcp_calls == ["/api/v1/feeds/search", "/api/v1/feeds/detail"]
    assert result.partial is False
    assert result.diagnostics["search_transport"] == "mcp"
    assert result.diagnostics["fallback_reason"] == "spider_auth_required"
    assert result.diagnostics["discarded_spider_cursor"] is True
    assert "used logged-in MCP first-page fallback" in result.warnings[0]


def test_xiaohongshu_old_single_day_uses_a_covering_relative_filter():
    current = datetime(2026, 8, 25, tzinfo=UTC)

    assert (
        _publish_time_filter(
            datetime(2026, 8, 20, tzinfo=UTC),
            datetime(2026, 8, 21, tzinfo=UTC),
            current=current,
        )
        == "一周内"
    )
    assert (
        _publish_time_filter(
            datetime(2025, 8, 20, tzinfo=UTC),
            datetime(2025, 8, 21, tzinfo=UTC),
            current=current,
        )
        == "不限"
    )


def test_xiaohongshu_newest_boundary_ignores_one_old_recommendation():
    since = datetime(2026, 7, 26, tzinfo=UTC)
    old = datetime(2026, 7, 25, tzinfo=UTC)
    new = datetime(2026, 7, 27, tzinfo=UTC)

    assert _newest_page_reached_since([old, new, new, new, new], since) is False
    assert _newest_page_reached_since([old, old, old, old, new], since) is True
    assert _newest_page_reached_since([old, old, old], since) is True
    assert _newest_page_reached_since([old, old, new], since) is False


def test_xiaohongshu_even_sampling_includes_the_ranked_page_tail():
    candidates = [{"id": f"note-{index}"} for index in range(10)]

    selected = _evenly_sample_candidates(candidates, 5)

    assert [row["id"] for row in selected] == [
        "note-0",
        "note-2",
        "note-4",
        "note-7",
        "note-9",
    ]


@pytest.mark.asyncio
async def test_xiaohongshu_known_old_candidates_stop_without_detail_requests():
    known_ids = {f"known-{index}" for index in range(5)}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/feeds/search"
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "feeds": [
                        {"id": note_id, "xsecToken": f"token-{note_id}"}
                        for note_id in sorted(known_ids)
                    ],
                    "has_more": True,
                    "next_cursor": "page-2",
                },
            },
        )

    source = XiaohongshuSource(
        endpoint="",
        spider_endpoint="http://spider.test",
        use_fixture=False,
        spider_transport=httpx.MockTransport(handler),
        known_source_item_ids=known_ids,
        known_source_published_at={
            note_id: datetime(2026, 7, 25, tzinfo=UTC) for note_id in known_ids
        },
        min_request_interval=0,
    )
    result = await source.collect(
        "投资",
        datetime(2026, 7, 26, tzinfo=UTC),
        xiaohongshu_strategy_cursor("最新"),
        until=datetime(2026, 8, 25, tzinfo=UTC),
    )

    assert result.items == []
    assert result.exhausted is True
    assert result.next_cursor is None


@pytest.mark.asyncio
async def test_xiaohongshu_drops_search_hit_when_detail_is_off_topic():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/feeds/search":
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "feeds": [
                            {
                                "id": "off-topic-note",
                                "xsecToken": "temporary-browser-token",
                            }
                        ]
                    },
                },
            )
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "note": {
                        "noteId": "off-topic-note",
                        "time": 1_776_000_000_000,
                        "title": "今年还能囤酒吗",
                        "desc": "白酒价格清单，与贵金属市场无关。",
                    },
                    "comments": {"list": []},
                },
            },
        )

    source = XiaohongshuSource(
        endpoint="http://xhs.test",
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_request_interval=0,
    )
    result = await source.collect(
        "黄金 ETF 投资",
        datetime(2026, 4, 1, tzinfo=UTC),
        until=datetime(2026, 5, 1, tzinfo=UTC),
    )

    assert result.items == []
    assert result.warnings == []


def test_xiaohongshu_backfill_plans_market_wide_paginated_queries(session, settings):
    topics = session.scalars(
        select(Topic).where(Topic.slug.in_(("gold", "broad-a-share"))).order_by(Topic.id)
    ).all()
    specs = jobs_module._backfill_job_specs(
        topics,
        ["xiaohongshu"],
        since=datetime(2026, 7, 19, tzinfo=UTC),
        until=datetime(2026, 8, 19, tzinfo=UTC),
        configured_queries=jobs_module._source_queries(settings.config_dir),
        configured_variants=jobs_module._source_query_variants(settings.config_dir),
        configured_page_limits=jobs_module._source_page_limits(settings.config_dir),
        default_page_limit=100,
    )

    assert len(specs) == 6
    assert [row["query"] for row in specs] == ["投资", "股票", "ETF", "基金", "持仓", "散户"]
    assert all(row["topic"] is None for row in specs)
    assert all(row["sort_by"] == "最新" for row in specs)
    assert all(row["page_limit"] == 20 and row["initial_cursor"] for row in specs)
    assert all(row["sampling_mode"] == "historical" for row in specs)


def test_guba_gold_backfill_uses_bounded_hot_feed(session, settings):
    topic = session.scalar(select(Topic).where(Topic.slug == "gold"))
    strategies = jobs_module._source_collection_strategies(settings.config_dir)
    specs = jobs_module._backfill_job_specs(
        [topic],
        ["guba"],
        since=datetime(2026, 7, 26, tzinfo=UTC),
        until=datetime(2026, 8, 25, tzinfo=UTC),
        configured_queries=jobs_module._source_queries(settings.config_dir),
        configured_variants=jobs_module._source_query_variants(settings.config_dir),
        configured_page_limits=jobs_module._source_page_limits(settings.config_dir),
        configured_source_strategies=strategies,
        default_page_limit=100,
    )

    assert len(specs) == 1
    assert specs[0]["key"] == "gold:guba:hot"
    assert specs[0]["sort_by"] == "热门"
    assert specs[0]["page_limit"] == 5
    cursor = guba_module.decode_cursor("guba", specs[0]["initial_cursor"])
    assert cursor["mode"] == "hot"
    assert cursor["max_items"] == 200


def test_xiaohongshu_daily_plan_uses_bounded_market_wide_queries(session, settings):
    topic = session.scalar(select(Topic).where(Topic.slug == "gold"))
    current = jobs_module.now_utc()
    specs = jobs_module._backfill_job_specs(
        [topic],
        ["xiaohongshu"],
        since=current - timedelta(hours=24),
        until=current,
        configured_queries=jobs_module._source_queries(settings.config_dir),
        configured_variants=jobs_module._source_query_variants(settings.config_dir),
        configured_page_limits=jobs_module._source_page_limits(settings.config_dir),
        default_page_limit=100,
    )

    assert len(specs) == 6
    assert [row["query"] for row in specs] == ["投资", "股票", "ETF", "基金", "持仓", "散户"]
    assert all(row["topic"] is None and row["sort_by"] == "最新" for row in specs)
    assert all(row["page_limit"] == 1 for row in specs)
    assert all(row["sampling_mode"] == "daily" for row in specs)


def test_xiaohongshu_old_single_date_uses_historical_budget(session, settings):
    topic = session.scalar(select(Topic).where(Topic.slug == "gold"))
    current = jobs_module.now_utc()
    specs = jobs_module._backfill_job_specs(
        [topic],
        ["xiaohongshu"],
        since=current - timedelta(days=31),
        until=current - timedelta(days=30),
        configured_queries=jobs_module._source_queries(settings.config_dir),
        configured_variants=jobs_module._source_query_variants(settings.config_dir),
        configured_page_limits=jobs_module._source_page_limits(settings.config_dir),
        default_page_limit=100,
    )

    assert all(row["page_limit"] == 20 for row in specs)
    assert all(row["sampling_mode"] == "historical" for row in specs)


def test_xiaohongshu_historical_budget_exhaustion_is_explicitly_partial(
    session, settings, tmp_path, monkeypatch
):
    def fake_collect_source(_session, _source_name, **_kwargs):
        return {
            "pages": 1,
            "items_collected": 1,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_partial": False,
            "source_degraded": False,
            "warnings": [],
            "diagnostics": {
                "reached_window_start": False,
                "retained_publication_days": {"2026-07-28": 1},
            },
            "exhausted": False,
            "next_cursor": "next-history-page",
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    monkeypatch.setattr(
        jobs_module,
        "_xiaohongshu_discovery_config",
        lambda _path: {
            "queries": ["投资"],
            "daily_max_pages": 1,
            "historical_max_pages": 1,
        },
    )
    kwargs = {
        "source_names": ["xiaohongshu"],
        "settings": settings,
        "topic_slugs": {"gold"},
        "state_path": tmp_path / "xhs-old-day.json",
        "since": datetime(2026, 7, 28, tzinfo=UTC),
        "until": datetime(2026, 7, 29, tzinfo=UTC),
        "max_jobs": 1,
        "cooldown_seconds": 0,
    }

    first = jobs_module.backfill_active_topics(session, **kwargs)
    second = jobs_module.backfill_active_topics(session, **kwargs)

    assert first["pending_jobs"] == 1
    assert second["completed"] is True
    assert second["coverage_complete"] is False
    assert second["jobs"][0]["terminal_reason"] == "partial_budget_exhausted"
    assert second["query_coverage"][0]["daily_samples"] == {"2026-07-28": 1}


def test_zhihu_backfill_plans_daily_market_questions_not_topic_keywords(session, settings):
    topics = session.scalars(select(Topic).order_by(Topic.id)).all()
    specs = jobs_module._backfill_job_specs(
        topics,
        ["zhihu"],
        since=datetime(2026, 8, 18, 16, tzinfo=UTC),
        until=datetime(2026, 8, 21, 10, tzinfo=UTC),
        configured_queries=jobs_module._source_queries(settings.config_dir),
        configured_variants=jobs_module._source_query_variants(settings.config_dir),
        configured_page_limits=jobs_module._source_page_limits(settings.config_dir),
        default_page_limit=100,
    )

    assert len(specs) == 9
    assert {row["topic"].slug for row in specs} == {
        "broad-a-share",
        "hang-seng-tech",
        "nasdaq",
    }
    assert all(row["page_limit"] == 1 and row["initial_cursor"] is None for row in specs)
    assert {row["query"] for row in specs[:3]} == {
        "如何看待2026年8月21日A股市场行情走势？",
        "如何看待2026年8月21日港股市场行情走势？",
        "如何看待2026年8月21日美股市场行情走势？",
    }
    assert all("黄金 股票" not in row["query"] for row in specs)


def test_xiaohongshu_backfill_job_budget_resumes_next_round_robin_batch(
    session, settings, tmp_path, monkeypatch
):
    calls: list[dict] = []

    def fake_collect_source(_session, source_name, **kwargs):
        calls.append({"source": source_name, **kwargs})
        return {
            "pages": 1,
            "items_collected": 3,
            "duplicates": 0,
            "topic_links_added": 3,
            "source_partial": False,
            "source_degraded": False,
            "warnings": [],
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    state_path = tmp_path / "xiaohongshu-backfill.json"
    window = {
        "since": datetime(2026, 5, 18, tzinfo=UTC),
        "until": datetime(2026, 8, 18, tzinfo=UTC),
    }

    first = jobs_module.backfill_active_topics(
        session,
        source_names=["xiaohongshu"],
        settings=settings,
        topic_slugs={"gold", "broad-a-share"},
        state_path=state_path,
        max_jobs=2,
        cooldown_seconds=0,
        **window,
    )
    second = jobs_module.backfill_active_topics(
        session,
        source_names=["xiaohongshu"],
        settings=settings,
        topic_slugs={"gold", "broad-a-share"},
        state_path=state_path,
        max_jobs=2,
        cooldown_seconds=0,
        **window,
    )

    assert first["attempted_jobs"] == 2
    assert first["pending_jobs"] == 4
    assert second["attempted_jobs"] == 2
    assert second["pending_jobs"] == 2
    assert [row["query"] for row in calls] == [
        "投资",
        "股票",
        "ETF",
        "基金",
    ]
    assert all(row["start_cursor"] for row in calls)


def test_backfill_per_source_budget_reaches_jobs_before_wrapped_resume_point(
    session, settings, tmp_path, monkeypatch
):
    calls: list[str] = []

    def fake_collect_source(_session, source_name, **_kwargs):
        calls.append(source_name)
        return {
            "pages": 1,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_partial": False,
            "source_degraded": False,
            "warnings": [],
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    state_path = tmp_path / "wrapped-source-budget.json"
    window = {
        "since": datetime(2026, 5, 18, tzinfo=UTC),
        "until": datetime(2026, 8, 18, tzinfo=UTC),
    }

    jobs_module.backfill_active_topics(
        session,
        source_names=["guba", "xiaohongshu"],
        settings=settings,
        topic_slugs={"gold", "broad-a-share"},
        state_path=state_path,
        max_jobs=1,
        cooldown_seconds=0,
        **window,
    )
    checkpoint = json.loads(state_path.read_text(encoding="utf-8"))
    checkpoint["next_job_key"] = "market-wide:xiaohongshu:query:01"
    state_path.write_text(json.dumps(checkpoint), encoding="utf-8")
    calls.clear()

    result = jobs_module.backfill_active_topics(
        session,
        source_names=["guba", "xiaohongshu"],
        settings=settings,
        topic_slugs={"gold", "broad-a-share"},
        state_path=state_path,
        max_jobs_per_source=1,
        cooldown_seconds=0,
        **window,
    )

    assert result["attempted_jobs"] == 2
    assert calls == ["xiaohongshu", "guba"]


def test_backfill_runs_different_sources_concurrently(
    session, settings, tmp_path, monkeypatch
):
    active = 0
    maximum_active = 0
    started: list[str] = []

    async def fake_collect_source_async(_session, source_name, **_kwargs):
        nonlocal active, maximum_active
        started.append(source_name)
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.02)
        active -= 1
        return {
            "source": source_name,
            "pages": 1,
            "items_collected": 1,
            "duplicates": 0,
            "topic_links_added": 1,
            "source_partial": False,
            "source_degraded": False,
            "warnings": [],
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source_async", fake_collect_source_async)

    result = jobs_module.backfill_active_topics(
        session,
        source_names=["guba", "taoguba"],
        settings=settings,
        topic_slugs={"gold"},
        state_path=tmp_path / "parallel-sources.json",
        since=datetime(2026, 8, 19, 16, tzinfo=UTC),
        until=datetime(2026, 8, 20, 16, tzinfo=UTC),
        max_jobs_per_source=1,
        source_concurrency=2,
        cooldown_seconds=0,
    )

    assert started == ["guba", "taoguba"]
    assert maximum_active == 2
    assert result["attempted_jobs"] == 2
    assert result["pending_jobs"] == 0
    assert all(row["done"] for row in result["jobs"])


def test_parallel_backfill_isolates_one_source_failure(
    session, settings, tmp_path, monkeypatch
):
    async def fake_collect_source_async(_session, source_name, **kwargs):
        await asyncio.sleep(0)
        if source_name == "guba":
            raise RuntimeError("unexpected worker failure")
        return {
            "source": source_name,
            "pages": 1,
            "items_collected": 1,
            "duplicates": 0,
            "topic_links_added": 1,
            "source_partial": False,
            "source_degraded": False,
            "warnings": [],
            "error": None,
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source_async", fake_collect_source_async)

    result = jobs_module.backfill_active_topics(
        session,
        source_names=["guba", "taoguba"],
        settings=settings,
        topic_slugs={"gold"},
        state_path=tmp_path / "parallel-failure.json",
        since=datetime(2026, 8, 19, 16, tzinfo=UTC),
        until=datetime(2026, 8, 20, 16, tzinfo=UTC),
        max_jobs_per_source=1,
        source_concurrency=2,
        cooldown_seconds=0,
    )

    jobs = {row["source"]: row for row in result["jobs"]}
    assert result["attempted_jobs"] == 2
    assert jobs["guba"]["done"] is False
    assert jobs["guba"]["next_retry_at"] is not None
    assert jobs["taoguba"]["done"] is True


@pytest.mark.asyncio
async def test_request_rate_limiter_persists_interval_across_instances(tmp_path, monkeypatch):
    monkeypatch.setenv("RETAIL_TIDE_STATE_DIR", str(tmp_path))
    first = RequestRateLimiter()
    second = RequestRateLimiter()

    await first.wait("example-source", 0.05)
    started = time.monotonic()
    await second.wait("example-source", 0.05)

    assert time.monotonic() - started >= 0.04


@pytest.mark.asyncio
async def test_request_rate_limiter_keeps_interval_after_slow_operation(tmp_path, monkeypatch):
    monkeypatch.setenv("RETAIL_TIDE_STATE_DIR", str(tmp_path))
    first = RequestRateLimiter()
    await first.wait("example-source", 0.01)
    await asyncio.sleep(0.02)
    await first.defer("example-source", 0.05)

    started = time.monotonic()
    await RequestRateLimiter().wait("example-source", 0.05)

    assert time.monotonic() - started >= 0.04


def test_guba_backfill_caps_each_resumable_batch_at_ten_pages(
    session, settings, tmp_path, monkeypatch
):
    calls: list[dict] = []

    def fake_collect_source(_session, source_name, **kwargs):
        calls.append({"source": source_name, **kwargs})
        return {
            "pages": 10,
            "items_collected": 30,
            "duplicates": 0,
            "topic_links_added": 30,
            "source_partial": False,
            "source_degraded": False,
            "warnings": [],
            "exhausted": False,
            "next_cursor": "next-guba-page",
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    result = jobs_module.backfill_active_topics(
        session,
        source_names=["guba"],
        settings=settings,
        topic_slugs={"semiconductor"},
        state_path=tmp_path / "guba-backfill.json",
        since=datetime(2026, 7, 26, tzinfo=UTC),
        until=datetime(2026, 8, 25, tzinfo=UTC),
        batch_pages=100,
        max_jobs=1,
        cooldown_seconds=0,
    )

    assert result["attempted_jobs"] == 1
    assert calls[0]["max_pages"] == 10
    assert result["jobs"][0]["pages"] == 10


def test_xiaohongshu_partial_strategy_keeps_page_budget_for_next_run(
    session, settings, tmp_path, monkeypatch
):
    outcomes = iter(("partial", "success"))

    def fake_collect_source(_session, _source_name, **_kwargs):
        outcome = next(outcomes)
        partial = outcome == "partial"
        return {
            "pages": 1,
            "items_collected": 2,
            "duplicates": 0,
            "topic_links_added": 2,
            "source_partial": partial,
            "source_degraded": False,
            "warnings": ["temporary search fallback"] if partial else [],
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    state_path = tmp_path / "xiaohongshu-partial.json"
    kwargs = {
        "source_names": ["xiaohongshu"],
        "settings": settings,
        "topic_slugs": {"ai"},
        "state_path": state_path,
        "since": datetime(2026, 5, 18, tzinfo=UTC),
        "until": datetime(2026, 8, 18, tzinfo=UTC),
        "max_jobs": 1,
        "cooldown_seconds": 0,
    }

    first = jobs_module.backfill_active_topics(session, **kwargs)
    checkpoint = json.loads(state_path.read_text(encoding="utf-8"))
    checkpoint["next_job_key"] = "market-wide:xiaohongshu:query:00"
    checkpoint["jobs"]["market-wide:xiaohongshu:query:00"]["next_retry_at"] = datetime(
        2026, 5, 1, tzinfo=UTC
    ).isoformat()
    state_path.write_text(json.dumps(checkpoint), encoding="utf-8")
    second = jobs_module.backfill_active_topics(session, **kwargs)

    assert first["jobs"][0]["partial"] is True
    assert first["jobs"][0]["pages"] == 0
    assert second["jobs"][0]["done"] is True
    assert second["jobs"][0]["pages"] == 1
    assert second["jobs"][0]["attempts"] == 2


def test_backfill_defers_partial_job_until_retry_time(session, settings, tmp_path, monkeypatch):
    calls = 0

    def fake_collect_source(_session, _source_name, **_kwargs):
        nonlocal calls
        calls += 1
        return {
            "pages": 1,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_partial": True,
            "source_degraded": False,
            "warnings": ["publication time could not be verified"],
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    result = jobs_module.backfill_active_topics(
        session,
        source_names=["guba"],
        settings=settings,
        topic_slugs={"gold"},
        state_path=tmp_path / "partial-terminal.json",
        since=datetime(2026, 5, 18, tzinfo=UTC),
        until=datetime(2026, 8, 18, tzinfo=UTC),
        max_jobs=1,
        max_retries=6,
        cooldown_seconds=0,
    )

    deferred = jobs_module.backfill_active_topics(
        session,
        source_names=["guba"],
        settings=settings,
        topic_slugs={"gold"},
        state_path=tmp_path / "partial-terminal.json",
        since=datetime(2026, 5, 18, tzinfo=UTC),
        until=datetime(2026, 8, 18, tzinfo=UTC),
        max_jobs=1,
        max_retries=6,
        cooldown_seconds=0,
    )

    assert result["completed"] is False
    assert result["pending_jobs"] == 1
    assert result["jobs"][0]["next_retry_at"] is not None
    assert deferred["attempted_jobs"] == 0
    assert len(deferred["deferred_jobs"]) == 1
    assert calls == 1


def test_backfill_partial_strategy_does_not_cool_down_other_strategies(
    session, settings, tmp_path, monkeypatch
):
    calls: list[str] = []

    def fake_collect_source(_session, _source_name, **kwargs):
        calls.append(kwargs["query"])
        partial = len(calls) == 1
        return {
            "pages": 1,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_partial": partial,
            "source_degraded": False,
            "warnings": ["temporary search fallback"] if partial else [],
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    state_path = tmp_path / "xiaohongshu-strategy-cooldown.json"
    kwargs = {
        "source_names": ["xiaohongshu"],
        "settings": settings,
        "topic_slugs": {"ai"},
        "state_path": state_path,
        "since": datetime(2026, 5, 18, tzinfo=UTC),
        "until": datetime(2026, 8, 18, tzinfo=UTC),
        "max_jobs_per_source": 1,
        "cooldown_seconds": 0,
    }

    first = jobs_module.backfill_active_topics(session, **kwargs)
    second = jobs_module.backfill_active_topics(session, **kwargs)

    assert first["attempted_jobs"] == 1
    assert first["jobs"][0]["partial"] is True
    assert second["attempted_jobs"] == 1
    assert len(calls) == 2
    assert calls[0] != calls[1]


def test_backfill_repeated_partial_strategies_open_source_circuit(
    session, settings, tmp_path, monkeypatch
):
    calls: list[str] = []

    def fake_collect_source(_session, _source_name, **kwargs):
        calls.append(kwargs["query"])
        return {
            "pages": 1,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_partial": True,
            "source_degraded": False,
            "warnings": ["search timed out"],
            "exhausted": True,
            "next_cursor": None,
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    state_path = tmp_path / "xiaohongshu-partial-circuit.json"
    kwargs = {
        "source_names": ["xiaohongshu"],
        "settings": settings,
        "topic_slugs": {"ai"},
        "state_path": state_path,
        "since": datetime(2026, 5, 18, tzinfo=UTC),
        "until": datetime(2026, 8, 18, tzinfo=UTC),
        "max_jobs_per_source": 1,
        "cooldown_seconds": 0,
    }

    attempts = [jobs_module.backfill_active_topics(session, **kwargs) for _ in range(4)]

    assert [result["attempted_jobs"] for result in attempts] == [1, 1, 1, 0]
    assert len(calls) == jobs_module.PARTIAL_SOURCE_CIRCUIT_BREAKER
    assert attempts[-1]["deferred_jobs"]
    assert all(
        "source cooldown until" in row["deferred_reason"] for row in attempts[-1]["deferred_jobs"]
    )


def test_backfill_failure_cools_down_every_job_for_the_source(
    session, settings, tmp_path, monkeypatch
):
    calls: list[str] = []

    def fake_collect_source(_session, _source_name, **kwargs):
        calls.append(kwargs["query"])
        return {
            "pages": 0,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_partial": False,
            "source_degraded": True,
            "error": "temporary upstream connection failure",
            "warnings": [],
            "exhausted": False,
            "next_cursor": kwargs.get("start_cursor"),
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    state_path = tmp_path / "source-cooldown.json"
    kwargs = {
        "source_names": ["guba"],
        "settings": settings,
        "topic_slugs": {"gold", "ai"},
        "state_path": state_path,
        "since": datetime(2026, 5, 18, tzinfo=UTC),
        "until": datetime(2026, 8, 18, tzinfo=UTC),
        "max_jobs": 2,
        "max_retries": 6,
        "cooldown_seconds": 0,
    }

    first = jobs_module.backfill_active_topics(session, **kwargs)
    second = jobs_module.backfill_active_topics(session, **kwargs)

    assert first["attempted_jobs"] == 1
    assert second["attempted_jobs"] == 0
    assert len(second["deferred_jobs"]) == 2
    assert all("source cooldown until" in row["deferred_reason"] for row in second["jobs"])
    assert calls == ["黄金"]


def test_backfill_marks_job_terminal_after_retry_limit(session, settings, tmp_path, monkeypatch):
    calls = 0

    def fake_collect_source(_session, _source_name, **kwargs):
        nonlocal calls
        calls += 1
        return {
            "pages": 0,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_partial": False,
            "source_degraded": True,
            "error": "temporary upstream timeout",
            "warnings": [],
            "exhausted": False,
            "next_cursor": kwargs.get("start_cursor"),
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    state_path = tmp_path / "retry-limit.json"
    kwargs = {
        "source_names": ["guba"],
        "settings": settings,
        "topic_slugs": {"gold"},
        "state_path": state_path,
        "since": datetime(2026, 8, 28, tzinfo=UTC),
        "until": datetime(2026, 8, 29, tzinfo=UTC),
        "max_jobs": 1,
        "max_retries": 1,
        "cooldown_seconds": 0,
    }

    first = jobs_module.backfill_active_topics(session, **kwargs)
    second = jobs_module.backfill_active_topics(session, **kwargs)

    assert first["completed"] is True
    assert first["pending_jobs"] == 0
    assert first["jobs"][0]["terminal"] is True
    assert first["jobs"][0]["terminal_reason"] == "retry_limit_exhausted"
    assert second["attempted_jobs"] == 0
    assert calls == 1


def test_backfill_drain_mode_stops_after_first_source_failure(
    session, settings, tmp_path, monkeypatch
):
    calls: list[str] = []

    def fake_collect_source(_session, _source_name, **kwargs):
        calls.append(kwargs["query"])
        return {
            "pages": 0,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "source_partial": False,
            "source_degraded": True,
            "error": "login expired",
            "warnings": [],
            "exhausted": False,
            "next_cursor": kwargs.get("start_cursor"),
        }

    monkeypatch.setattr(jobs_module, "collect_source", fake_collect_source)
    result = jobs_module.backfill_active_topics(
        session,
        source_names=["guba"],
        settings=settings,
        topic_slugs={"gold", "ai"},
        state_path=tmp_path / "drain-source-cooldown.json",
        since=datetime(2026, 5, 18, tzinfo=UTC),
        until=datetime(2026, 8, 18, tzinfo=UTC),
        max_retries=6,
        cooldown_seconds=0,
        one_batch_per_job=False,
    )

    assert calls == ["黄金"]
    assert result["attempted_jobs"] == 1
    assert result["pending_jobs"] == 2
    assert result["jobs"][0]["next_retry_at"] is not None


@pytest.mark.asyncio
async def test_xiaohongshu_search_failure_does_not_use_personalized_feed():
    search_requests = 0
    search_bodies: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal search_requests
        if request.url.path == "/api/v1/feeds/search":
            search_requests += 1
            search_bodies.append(json.loads(request.content))
            return httpx.Response(500, json={"error": "服务器内部错误"})
        raise AssertionError(f"unexpected fallback path: {request.url.path}")

    source = XiaohongshuSource(
        endpoint="http://xhs.test/mcp",
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_request_interval=0,
    )
    with pytest.raises(SourceError, match="filtered and unfiltered search failed"):
        await source.collect(
            "半导体 芯片 存储 股票",
            datetime(2026, 8, 14, 5, 0, tzinfo=UTC),
            until=datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
        )

    assert search_requests == 2
    assert "filters" in search_bodies[0]
    assert search_bodies[1] == {"keyword": "半导体 芯片 存储 股票"}


@pytest.mark.asyncio
async def test_xiaohongshu_filtered_search_failure_retries_without_filters_once():
    search_bodies: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/feeds/search"
        body = json.loads(request.content)
        search_bodies.append(body)
        if len(search_bodies) == 1:
            return httpx.Response(
                500,
                json={"error": "筛选面板不可用", "details": "找不到发布时间选项"},
            )
        return httpx.Response(200, json={"success": True, "data": {"feeds": []}})

    source = XiaohongshuSource(
        endpoint="http://xhs.test",
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_request_interval=0,
    )
    result = await source.collect(
        "黄金 ETF 投资",
        datetime(2026, 8, 14, 5, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )

    assert "filters" in search_bodies[0]
    assert search_bodies[1] == {"keyword": "黄金 ETF 投资"}
    assert result.items == []
    assert result.warnings == [
        (
            "filtered keyword search failed "
            "after 1 attempts "
            "(xiaohongshu MCP returned HTTP 500: 筛选面板不可用: 找不到发布时间选项); "
            "used one "
            "unfiltered keyword search and validated topic and publication time "
            "from note details locally"
        )
    ]


@pytest.mark.asyncio
async def test_xiaohongshu_transient_filtered_search_failure_recovers_without_partial():
    search_bodies: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/feeds/search"
        search_bodies.append(json.loads(request.content))
        if len(search_bodies) == 1:
            return httpx.Response(500, json={"error": "临时搜索失败"})
        return httpx.Response(200, json={"success": True, "data": {"feeds": []}})

    source = XiaohongshuSource(
        endpoint="http://xhs.test",
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        min_request_interval=0,
    )
    result = await source.collect(
        "黄金 ETF 投资",
        datetime(2026, 8, 14, 5, 0, tzinfo=UTC),
        until=datetime(2026, 8, 14, 6, 0, tzinfo=UTC),
    )

    assert len(search_bodies) == 2
    assert search_bodies[1] == {"keyword": "黄金 ETF 投资"}
    assert result.items == []
    assert any("unfiltered keyword search" in warning for warning in result.warnings)


def test_empty_and_bad_source_response():
    empty = parse_paged_response("fixture", [], observation_kind="forum_post")
    assert empty.items == [] and empty.exhausted
    with pytest.raises(SourceError):
        parse_paged_response(
            "fixture", {"items": [{"body": "no id"}]}, observation_kind="forum_post"
        )


def test_raw_collection_is_idempotent_and_versions_are_append_only(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    observation = RawObservationSchema(
        source="guba",
        source_item_id="stable-1",
        observation_kind="forum_post",
        published_at=datetime(2026, 8, 1, tzinfo=UTC),
        observed_at=datetime(2026, 8, 2, tzinfo=UTC),
        payload={"body": "黄金", "author_id": "private-source-id"},
    )
    first, inserted = insert_raw_observation(session, source.id, observation)
    second, duplicate = insert_raw_observation(session, source.id, observation)
    assert inserted and not duplicate and first.id == second.id
    edited = observation.model_copy(
        update={"payload": {"body": "黄金更新", "author_id": "private-source-id"}}
    )
    third, inserted = insert_raw_observation(session, source.id, edited)
    assert inserted and third.id != first.id
    normalize_raw_observation(session, first)
    normalize_raw_observation(session, third)
    session.commit()
    assert session.scalars(select(RawObservation)).all().__len__() == 2
    contents = session.scalars(
        select(Content).where(Content.source_id == source.id, Content.source_item_id == "stable-1")
    ).all()
    assert len(contents) == 1
    assert contents[0].body == "黄金更新"
    assert first.observed_at.tzinfo is not None


def test_ineligible_zhihu_rank_snapshot_remains_raw_only(session, settings):
    source = session.scalar(select(Source).where(Source.name == "zhihu"))
    topic = session.scalar(select(Topic).where(Topic.slug == "broad-a-share"))
    observation = RawObservationSchema(
        source="zhihu",
        source_item_id="answer:market-snapshot-only",
        observation_kind="zhihu_answer_snapshot",
        published_at=None,
        observed_at=datetime(2026, 8, 21, tzinfo=UTC),
        payload={
            "body": "市场复盘排名快照",
            "market_session_date": "2026-08-20",
            "publication_time_verified": False,
            "timestamp_semantics": "observed_rank_snapshot",
        },
    )
    raw, _inserted = insert_raw_observation(session, source.id, observation)
    link_raw_observation_topic(
        session,
        raw,
        topic_id=topic.id,
        collection_query="如何看待8月20日A股市场行情走势？",
    )
    session.commit()

    assert normalize_pending(session, limit=100, settings=settings) == 0
    assert (
        session.scalar(
            select(Content).where(
                Content.source_id == source.id,
                Content.source_item_id == observation.source_item_id,
            )
        )
        is None
    )
    with pytest.raises(ValueError, match="not eligible for reference analysis"):
        normalize_raw_observation(session, raw, settings=settings)


def test_eligible_zhihu_rank_snapshot_becomes_reference_content(session, settings):
    source = session.scalar(select(Source).where(Source.name == "zhihu"))
    topic = session.scalar(select(Topic).where(Topic.slug == "broad-a-share"))
    observation = RawObservationSchema(
        source="zhihu",
        source_item_id="answer:market-reference",
        observation_kind="zhihu_answer_snapshot",
        published_at=None,
        observed_at=datetime(2026, 8, 21, tzinfo=UTC),
        payload={
            "title": "如何看待2026年8月20日A股市场行情走势？",
            "body": "A股大盘成交放大，半导体板块走强，但不建议追高。",
            "market_session_date": "2026-08-20",
            "answer_edit_time": "2026-08-20T09:00:00+00:00",
            "publication_time_verified": False,
            "timestamp_semantics": "observed_rank_snapshot",
            "reference_eligible": True,
            "reference_reason": "explicit_market_session_date",
        },
    )
    raw, _inserted = insert_raw_observation(session, source.id, observation)
    link_raw_observation_topic(
        session,
        raw,
        topic_id=topic.id,
        collection_query="如何看待2026年8月20日A股市场行情走势？",
    )
    session.commit()

    assert normalize_pending(session, limit=100, settings=settings) == 1
    content = session.scalar(
        select(Content).where(
            Content.source_id == source.id,
            Content.source_item_id == "answer:market-reference@2026-08-20",
        )
    )
    assert content is not None
    assert content.kind == "reference_answer"
    assert content.published_at == datetime(2026, 8, 20, 7, tzinfo=UTC)
    assert content.body.startswith("A股大盘")


def test_collection_query_topic_is_preserved_outside_raw_payload(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    topic = session.scalar(select(Topic).where(Topic.slug == "semiconductor"))
    observation = RawObservationSchema(
        source="guba",
        source_item_id="stable-query-context-1",
        observation_kind="forum_post",
        published_at=datetime(2026, 8, 14, tzinfo=UTC),
        observed_at=datetime(2026, 8, 14, tzinfo=UTC),
        payload={"body": "今天成交明显放大，但正文没有重复搜索词。"},
    )
    raw, _inserted = insert_raw_observation(session, source.id, observation)
    assert link_raw_observation_topic(
        session,
        raw,
        topic_id=topic.id,
        collection_query=topic.name,
    )
    assert not link_raw_observation_topic(
        session,
        raw,
        topic_id=topic.id,
        collection_query=topic.name,
    )
    normalize_raw_observation(session, raw)
    resolve_pending_entities(session)

    link = session.scalar(select(RawObservationTopic))
    entity = session.scalar(
        select(ContentEntity).where(
            ContentEntity.entity_type == "topic",
            ContentEntity.entity_id == topic.id,
        )
    )
    assert link.collection_query == "半导体"
    assert "collection_query" not in raw.payload
    assert entity is not None
    assert entity.method == "collection_query"


def test_discovery_consumer_content_is_traceable_but_excluded_from_market_topic(session):
    source = session.scalar(select(Source).where(Source.name == "zhihu"))
    topic = session.scalar(select(Topic).where(Topic.slug == "liquor"))
    title = "今年还能囤酒吗? 2026年最保值的十款白酒"
    body = "欢迎关注公众号。飞天茅台适合送礼，价格稳定，是口粮酒清单。"
    observation = RawObservationSchema(
        source="zhihu",
        source_item_id="consumer-liquor-1",
        observation_kind="zhihu_article",
        published_at=datetime(2026, 8, 17, tzinfo=UTC),
        observed_at=datetime(2026, 8, 17, tzinfo=UTC),
        payload={"title": title, "body": body, "source_role": "discovery"},
    )
    raw, _inserted = insert_raw_observation(session, source.id, observation)
    link_raw_observation_topic(
        session,
        raw,
        topic_id=topic.id,
        collection_query="白酒 股票 ETF 投资",
    )
    content = normalize_raw_observation(session, raw)
    resolve_pending_entities(session)

    assert not is_market_relevant_text(f"{title} {body}")
    assert not is_market_relevant_content(
        "500元买什么白酒送人好",
        "白酒适合送礼，贵州茅台现在是 A 股市值第一名。",
    )
    assert session.scalar(
        select(RawObservationTopic).where(RawObservationTopic.raw_observation_id == raw.id)
    )
    assert (
        session.scalar(select(ContentEntity).where(ContentEntity.content_id == content.id)) is None
    )


def test_discovery_content_with_securities_context_keeps_topic_link(session):
    source = session.scalar(select(Source).where(Source.name == "zhihu"))
    topic = session.scalar(select(Topic).where(Topic.slug == "liquor"))
    body = "白酒板块估值回落，贵州茅台 600519.SH 的股价和成交量值得继续观察。"
    observation = RawObservationSchema(
        source="zhihu",
        source_item_id="market-liquor-1",
        observation_kind="zhihu_article",
        published_at=datetime(2026, 8, 17, tzinfo=UTC),
        observed_at=datetime(2026, 8, 17, tzinfo=UTC),
        payload={"title": "白酒板块还能买吗", "body": body, "source_role": "discovery"},
    )
    raw, _inserted = insert_raw_observation(session, source.id, observation)
    link_raw_observation_topic(
        session,
        raw,
        topic_id=topic.id,
        collection_query="白酒 股票 ETF 投资",
    )
    content = normalize_raw_observation(session, raw)
    resolve_pending_entities(session)

    assert is_market_relevant_text(body)
    assert session.scalar(
        select(ContentEntity).where(
            ContentEntity.content_id == content.id,
            ContentEntity.entity_type == "topic",
            ContentEntity.entity_id == topic.id,
        )
    )


def test_discovery_query_context_does_not_force_wrong_topic(session):
    source = session.scalar(select(Source).where(Source.name == "zhihu"))
    liquor = session.scalar(select(Topic).where(Topic.slug == "liquor"))
    broad = session.scalar(select(Topic).where(Topic.slug == "broad-a-share"))
    observation = RawObservationSchema(
        source="zhihu",
        source_item_id="broad-result-from-liquor-query",
        observation_kind="zhihu_answer",
        published_at=datetime(2026, 8, 17, tzinfo=UTC),
        observed_at=datetime(2026, 8, 17, tzinfo=UTC),
        payload={
            "title": "如何看待今天的 A 股大盘行情",
            "body": "上证指数成交量放大，金融权重领涨。",
            "source_role": "discovery",
        },
    )
    raw, _inserted = insert_raw_observation(session, source.id, observation)
    link_raw_observation_topic(
        session,
        raw,
        topic_id=liquor.id,
        collection_query="白酒 股票 ETF 投资",
    )
    content = normalize_raw_observation(session, raw)
    resolve_pending_entities(session)

    topic_ids = set(
        session.scalars(
            select(ContentEntity.entity_id).where(
                ContentEntity.content_id == content.id,
                ContentEntity.entity_type == "topic",
            )
        ).all()
    )
    assert broad.id in topic_ids
    assert liquor.id not in topic_ids


def test_truncated_recrawl_does_not_replace_a_complete_raw_version(session):
    source = session.scalar(select(Source).where(Source.name == "guba"))
    complete = RawObservationSchema(
        source="guba",
        source_item_id="stable-complete-1",
        observation_kind="forum_post",
        published_at=datetime(2026, 8, 1, tzinfo=UTC),
        observed_at=datetime(2026, 8, 2, tzinfo=UTC),
        payload={"body": "完整正文内容", "body_truncated": False, "views": 10},
    )
    first, inserted = insert_raw_observation(session, source.id, complete)
    assert inserted

    degraded = complete.model_copy(
        update={"payload": {"body": "完整正文...", "body_truncated": True, "views": 11}}
    )
    preserved, inserted = insert_raw_observation(session, source.id, degraded)

    assert not inserted
    assert preserved.id == first.id
    assert session.scalars(
        select(RawObservation).where(RawObservation.source_item_id == "stable-complete-1")
    ).all() == [first]
