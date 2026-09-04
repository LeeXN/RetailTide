import json
from dataclasses import replace
from datetime import datetime, timedelta

import pytest
import typer
from typer.testing import CliRunner

import retail_tide.cli as cli_module
from retail_tide.config import (
    DEFAULT_PUBLIC_SOURCES,
    Settings,
    SourceCredential,
    llm_config_status,
    source_config_status,
)
from retail_tide.pipeline.analysis import (
    FailoverAnalysisProvider,
    OpenAICompatibleAnalysisProvider,
    analysis_provider_for_settings,
)
from retail_tide.time import (
    SHANGHAI,
    UTC,
    floor_bucket,
    parse_collection_bound,
    resolve_collection_window,
    scheduled_post_window,
)


def test_default_window_is_today_in_shanghai_until_now():
    current = datetime(2026, 8, 14, 7, 30, tzinfo=UTC)

    start, end = resolve_collection_window(now=current)

    assert start == datetime(2026, 8, 13, 16, tzinfo=UTC)
    assert end == current


def test_scheduled_post_windows_use_shanghai_calendar_boundaries():
    noon = datetime(2026, 8, 21, 4, 0, tzinfo=UTC)

    assert scheduled_post_window(current=noon) == (
        datetime(2026, 8, 19, 16, 0, tzinfo=UTC),
        datetime(2026, 8, 20, 16, 0, tzinfo=UTC),
    )


def test_scheduled_refresh_runs_previous_day_with_market_sync(monkeypatch, tmp_path):
    start = datetime(2026, 8, 20, 16, 0, tzinfo=UTC)
    end = datetime(2026, 8, 21, 4, 0, tzinfo=UTC)
    captured = {}
    monkeypatch.setenv(
        "RETAIL_TIDE_SCHEDULED_STATE_FILE",
        str(tmp_path / "scheduled-refresh.json"),
    )
    monkeypatch.setattr(cli_module, "scheduled_post_window", lambda: (start, end))
    monkeypatch.setattr(cli_module, "refresh", lambda **kwargs: captured.update(kwargs))

    cli_module.scheduled_refresh(limit=123)

    assert captured["since"] == start.isoformat()
    assert captured["until"] == end.isoformat()
    assert captured["limit"] == 123
    assert captured["name"] is None
    assert captured["exclude_source"] == ["wikimedia-pageviews"]
    assert captured["sync_market_data"] is True


def test_scheduled_wikimedia_runs_independently_without_market_sync(monkeypatch, tmp_path):
    start = datetime(2026, 8, 30, 16, 0, tzinfo=UTC)
    end = datetime(2026, 8, 31, 16, 0, tzinfo=UTC)
    captured = {}
    monkeypatch.setenv(
        "RETAIL_TIDE_SCHEDULED_WIKIMEDIA_STATE_FILE",
        str(tmp_path / "scheduled-wikimedia.json"),
    )
    monkeypatch.setattr(cli_module, "scheduled_post_window", lambda: (start, end))
    monkeypatch.setattr(cli_module, "refresh", lambda **kwargs: captured.update(kwargs))

    cli_module.scheduled_wikimedia(limit=456)

    assert captured["since"] == start.isoformat()
    assert captured["until"] == end.isoformat()
    assert captured["limit"] == 456
    assert captured["name"] == "wikimedia-pageviews"
    assert captured["exclude_source"] is None
    assert captured["sync_market_data"] is False


def test_rebuild_derived_runs_pipeline_market_and_return_recalculation(monkeypatch):
    class Session:
        closed = False

        def close(self):
            self.closed = True

    session = Session()
    settings = Settings()
    captured = {}
    monkeypatch.setattr(cli_module, "_session", lambda: (settings, object(), session))
    monkeypatch.setattr(
        cli_module,
        "remove_misnormalized_zhihu_snapshots",
        lambda actual_session: {"content": 0},
    )
    monkeypatch.setattr(
        cli_module,
        "reset_metric_event_derivatives",
        lambda actual_session: {"platform_metrics": 0},
    )
    monkeypatch.setattr(
        cli_module,
        "run_core_pipeline",
        lambda actual_session, *, limit, settings: (
            captured.update({"session": actual_session, "limit": limit, "settings": settings})
            or {"analyzed": 7}
        ),
    )
    monkeypatch.setattr(
        cli_module,
        "_sync_topic_market",
        lambda actual_session, actual_settings, *, end: {"assets": [], "errors": []},
    )
    monkeypatch.setattr(
        cli_module,
        "evaluate_events",
        lambda actual_session, *, settings: {"evaluated": 3},
    )
    monkeypatch.setattr(cli_module, "_json", lambda value: captured.update({"output": value}))

    cli_module.rebuild_derived(limit=123, sync_market_data=True)

    assert captured["session"] is session
    assert captured["limit"] == 123
    assert captured["settings"] is settings
    assert captured["output"]["pipeline"]["analyzed"] == 7
    assert captured["output"]["pipeline"]["returns_after_market_sync"] == {"evaluated": 3}
    assert session.closed


def test_history_requires_explicit_days_or_range():
    current = datetime(2026, 8, 14, 7, 30, tzinfo=UTC)
    start, end = resolve_collection_window(days=7, now=current)
    assert end - start == timedelta(days=7)

    with pytest.raises(ValueError, match="days cannot be combined"):
        resolve_collection_window(days=7, since=current - timedelta(days=1), now=current)
    with pytest.raises(ValueError, match="since is required"):
        resolve_collection_window(until=current, now=current)


def test_date_only_collection_end_is_inclusive_in_shanghai():
    assert parse_collection_bound("2026-08-23") == datetime(2026, 8, 22, 16, tzinfo=UTC)
    assert parse_collection_bound("2026-08-23", end=True) == datetime(2026, 8, 23, 16, tzinfo=UTC)
    assert SHANGHAI.key == "Asia/Shanghai"


def test_daily_bucket_uses_shanghai_natural_day():
    assert floor_bucket(datetime(2026, 8, 14, 15, 59, tzinfo=UTC), "1d") == datetime(
        2026, 8, 13, 16, tzinfo=UTC
    )
    assert floor_bucket(datetime(2026, 8, 14, 16, 0, tzinfo=UTC), "1d") == datetime(
        2026, 8, 14, 16, tzinfo=UTC
    )


def test_backfill_default_days_does_not_conflict_with_exact_range():
    assert cli_module._backfill_days(None, None, None) is None
    assert (
        cli_module._backfill_days(
            "2026-08-18T00:00:00+08:00",
            "2026-08-19T10:49:56+08:00",
            None,
        )
        is None
    )


def test_cli_range_expands_to_two_explicit_date_bounds():
    assert cli_module._apply_date_range("2026-08-20-2026-08-23", None, None, None) == (
        "2026-08-20",
        "2026-08-23",
        None,
    )
    with pytest.raises(ValueError, match="cannot be combined"):
        cli_module._apply_date_range("2026-08-20-2026-08-23", None, None, 3)


def test_cli_days_are_normalized_to_explicit_bounds_once():
    current = datetime(2026, 8, 25, 6, 30, tzinfo=UTC)
    start, end, days = cli_module._cli_collection_bounds(None, None, 3, current=current)
    assert start == datetime(2026, 8, 22, 16, 0, tzinfo=UTC)
    assert end == current
    assert days is None


def test_cli_single_date_is_exclusive_and_expands_to_one_day():
    assert cli_module._apply_single_date("2026-08-24", None, None, None, None) == (
        "2026-08-24",
        "2026-08-24",
        None,
        None,
    )
    with pytest.raises(ValueError, match="cannot be combined"):
        cli_module._apply_single_date("2026-08-24", "2026-08-20", None, None, None)


def test_open_collection_windows_get_one_checkpoint_per_shanghai_date():
    first = datetime(2026, 8, 25, 6, 30, tzinfo=UTC)
    same_date = datetime(2026, 8, 25, 10, 0, tzinfo=UTC)
    next_date = datetime(2026, 8, 26, 1, 0, tzinfo=UTC)
    kwargs = {
        "source_names": ["guba"],
        "since": None,
        "until": None,
        "date_range": None,
        "days": None,
        "single_date": "2026-08-25",
        "topic_slugs": [],
    }

    first_key = cli_module._collection_resume_key(current=first, **kwargs)
    assert cli_module._collection_resume_key(current=same_date, **kwargs) == first_key
    assert cli_module._collection_resume_key(current=next_date, **kwargs) != first_key

    closed_kwargs = {**kwargs, "single_date": "2026-08-24"}
    assert cli_module._collection_resume_key(
        current=first, **closed_kwargs
    ) == cli_module._collection_resume_key(current=next_date, **closed_kwargs)

    historical_kwargs = {
        **kwargs,
        "since": "2026-07-26",
        "single_date": None,
    }
    assert cli_module._collection_resume_key(
        current=first, **historical_kwargs
    ) == cli_module._collection_resume_key(current=next_date, **historical_kwargs)


def test_public_cli_exposes_only_product_commands():
    result = CliRunner().invoke(cli_module.app, ["--help"])

    assert result.exit_code == 0
    for command in ("setup", "serve", "status", "refresh"):
        assert command in result.stdout
    for internal in ("backfill", "normalize", "analyze", "aggregate"):
        assert internal not in result.stdout


def test_refresh_requires_an_explicit_product_window():
    result = CliRunner().invoke(cli_module.app, ["refresh"])

    assert result.exit_code == 2
    assert "refresh requires --date, --days, or --since" in result.stderr


def test_scheduled_refresh_pins_failed_window_until_success(tmp_path, monkeypatch):
    state_path = tmp_path / "scheduled-refresh.json"
    start = datetime(2026, 8, 20, 16, tzinfo=UTC)
    end = datetime(2026, 8, 21, 16, tzinfo=UTC)
    calls = []

    monkeypatch.setenv("RETAIL_TIDE_SCHEDULED_STATE_FILE", str(state_path))
    monkeypatch.setattr(cli_module, "scheduled_post_window", lambda: (start, end))

    def fail_once(**kwargs):
        calls.append(kwargs)
        raise typer.Exit(code=1)

    monkeypatch.setattr(cli_module, "refresh", fail_once)
    with pytest.raises(typer.Exit):
        cli_module.scheduled_refresh(limit=123)

    assert state_path.exists()
    assert state_path.stat().st_mode & 0o777 == 0o600

    monkeypatch.setattr(
        cli_module,
        "scheduled_post_window",
        lambda: (start, end),
    )
    monkeypatch.setattr(cli_module, "refresh", lambda **kwargs: calls.append(kwargs))
    cli_module.scheduled_refresh(limit=123)

    assert [call["since"] for call in calls] == [start.isoformat(), start.isoformat()]
    assert [call["until"] for call in calls] == [end.isoformat(), end.isoformat()]
    assert all(call["limit"] == 123 for call in calls)
    assert all(call["sync_market_data"] is True for call in calls)
    assert not state_path.exists()


def test_scheduled_refresh_catches_up_closed_days_after_pinned_success(tmp_path, monkeypatch):
    state_path = tmp_path / "scheduled-refresh.json"
    first_start = datetime(2026, 9, 1, 16, tzinfo=UTC)
    first_end = datetime(2026, 9, 2, 16, tzinfo=UTC)
    latest_start = first_end
    latest_end = datetime(2026, 9, 3, 16, tzinfo=UTC)
    calls = []
    state_path.write_text(
        f'{{"version":1,"since":"{first_start.isoformat()}","until":"{first_end.isoformat()}"}}',
        encoding="utf-8",
    )

    monkeypatch.setenv("RETAIL_TIDE_SCHEDULED_STATE_FILE", str(state_path))
    monkeypatch.setattr(
        cli_module,
        "scheduled_post_window",
        lambda: (latest_start, latest_end),
    )
    monkeypatch.setattr(cli_module, "refresh", lambda **kwargs: calls.append(kwargs))

    cli_module.scheduled_refresh(limit=123)

    assert [(call["since"], call["until"]) for call in calls] == [
        (first_start.isoformat(), first_end.isoformat()),
        (latest_start.isoformat(), latest_end.isoformat()),
    ]
    assert not state_path.exists()


def test_scheduled_refresh_pins_the_next_window_when_catchup_fails(tmp_path, monkeypatch):
    state_path = tmp_path / "scheduled-refresh.json"
    first_start = datetime(2026, 9, 1, 16, tzinfo=UTC)
    first_end = datetime(2026, 9, 2, 16, tzinfo=UTC)
    latest_start = first_end
    latest_end = datetime(2026, 9, 3, 16, tzinfo=UTC)
    calls = []
    state_path.write_text(
        f'{{"version":1,"since":"{first_start.isoformat()}","until":"{first_end.isoformat()}"}}',
        encoding="utf-8",
    )

    monkeypatch.setenv("RETAIL_TIDE_SCHEDULED_STATE_FILE", str(state_path))
    monkeypatch.setattr(
        cli_module,
        "scheduled_post_window",
        lambda: (latest_start, latest_end),
    )

    def fail_catchup(**kwargs):
        calls.append(kwargs)
        if len(calls) == 2:
            raise typer.Exit(code=1)

    monkeypatch.setattr(cli_module, "refresh", fail_catchup)

    with pytest.raises(typer.Exit):
        cli_module.scheduled_refresh(limit=123)

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert [(call["since"], call["until"]) for call in calls] == [
        (first_start.isoformat(), first_end.isoformat()),
        (latest_start.isoformat(), latest_end.isoformat()),
    ]
    assert state["since"] == latest_start.isoformat()
    assert state["until"] == latest_end.isoformat()
    assert state_path.stat().st_mode & 0o777 == 0o600


def test_terminal_collection_jobs_do_not_block_available_pipeline_data():
    collection = {
        "jobs": [
            {"source": "guba", "done": True, "terminal": False},
            {
                "source": "zhihu",
                "done": False,
                "terminal": True,
                "terminal_reason": "retry_limit_exhausted",
            },
        ]
    }

    assert cli_module._bounded_collection_blockers(collection) == []


def test_repair_derived_rebuilds_wikimedia_signals_without_llm(settings, monkeypatch):
    class Session:
        closed = False

        def close(self):
            self.closed = True

    session = Session()
    captured = {}
    monkeypatch.setattr(cli_module, "_session", lambda: (settings, object(), session))
    monkeypatch.setattr(
        cli_module,
        "remove_misnormalized_zhihu_snapshots",
        lambda *_args, **_kwargs: {"content": 0},
    )
    monkeypatch.setattr(
        cli_module,
        "reset_metric_event_derivatives",
        lambda *_args, **_kwargs: {"platform_metrics": 0},
    )
    monkeypatch.setattr(
        cli_module,
        "aggregate_trend_signals",
        lambda *_args, **_kwargs: 30,
    )
    monkeypatch.setattr(
        cli_module,
        "aggregate_metrics",
        lambda *_args, bucket_size, **_kwargs: 10 if bucket_size == "1h" else 20,
    )
    monkeypatch.setattr(cli_module, "detect_events", lambda *_args, **_kwargs: 3)
    monkeypatch.setattr(cli_module, "evaluate_events", lambda *_args, **_kwargs: 4)
    monkeypatch.setattr(cli_module, "_json", lambda value: captured.update(value))

    cli_module.repair_derived()

    assert captured["trend_signals"] == 30
    assert captured["metrics_1h"] == 10
    assert captured["metrics_1d"] == 20
    assert session.closed


def test_refresh_keeps_retry_state_but_analyzes_available_required_data(
    settings, tmp_path, monkeypatch
):
    class Session:
        closed = False

        def close(self):
            self.closed = True

    session = Session()
    settings = replace(
        settings,
        enabled_sources=("guba",),
        run_lock_file=tmp_path / "refresh.lock",
    )
    captured = {}
    job = {
        "source": "guba",
        "topic_slug": "gold",
        "done": False,
        "terminal": False,
        "items_collected": 2,
        "duplicates": 0,
        "topic_links_added": 2,
        "error": "source cooldown until later",
    }
    collection = {
        "completed": False,
        "state_file": str(tmp_path / "state.json"),
        "attempted_jobs": 1,
        "pending_jobs": 1,
        "terminal_jobs": [],
        "deferred_jobs": [job],
        "degraded": [job],
        "partial": [],
        "jobs": [job],
    }
    monkeypatch.setattr(cli_module, "_session", lambda: (settings, object(), session))
    monkeypatch.setattr(cli_module, "sync_registry", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        cli_module,
        "_collect_bounded_until_blocked",
        lambda *_args, **_kwargs: collection,
    )
    monkeypatch.setattr(
        cli_module,
        "run_core_pipeline",
        lambda *_args, **_kwargs: {
            "analyzed": 2,
            "analysis_tasks": {
                "failed": 0,
                "pending": 0,
                "untracked": 0,
                "retry_ready": 0,
                "retry_deferred": 0,
            },
        },
    )
    monkeypatch.setattr(cli_module, "_json", lambda value: captured.update(value))

    with pytest.raises(typer.Exit) as exc:
        cli_module.refresh(
            name=None,
            target_date="2026-08-20",
            since=None,
            until=None,
            date_range=None,
            days=None,
            limit=100,
            topic=None,
            sync_market_data=False,
        )

    assert exc.value.exit_code == 1
    assert captured["status"] == "complete_with_warnings"
    assert captured["pipeline"]["analyzed"] == 2
    assert "required source collection was incomplete: guba" in captured["warnings"]
    assert session.closed


def test_bounded_collection_continues_healthy_source_while_another_is_deferred(
    monkeypatch, tmp_path
):
    deferred = {
        "source": "guba",
        "done": False,
        "terminal": False,
        "deferred_reason": "source cooldown until later",
        "next_retry_at": "2099-01-01T00:00:00+00:00",
    }
    xhs_pending = {
        "source": "xiaohongshu",
        "done": False,
        "terminal": False,
        "next_retry_at": None,
    }
    first = {
        "attempted_jobs": 1,
        "jobs": [deferred, xhs_pending],
    }
    second = {
        "attempted_jobs": 1,
        "jobs": [deferred, {**xhs_pending, "done": True}],
    }
    outcomes = iter((first, second))
    calls = 0
    concurrency_values = []

    def fake_backfill(*_args, **kwargs):
        nonlocal calls
        calls += 1
        concurrency_values.append(kwargs["source_concurrency"])
        return next(outcomes)

    monkeypatch.setattr(cli_module, "backfill_active_topics", fake_backfill)

    result = cli_module._collect_bounded_until_blocked(
        object(),
        source_names=["guba", "xiaohongshu"],
        since=datetime(2026, 7, 26, tzinfo=UTC),
        until=datetime(2026, 8, 25, tzinfo=UTC),
        settings=type("Settings", (), {"source_concurrency": 3})(),
        topic_slugs=set(),
        state_path=tmp_path / "state.json",
    )

    assert result is second
    assert calls == 2
    assert concurrency_values == [3, 3]


def test_refresh_enriches_common_crawl_before_llm(settings, tmp_path, monkeypatch):
    class Session:
        closed = False

        def close(self):
            self.closed = True

    session = Session()
    settings = replace(
        settings,
        enabled_sources=("guba", "common-crawl"),
        run_lock_file=tmp_path / "refresh.lock",
    )
    order = []
    captured = {}
    collection = {
        "completed": True,
        "state_file": str(tmp_path / "state.json"),
        "attempted_jobs": 1,
        "pending_jobs": 0,
        "terminal_jobs": [],
        "deferred_jobs": [],
        "degraded": [],
        "partial": [],
        "jobs": [
            {
                "source": "guba",
                "topic_slug": "gold",
                "done": True,
                "terminal": False,
                "items_collected": 2,
                "duplicates": 1,
                "topic_links_added": 2,
            }
        ],
    }
    monkeypatch.setattr(cli_module, "_session", lambda: (settings, object(), session))
    monkeypatch.setattr(cli_module, "sync_registry", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        cli_module,
        "_collect_bounded_until_blocked",
        lambda *_args, **_kwargs: collection,
    )
    monkeypatch.setattr(
        cli_module,
        "normalize_pending",
        lambda *_args, **_kwargs: order.append("normalize-before-archive") or 2,
    )
    monkeypatch.setattr(
        cli_module,
        "resolve_pending_entities",
        lambda *_args, **_kwargs: order.append("resolve-before-archive") or 2,
    )
    monkeypatch.setattr(
        cli_module,
        "enrich_common_crawl",
        lambda *_args, **_kwargs: (
            order.append("common-crawl") or {"source_degraded": False, "captures_inserted": 1}
        ),
    )
    monkeypatch.setattr(
        cli_module,
        "run_core_pipeline",
        lambda *_args, **_kwargs: (
            order.append("llm-pipeline")
            or {
                "analyzed": 2,
                "analysis_tasks": {
                    "failed": 0,
                    "pending": 0,
                    "untracked": 0,
                    "retry_ready": 0,
                    "retry_deferred": 0,
                },
            }
        ),
    )
    monkeypatch.setattr(cli_module, "_json", lambda value: captured.update(value))

    cli_module.refresh(
        name=None,
        target_date="2026-08-20",
        since=None,
        until=None,
        date_range=None,
        days=None,
        limit=100,
        topic=None,
        sync_market_data=False,
    )
    assert order == [
        "normalize-before-archive",
        "resolve-before-archive",
        "common-crawl",
        "llm-pipeline",
    ]
    assert captured["status"] == "complete"
    assert captured["archive"]["captures_inserted"] == 1
    assert session.closed


def test_refresh_collects_supplements_with_required_sources_without_blocking_llm(
    settings, tmp_path, monkeypatch
):
    class Session:
        closed = False

        def close(self):
            self.closed = True

    session = Session()
    settings = replace(
        settings,
        enabled_sources=("guba", "xiaohongshu", "wikimedia-pageviews"),
        run_lock_file=tmp_path / "refresh.lock",
        source_concurrency=3,
    )
    captured = {}
    collection_calls = []
    jobs = [
        {
            "source": "guba",
            "topic_slug": "gold",
            "done": True,
            "terminal": False,
            "items_collected": 2,
            "duplicates": 0,
            "topic_links_added": 2,
        },
        {
            "source": "xiaohongshu",
            "topic_slug": None,
            "done": False,
            "terminal": False,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "deferred_reason": "login is unavailable",
            "next_retry_at": "2099-01-01T00:00:00+00:00",
        },
        {
            "source": "wikimedia-pageviews",
            "topic_slug": "gold",
            "done": False,
            "terminal": False,
            "items_collected": 0,
            "duplicates": 0,
            "topic_links_added": 0,
            "deferred_reason": "source cooldown until later",
            "next_retry_at": "2099-01-01T00:00:00+00:00",
        },
    ]

    def fake_collection(*_args, **kwargs):
        collection_calls.append(kwargs)
        return {
            "completed": False,
            "state_file": str(tmp_path / "combined.json"),
            "attempted_jobs": 2,
            "pending_jobs": 2,
            "terminal_jobs": [],
            "deferred_jobs": [jobs[1], jobs[2]],
            "degraded": [],
            "partial": [],
            "jobs": jobs,
        }

    monkeypatch.setattr(cli_module, "_session", lambda: (settings, object(), session))
    monkeypatch.setattr(cli_module, "sync_registry", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(cli_module, "_collect_bounded_until_blocked", fake_collection)
    monkeypatch.setattr(
        cli_module,
        "run_core_pipeline",
        lambda *_args, **_kwargs: {
            "analyzed": 2,
            "analysis_tasks": {
                "failed": 0,
                "pending": 0,
                "untracked": 0,
                "retry_ready": 0,
                "retry_deferred": 0,
            },
        },
    )
    monkeypatch.setattr(cli_module, "_json", lambda value: captured.update(value))

    with pytest.raises(typer.Exit) as exc:
        cli_module.refresh(
            name=None,
            target_date="2026-08-20",
            since=None,
            until=None,
            date_range=None,
            days=None,
            limit=100,
            topic=None,
            sync_market_data=False,
        )

    assert exc.value.exit_code == 1
    assert collection_calls[0]["source_names"] == [
        "guba",
        "xiaohongshu",
        "wikimedia-pageviews",
    ]
    assert captured["collection"]["required"]["complete"] is True
    assert captured["collection"]["supplements"]["complete"] is False
    assert captured["status"] == "complete_with_warnings"
    assert "xiaohongshu collection was incomplete" in captured["warnings"]
    assert "wikimedia-pageviews collection was incomplete" in captured["warnings"]
    assert session.closed


def test_wikimedia_only_refresh_skips_content_pipeline(settings, tmp_path, monkeypatch):
    class Session:
        closed = False

        def close(self):
            self.closed = True

    session = Session()
    settings = replace(
        settings,
        enabled_sources=("wikimedia-pageviews",),
        run_lock_file=tmp_path / "refresh.lock",
    )
    captured = {}
    collection = {
        "completed": True,
        "state_file": str(tmp_path / "wikimedia.json"),
        "attempted_jobs": 10,
        "pending_jobs": 0,
        "terminal_jobs": [],
        "deferred_jobs": [],
        "degraded": [],
        "partial": [],
        "jobs": [
            {
                "source": "wikimedia-pageviews",
                "topic_slug": "gold",
                "done": True,
                "terminal": False,
                "items_collected": 1,
                "duplicates": 0,
                "topic_links_added": 1,
            }
        ],
    }
    monkeypatch.setattr(cli_module, "_session", lambda: (settings, object(), session))
    monkeypatch.setattr(cli_module, "sync_registry", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        cli_module,
        "_collect_bounded_until_blocked",
        lambda *_args, **_kwargs: collection,
    )
    monkeypatch.setattr(
        cli_module,
        "run_core_pipeline",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("Wikimedia timer must not run the content pipeline")
        ),
    )
    normalization = {}
    monkeypatch.setattr(
        cli_module,
        "normalize_pending",
        lambda *_args, **kwargs: normalization.update(kwargs) or 1,
    )
    monkeypatch.setattr(
        cli_module,
        "aggregate_trend_signals",
        lambda *_args, **_kwargs: 1,
    )
    monkeypatch.setattr(cli_module, "_json", lambda value: captured.update(value))

    cli_module.refresh(
        name="wikimedia-pageviews",
        target_date="2026-08-31",
        since=None,
        until=None,
        date_range=None,
        days=None,
        limit=100,
        topic=None,
        sync_market_data=False,
    )

    assert captured["status"] == "complete"
    assert captured["pipeline"]["mode"] == "wikimedia-only"
    assert captured["pipeline"]["normalized"] == 1
    assert captured["pipeline"]["trend_signals"] == 1
    assert normalization["source_names"] == {"wikimedia-pageviews"}
    assert session.closed


def test_live_source_status_does_not_expose_secret_values():
    settings = Settings()
    guba = source_config_status("guba", settings=settings)
    optional = source_config_status("common-crawl", settings=settings)

    assert guba["configured"]
    assert guba["transport"] == "built-in-public"
    assert guba["missing"] == []
    assert not optional["configured"]
    assert optional["missing"] == ["RETAIL_TIDE_HTTP_USER_AGENT"]
    assert "api_key" not in str(guba)
    assert "api_key" not in str(optional)


def test_public_status_distinguishes_required_content_from_supplements(monkeypatch):
    settings = Settings(
        enabled_sources=("guba", "xiaohongshu", "common-crawl"),
        http_user_agent="RetailTide/0.1 (contact@example.com)",
    )
    captured = {}
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "_json", lambda value: captured.update(value))

    cli_module.status()

    rows = {row["name"]: row for row in captured["sources"]}
    assert rows["guba"]["pipeline_role"] == "required-content"
    assert rows["guba"]["blocks_llm"] is True
    assert rows["xiaohongshu"]["pipeline_role"] == "supplement"
    assert rows["xiaohongshu"]["blocks_llm"] is False
    assert rows["common-crawl"]["pipeline_role"] == "supplement"
    assert rows["common-crawl"]["blocks_llm"] is False


def test_public_source_is_ready_only_after_user_agent_identity():
    status = source_config_status(
        "wikimedia-pageviews",
        settings=Settings(http_user_agent="RetailTide/0.1 (ops@example.com)"),
    )
    assert status["configured"] is True
    assert status["endpoint_configured"] is True
    assert status["missing"] == []


def test_demo_settings_are_isolated_from_live_database():
    settings = Settings(database_url="sqlite:///live.db")

    demo = settings.for_demo()

    assert demo.data_mode == "demo"
    assert demo.market_provider == "synthetic-a-share"
    assert demo.database_url.endswith("retail_tide-demo.db")


def test_live_settings_default_to_market_specific_public_data():
    assert Settings().market_provider == "public"
    assert Settings().database_url == "sqlite:///retail-tide.db"
    assert Settings().request_interval("guba") == 15.0


def test_live_settings_default_to_canonical_database(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("RETAIL_TIDE_ENV_FILE", raising=False)
    monkeypatch.delenv("RETAIL_TIDE_DATABASE_URL", raising=False)
    monkeypatch.delenv("RETAIL_TIDE_DATA_MODE", raising=False)

    assert Settings.from_env().database_url == "sqlite:///retail-tide.db"


def test_source_concurrency_defaults_to_five_and_is_bounded(monkeypatch, tmp_path):
    monkeypatch.setenv("RETAIL_TIDE_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.delenv("RETAIL_TIDE_SOURCE_CONCURRENCY", raising=False)

    assert Settings.from_env().source_concurrency == 5

    monkeypatch.setenv("RETAIL_TIDE_SOURCE_CONCURRENCY", "99")
    assert Settings.from_env().source_concurrency == 8

    monkeypatch.setenv("RETAIL_TIDE_SOURCE_CONCURRENCY", "0")
    assert Settings.from_env().source_concurrency == 1


def test_default_live_profile_only_requires_p0_sources():
    settings = Settings()

    assert settings.enabled_sources == ("guba", "taoguba")
    assert settings.collector_version == "collector-v2"
    assert source_config_status("guba", settings=settings)["required"] is True
    assert source_config_status("common-crawl", settings=settings)["required"] is False
    assert source_config_status("zhihu", settings=settings)["enabled"] is False
    assert DEFAULT_PUBLIC_SOURCES == ("wikimedia-pageviews",)


def test_zhihu_uses_built_in_endpoint_and_only_requires_access_secret():
    missing = source_config_status("zhihu", settings=Settings())
    configured = source_config_status(
        "zhihu",
        settings=Settings(
            enabled_sources=("guba", "taoguba", "zhihu"),
            source_credentials={
                "zhihu": SourceCredential(
                    "zhihu",
                    access_token="example-access-secret",
                )
            },
        ),
    )

    assert missing["transport"] == "built-in-official-api"
    assert missing["endpoint_source"] == "built-in"
    assert missing["missing"] == ["RETAIL_TIDE_ZHIHU_ACCESS_TOKEN or ZHIHU_ACCESS_SECRET"]
    assert configured["configured"] is True
    assert configured["endpoint_configured"] is False
    assert configured["endpoint_available"] is True
    assert configured["missing"] == []


def test_xiaohongshu_uses_project_owned_mcp_endpoint_without_copying_credentials():
    missing = source_config_status("xiaohongshu", settings=Settings())
    configured = source_config_status(
        "xiaohongshu",
        settings=Settings(
            enabled_sources=("guba", "taoguba", "xiaohongshu"),
            source_credentials={
                "xiaohongshu": SourceCredential("xiaohongshu", endpoint="http://127.0.0.1:18060")
            },
        ),
    )

    assert missing["missing"] == [
        "RETAIL_TIDE_XIAOHONGSHU_SPIDER_ENDPOINT or RETAIL_TIDE_XIAOHONGSHU_ENDPOINT"
    ]
    assert configured["configured"] is True
    assert configured["credential_configured"] is False
    assert configured["session_auth"] == "managed-by-collector-services"
    assert "cookie" not in str(configured).casefold()


def test_zhihu_setup_prompts_only_for_access_secret(monkeypatch):
    values = {"RETAIL_TIDE_ZHIHU_ENDPOINT": "https://legacy.example.test/search"}

    monkeypatch.setattr(cli_module.typer, "confirm", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        cli_module,
        "_secret_prompt",
        lambda _label, _current: "example-access-secret",
    )

    def unexpected_endpoint_prompt(*_args, **_kwargs):
        raise AssertionError("Zhihu setup must not prompt for an endpoint")

    monkeypatch.setattr(cli_module, "_text_prompt", unexpected_endpoint_prompt)

    assert cli_module._configure_source(values, "zhihu") is True
    assert values["RETAIL_TIDE_ZHIHU_ENDPOINT"] == ""
    assert values["RETAIL_TIDE_ZHIHU_ACCESS_TOKEN"] == "example-access-secret"


def test_zhihu_accepts_official_skill_secret_environment_name(monkeypatch, tmp_path):
    monkeypatch.setenv("RETAIL_TIDE_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("ZHIHU_ACCESS_SECRET", "example-access-secret")
    monkeypatch.delenv("RETAIL_TIDE_ZHIHU_ACCESS_TOKEN", raising=False)

    settings = Settings.from_env()

    assert settings.source_credential("zhihu").access_token == "example-access-secret"
    assert source_config_status("zhihu", settings=settings)["configured"] is True


def test_runtime_without_env_enables_built_in_p0_sources(monkeypatch, tmp_path):
    monkeypatch.setenv("RETAIL_TIDE_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.delenv("RETAIL_TIDE_ENABLED_SOURCES", raising=False)

    settings = Settings.from_env()

    assert settings.enabled_sources == ("guba", "taoguba")


def test_legacy_empty_source_list_migrates_to_public_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("RETAIL_TIDE_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("RETAIL_TIDE_ENABLED_SOURCES", "")

    settings = Settings.from_env()

    assert settings.enabled_sources == ("guba", "taoguba")


def test_explicit_none_can_disable_all_sources(monkeypatch, tmp_path):
    monkeypatch.setenv("RETAIL_TIDE_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("RETAIL_TIDE_ENABLED_SOURCES", "none")

    settings = Settings.from_env()

    assert settings.enabled_sources == ()


def test_external_llm_requires_endpoint_key_and_model_without_exposing_key():
    settings = Settings(
        llm_provider="openai",
        llm_base_url="https://api.openai.com/v1",
        analysis_model="gpt-5",
    )

    status = llm_config_status(settings)
    assert status["configured"] is False
    assert status["missing"] == ["RETAIL_TIDE_LLM_API_KEY"]
    assert "api_key" not in str(status)
    with pytest.raises(ValueError, match="LLM configuration is incomplete"):
        analysis_provider_for_settings(settings)


def test_external_llm_provider_is_constructed_only_after_configuration():
    provider = analysis_provider_for_settings(
        Settings(
            llm_provider="openai-compatible",
            llm_base_url="http://llm.test/v1",
            llm_api_key="secret",
            analysis_model="model-a",
            llm_timeout_seconds=91,
        )
    )

    assert isinstance(provider, OpenAICompatibleAnalysisProvider)
    assert provider.endpoint == "http://llm.test/v1/chat/completions"
    assert provider.model == "model-a"
    assert provider.timeout == 91


def test_dual_llm_configuration_is_generic_and_keeps_credentials_secret(monkeypatch, tmp_path):
    monkeypatch.setenv("RETAIL_TIDE_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("RETAIL_TIDE_LLM_PROVIDER", "openai-compatible")
    monkeypatch.setenv("RETAIL_TIDE_LLM_BASE_URL", "https://primary.test/v1")
    monkeypatch.setenv("RETAIL_TIDE_LLM_API_KEY", "primary-secret")
    monkeypatch.setenv("RETAIL_TIDE_ANALYSIS_MODEL", "primary-model")
    monkeypatch.setenv("RETAIL_TIDE_LLM_MIN_INTERVAL", "2")
    monkeypatch.setenv("RETAIL_TIDE_LLM_TIMEOUT_SECONDS", "80")
    monkeypatch.setenv("RETAIL_TIDE_LLM_FALLBACK_PROVIDER", "openai-compatible")
    monkeypatch.setenv("RETAIL_TIDE_LLM_FALLBACK_BASE_URL", "https://backup.test/v1")
    monkeypatch.setenv("RETAIL_TIDE_LLM_FALLBACK_API_KEY", "backup-secret")
    monkeypatch.setenv("RETAIL_TIDE_LLM_FALLBACK_MODEL", "backup-model")
    monkeypatch.setenv("RETAIL_TIDE_LLM_FALLBACK_MIN_INTERVAL", "4")
    monkeypatch.setenv("RETAIL_TIDE_LLM_FALLBACK_TIMEOUT_SECONDS", "95")

    settings = Settings.from_env()
    status = llm_config_status(settings)
    provider = analysis_provider_for_settings(settings)

    assert status["configured"] is True
    assert status["failover_enabled"] is True
    assert [item["model"] for item in status["providers"]] == [
        "primary-model",
        "backup-model",
    ]
    assert "primary-secret" not in str(status)
    assert "backup-secret" not in str(status)
    assert isinstance(provider, FailoverAnalysisProvider)
    assert provider.primary.endpoint == "https://primary.test/v1/chat/completions"
    assert provider.primary.min_interval == 2
    assert provider.fallback.endpoint == "https://backup.test/v1/chat/completions"
    assert provider.fallback.min_interval == 4
    assert provider.fallback.timeout == 95


def test_incomplete_fallback_does_not_disable_a_valid_primary():
    settings = Settings(
        llm_provider="openai-compatible",
        llm_base_url="https://primary.test/v1",
        llm_api_key="primary-secret",
        analysis_model="primary-model",
        llm_fallback_provider="openai-compatible",
        llm_fallback_base_url="https://backup.test/v1",
        llm_fallback_model="backup-model",
    )

    status = llm_config_status(settings)
    provider = analysis_provider_for_settings(settings)

    assert status["configured"] is True
    assert status["failover_enabled"] is False
    assert status["fallback_missing"] == ["RETAIL_TIDE_LLM_FALLBACK_API_KEY"]
    assert isinstance(provider, OpenAICompatibleAnalysisProvider)
