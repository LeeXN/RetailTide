import asyncio
import json
import subprocess
import sys
from datetime import datetime

import httpx
import pytest
import typer

from retail_tide import cli
from retail_tide.sources import xhs_control
from retail_tide.sources.xhs_control import XhsBlocked, XhsControl
from retail_tide.sources.xiaohongshu import SourceRequestError, XiaohongshuSource
from retail_tide.time import UTC


def test_pause_persists_and_resume_does_not_call_platform(tmp_path):
    control = XhsControl(tmp_path)
    control.failure("auth_required")
    with pytest.raises(XhsBlocked):
        XhsControl(tmp_path).check()
    asyncio.run(control.resume())
    assert control.check()["paused"] is False


def test_rate_limit_backoff_and_server_minimum(tmp_path, monkeypatch):
    monkeypatch.setattr(xhs_control.time, "time", lambda: 1000)
    control = XhsControl(tmp_path)
    for delay in (3600, 7200, 14400, 28800, 28800):
        control.failure("rate_limited")
        assert control.read()["retry_at"] == 1000 + delay
    control.failure("rate_limited", 40000)
    assert control.read()["retry_at"] == 41000
    control.completed(30, success=True)
    assert control.read()["failures"] == 0


@pytest.mark.asyncio
async def test_http_200_rate_limit_still_honors_provider_delay(tmp_path):
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, json={"success": False, "error_code": "rate_limited", "retry_after_seconds": 40000}
        )
    )
    control = XhsControl(tmp_path)
    source = XiaohongshuSource(
        endpoint="http://mcp.test",
        transport=transport,
        control=control,
        min_request_interval=0,
        request_jitter=0,
    )
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(SourceRequestError):
            await source._request(client, "POST", "/api/v1/feeds/search")
    assert control.read()["paused"] is False
    assert control.read()["retry_at"] - xhs_control.time.time() > 39990


@pytest.mark.asyncio
async def test_operation_lock_is_cross_process(tmp_path):
    control = XhsControl(tmp_path)
    script = """import fcntl, sys
with open(sys.argv[1], 'r') as handle:
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(75)
"""

    def probe():
        return subprocess.run(
            [sys.executable, "-c", script, str(tmp_path / "xiaohongshu-operation.lock")],
            check=False,
            timeout=5,
        ).returncode

    async with control.ownership():
        assert probe() == 75
    assert probe() == 0


@pytest.mark.asyncio
async def test_ownership_serializes_independent_instances_and_releases_on_cancel(tmp_path):
    first, second = XhsControl(tmp_path), XhsControl(tmp_path)
    entered = asyncio.Event()

    async def waiter():
        async with second.ownership():
            entered.set()

    async with first.ownership():
        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)
        assert not entered.is_set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    await asyncio.wait_for(waiter(), timeout=1)
    assert entered.is_set()


@pytest.mark.asyncio
async def test_pause_blocks_all_transports_without_a_second_request(tmp_path):
    calls = []

    def handler(request):
        calls.append(request.url.host)
        return httpx.Response(403, json={"message": "安全验证"})

    control = XhsControl(tmp_path)
    source = XiaohongshuSource(
        endpoint="http://mcp.test",
        use_fixture=False,
        transport=httpx.MockTransport(handler),
        control=control,
        min_request_interval=0,
        request_jitter=0,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(SourceRequestError):
            await source._request(client, "POST", "/api/v1/feeds/search")
        with pytest.raises(XhsBlocked):
            await source._request(
                client, "POST", "/api/v1/feeds/search", endpoint="http://spider.test"
            )
    assert calls == ["mcp.test"]
    assert control.read()["paused"] is True


@pytest.mark.asyncio
async def test_operation_cooldown_is_after_completion(tmp_path, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(xhs_control.time, "time", lambda: now[0])
    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)
        now[0] += delay

    monkeypatch.setattr(asyncio, "sleep", sleep)

    def handler(request):
        now[0] += 20
        if request.url.path.endswith("detail"):
            return httpx.Response(200, json={"data": {"note": {"id": "1"}}})
        return httpx.Response(200, json={"data": {"feeds": []}})

    control = XhsControl(tmp_path)
    transport = httpx.MockTransport(handler)
    source = XiaohongshuSource(
        endpoint="http://mcp.test",
        transport=transport,
        control=control,
        min_request_interval=0,
        request_jitter=0,
    )
    async with httpx.AsyncClient(transport=transport) as client:
        await source._request(client, "POST", "/api/v1/feeds/search")
        assert control.read()["next_request_at"] == 1080
        await source._request(client, "POST", "/api/v1/feeds/detail")
        assert sleeps == [60]
        assert control.read()["next_request_at"] == 1130


def test_xiaohongshu_schedule_pins_and_catches_up(tmp_path, monkeypatch):
    monkeypatch.setenv("RETAIL_TIDE_STATE_DIR", str(tmp_path))
    path = tmp_path / "schedule.json"
    monkeypatch.setenv("RETAIL_TIDE_SCHEDULED_XIAOHONGSHU_STATE_FILE", str(path))
    start = datetime(2026, 9, 12, 16, tzinfo=UTC)
    end = datetime(2026, 9, 13, 16, tzinfo=UTC)
    monkeypatch.setattr(cli, "scheduled_post_window", lambda: (start, end))
    monkeypatch.setattr(cli, "refresh", lambda **kwargs: (_ for _ in ()).throw(typer.Exit(1)))
    with pytest.raises(typer.Exit):
        cli.scheduled_xiaohongshu(limit=123)
    assert path.exists()
    calls = []
    monkeypatch.setattr(
        cli, "scheduled_post_window", lambda: (end, datetime(2026, 9, 14, 16, tzinfo=UTC))
    )
    monkeypatch.setattr(cli, "refresh", lambda **kwargs: calls.append(kwargs))
    cli.scheduled_xiaohongshu(limit=123)
    assert len(calls) == 2
    assert all(row["name"] == "xiaohongshu" and row["sync_market_data"] is False for row in calls)
    assert calls[0]["since"] == start.isoformat()
    assert not path.exists()


def test_paused_schedule_never_calls_refresh(tmp_path, monkeypatch):
    monkeypatch.setenv("RETAIL_TIDE_STATE_DIR", str(tmp_path))
    monkeypatch.setenv(
        "RETAIL_TIDE_SCHEDULED_XIAOHONGSHU_STATE_FILE", str(tmp_path / "schedule.json")
    )
    XhsControl(tmp_path).failure("auth_required")
    monkeypatch.setattr(cli, "refresh", lambda **kwargs: pytest.fail("must not collect"))
    with pytest.raises(typer.Exit) as error:
        cli.scheduled_xiaohongshu(limit=123)
    assert error.value.exit_code == 78


def test_split_schedule_preserves_original_jobs_and_is_idempotent(tmp_path, monkeypatch):
    from dataclasses import replace

    from retail_tide.config import Settings

    monkeypatch.setenv("RETAIL_TIDE_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("RETAIL_TIDE_SCHEDULED_STATE_FILE", str(tmp_path / "posts.json"))
    monkeypatch.setenv("RETAIL_TIDE_SCHEDULED_XIAOHONGSHU_STATE_FILE", str(tmp_path / "xhs.json"))
    monkeypatch.setattr(
        cli, "get_settings", lambda: replace(Settings(), run_lock_file=tmp_path / "refresh.lock")
    )
    (tmp_path / "refresh").mkdir()
    original = tmp_path / "refresh/original-content.json"
    state = {
        "version": 1,
        "since": "2026-09-12T16:00:00+00:00",
        "until": "2026-09-13T16:00:00+00:00",
        "jobs": {
            "xhs": {"source": "xiaohongshu", "done": False, "cursor": "page2", "pages": 1},
            "guba": {"source": "guba", "done": True, "pages": 3},
        },
    }
    original.write_text(json.dumps(state))
    cli.split_xiaohongshu_schedule()
    assert json.loads(original.read_text()) == state
    assert json.loads((tmp_path / "xhs.json").read_text())["since"] == state["since"]
    targets = list((tmp_path / "refresh").glob("*-content.json"))
    assert len(targets) == 3
    migrated = [json.loads(path.read_text()) for path in targets if path != original]
    assert any(row["jobs"] == {"xhs": state["jobs"]["xhs"]} for row in migrated)
    cli.split_xiaohongshu_schedule()
    assert len(list((tmp_path / "refresh").glob("*-content.json"))) == 3
