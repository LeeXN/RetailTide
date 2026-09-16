"""Durable request ownership and recovery state; never contains session material."""

from __future__ import annotations

import asyncio
import fcntl
import json
import math
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from .base import SourceError

RUN_DEADLINE: float | None = None


class XhsBlocked(SourceError):
    def __init__(self, state: dict):
        self.error_code = state.get("reason", "control_state_invalid")
        self.retryable = not state.get("paused", False)
        self.retry_after_seconds = max(0, state.get("retry_at", 0) - time.time())
        self.transport_name = "source-control"
        super().__init__(f"xiaohongshu blocked: {self.error_code}")


class XhsControl:
    def __init__(self, root: Path | None = None):
        self.root = root or Path(os.getenv("RETAIL_TIDE_STATE_DIR", "var/state"))
        self.path = self.root / "xiaohongshu-control.json"

    def read(self) -> dict:
        try:
            state = json.loads(self.path.read_text())
            if not isinstance(state, dict):
                raise TypeError("invalid control state")
            for field in ("retry_at", "next_request_at", "failures"):
                value = state.get(field, 0)
                if not isinstance(value, (int, float)):
                    raise TypeError("invalid control timestamp")
                if not math.isfinite(value) or value < 0:
                    raise ValueError("invalid control timestamp")
            return state
        except FileNotFoundError:
            return {}
        except (OSError, ValueError, TypeError):
            return {"paused": True, "reason": "control_state_invalid"}

    def write(self, state: dict) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = self.path.with_suffix(".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump({"version": 1, **state}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(self.path)

    def check(self) -> dict:
        if RUN_DEADLINE is not None and time.monotonic() >= RUN_DEADLINE:
            raise XhsBlocked({"reason": "run_budget_exhausted"})
        state = self.read()
        if state.get("paused") or state.get("retry_at", 0) > time.time():
            raise XhsBlocked(state)
        return state

    @asynccontextmanager
    async def ownership(self):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.root / "xiaohongshu-operation.lock", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await asyncio.sleep(0.1)
            yield
        finally:
            os.close(fd)

    def failure(self, code: str, retry_after: float | None = None) -> None:
        state = self.read()
        count = int(state.get("failures", 0)) + 1 if state.get("reason") == code else 1
        paused = code not in {"rate_limited", "transport_error", "upstream_timeout"}
        base = 3600 if code == "rate_limited" else 900
        cap = 28800 if code == "rate_limited" else 3600
        if retry_after is not None and not math.isfinite(retry_after):
            retry_after = None
        delay = max(min(base * 2 ** min(count - 1, 5), cap), retry_after or 0)
        self.write(
            {
                **state,
                "paused": paused,
                "reason": code,
                "failures": count,
                "retry_at": 0 if paused else time.time() + delay,
            }
        )

    def completed(self, interval: float, *, success: bool = False) -> None:
        state = self.read()
        if success:
            state.update(paused=False, reason=None, failures=0, retry_at=0)
        state["next_request_at"] = time.time() + interval
        self.write(state)

    async def resume(self) -> None:
        async with self.ownership():
            state = self.read()
            state.update(paused=False, reason=None, failures=0, retry_at=0)
            self.write(state)
