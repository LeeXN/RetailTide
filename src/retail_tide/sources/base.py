from __future__ import annotations

import asyncio
import base64
import fcntl
import hashlib
import html
import json
import os
import re
import stat
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from threading import Lock
from typing import Any, ClassVar, Literal, Protocol

import httpx
from pydantic import BaseModel

from ..schemas import CollectResult, RawObservation
from ..time import as_utc, now_utc, parse_datetime


class SourceError(RuntimeError):
    """A source was reachable or attempted but could not produce valid observations."""


class RequestRateLimiter:
    """Cross-process serial limiter for public endpoints.

    A source adapter may still add a stricter, source-specific delay. Keeping
    the limiter here prevents two concurrent jobs from accidentally sharing a
    public endpoint without pacing. The durable reservation prevents a fresh
    CLI process or a timer from resetting the source interval.
    """

    def __init__(self) -> None:
        self._next_request_at: dict[str, float] = {}
        # ``collect_source`` deliberately uses a fresh event loop for every
        # topic.  asyncio.Lock instances are loop-bound once they have waiters,
        # so keeping one in this process-wide limiter breaks the second topic
        # after a page issues concurrent detail requests.  Reserve request
        # slots under a short thread lock instead; callers can then sleep on
        # whichever event loop owns the current job.
        self._guard = Lock()

    @staticmethod
    def _state_path(source: str) -> Path:
        normalized = re.sub(r"[^a-z0-9_.-]+", "-", source.casefold()).strip("-")
        root = Path(os.getenv("RETAIL_TIDE_STATE_DIR", "var/state")) / "rate-limits"
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        return root / f"{normalized or 'source'}.next-request"

    @classmethod
    def _reserve_durable_slot(
        cls, source: str, interval: float, *, after_completion: bool = False
    ) -> float:
        path = cls._state_path(source)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags, 0o600)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode):
                raise RuntimeError(f"rate-limit state is not a regular file: {path}")
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "r+", encoding="utf-8", closefd=False) as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                raw = handle.read().strip()
                try:
                    durable_next = float(raw) if raw else 0.0
                except ValueError:
                    durable_next = 0.0
                now = time.time()
                slot = now if after_completion else max(now, durable_next)
                next_request = max(durable_next, slot + interval)
                handle.seek(0)
                handle.truncate()
                handle.write(f"{next_request:.6f}\n")
                handle.flush()
                os.fsync(handle.fileno())
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                return max(0.0, slot - now)
        finally:
            os.close(descriptor)

    async def wait(self, source: str, interval: float) -> None:
        if interval <= 0:
            return
        now = time.monotonic()
        with self._guard:
            slot = max(now, self._next_request_at.get(source, now))
            self._next_request_at[source] = slot + interval
        # The lock is held only for a tiny local file update. Keeping it
        # synchronous also avoids leaving a default executor thread behind in
        # short-lived CLI processes.
        durable_delay = self._reserve_durable_slot(source, interval)
        delay = max(slot - now, durable_delay)
        if delay:
            await asyncio.sleep(delay)

    async def defer(self, source: str, interval: float) -> None:
        """Keep a full source interval after a slow browser operation finishes."""

        if interval <= 0:
            return
        with self._guard:
            self._next_request_at[source] = max(
                self._next_request_at.get(source, 0.0),
                time.monotonic() + interval,
            )
        self._reserve_durable_slot(
            source,
            interval,
            after_completion=True,
        )


PUBLIC_RATE_LIMITER = RequestRateLimiter()


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


async def public_get(
    client: httpx.AsyncClient,
    url: str,
    *,
    attempts: int = 3,
    rate_source: str | None = None,
    min_interval: float = 0.0,
    **kwargs: Any,
) -> httpx.Response:
    """GET a public source with bounded retries for transient transport failures."""

    transient_statuses = {429, 500, 502, 503, 504}
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            if rate_source:
                await PUBLIC_RATE_LIMITER.wait(rate_source, min_interval)
            response = await client.get(url, **kwargs)
            if response.status_code not in transient_statuses or attempt + 1 >= attempts:
                response.raise_for_status()
                return response
            delay = min(_retry_after_seconds(response.headers.get("Retry-After")) or 0, 60)
        except httpx.TransportError as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                raise
            delay = 0
        await asyncio.sleep(delay or max(5.0, 0.25 * (2**attempt)))
    if last_error:
        raise last_error
    raise SourceError("public source request exhausted its retry budget")


class _PlainTextParser(HTMLParser):
    _BLOCK_TAGS: ClassVar[set[str]] = {
        "article",
        "blockquote",
        "br",
        "div",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "li",
        "p",
        "section",
        "tr",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in {"script", "style"}:
            self._ignored_depth += 1
            return
        if self._ignored_depth:
            return
        if tag == "img":
            self.parts.append(" [图片] ")
        elif tag in self._BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style"} and self._ignored_depth:
            self._ignored_depth -= 1
            return
        if not self._ignored_depth and tag in self._BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._ignored_depth:
            self.parts.append(data)


def html_to_text(value: Any) -> str:
    """Convert source HTML to stable analysis text without external dependencies."""

    if value in (None, ""):
        return ""
    parser = _PlainTextParser()
    text = str(value)
    try:
        parser.feed(text)
        parser.close()
        text = "".join(parser.parts)
    except Exception:  # noqa: BLE001 - malformed source HTML still has useful text
        text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text).replace("\xa0", " ").replace("\u200b", "")
    text = text.replace("[淘股吧]", "")
    lines = [re.sub(r"[ \t\r\f\v]+", " ", line).strip() for line in text.splitlines()]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(line for line in lines if line)).strip()


def encode_cursor(source: str, state: dict[str, Any]) -> str:
    payload = json.dumps({"v": 1, **state}, ensure_ascii=False, separators=(",", ":")).encode()
    encoded = base64.urlsafe_b64encode(payload).decode().rstrip("=")
    return f"{source}.v1.{encoded}"


def decode_cursor(source: str, cursor: str) -> dict[str, Any]:
    prefix = f"{source}.v1."
    if not cursor.startswith(prefix):
        raise SourceError(f"invalid {source} pagination cursor")
    encoded = cursor[len(prefix) :]
    try:
        payload = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        state = json.loads(payload)
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise SourceError(f"invalid {source} pagination cursor") from exc
    if not isinstance(state, dict) or state.get("v") != 1:
        raise SourceError(f"invalid {source} pagination cursor")
    return state


class ObservationSource(Protocol):
    name: str
    source_type: Literal["content", "trend", "archive"]

    async def collect(
        self,
        query: str,
        since: datetime,
        cursor: str | None = None,
        *,
        until: datetime | None = None,
    ) -> CollectResult: ...


def payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _first(item: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in item and item[key] is not None:
            return item[key]
    return default


def raw_from_mapping(
    source: str,
    item: dict[str, Any],
    *,
    observation_kind: str = "forum_post",
    observed_at: datetime | None = None,
) -> RawObservation:
    """Convert a source item without applying any semantic classification."""
    source_item_id = _first(item, "source_item_id", "id", "post_id", "question_id", "answer_id")
    if source_item_id is None:
        raise SourceError("source item has no stable id")
    published_at = parse_datetime(_first(item, "published_at", "created_at", "time"))
    observed_at = as_utc(observed_at or now_utc())
    assert observed_at is not None
    # Keep the source payload intact, only removing no fields and adding a traceable kind.
    payload = dict(item)
    payload.setdefault("observation_kind", observation_kind)
    return RawObservation(
        source=source,
        source_item_id=str(source_item_id),
        observation_kind=str(_first(item, "observation_kind", default=observation_kind)),
        published_at=published_at,
        observed_at=observed_at,
        payload=payload,
    )


def parse_paged_response(
    source: str,
    response: Any,
    *,
    observation_kind: str,
    observed_at: datetime | None = None,
) -> CollectResult:
    """Parse common JSON page shapes and fail loudly on malformed responses."""
    if isinstance(response, list):
        records = response
        next_cursor = None
        exhausted = True
    elif isinstance(response, dict):
        records = _first(response, "items", "data", "results", default=[])
        if isinstance(records, dict):
            records = _first(records, "items", "results", default=[])
        next_cursor = _first(response, "next_cursor", "nextCursor", "cursor")
        if "exhausted" in response:
            exhausted = bool(response["exhausted"])
        elif "has_more" in response:
            exhausted = not bool(response["has_more"])
        else:
            exhausted = next_cursor is None
    else:
        raise SourceError("source response must be an object or list")
    if not isinstance(records, list):
        raise SourceError("source response items must be a list")
    items = [
        raw_from_mapping(source, item, observation_kind=observation_kind, observed_at=observed_at)
        for item in records
        if isinstance(item, dict)
    ]
    return CollectResult(
        items=items, next_cursor=str(next_cursor) if next_cursor else None, exhausted=exhausted
    )


class ProbeResult(BaseModel):
    source: str
    source_type: str
    source_role: str = "production"
    checks: dict[str, Any]
    notes: list[str] = []
