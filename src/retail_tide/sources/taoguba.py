from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import quote

import httpx

from ..config import SourceCredential
from ..schemas import CollectResult
from ..source_sessions import (
    SourceSessionError,
    source_session_cookie_header,
    source_session_request_headers,
)
from ..time import SHANGHAI, UTC, as_utc, now_utc
from .base import (
    ProbeResult,
    SourceError,
    decode_cursor,
    encode_cursor,
    html_to_text,
    parse_paged_response,
    public_get,
    raw_from_mapping,
)
from .fixture import FixtureSource

TAOGUBA_BASE_URL = "https://www.tgb.cn"
TAOGUBA_SEARCH_URL = f"{TAOGUBA_BASE_URL}/search/getSearchTopicResult"
PUBLIC_HEADERS = {
    "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
    "User-Agent": ("Mozilla/5.0 (compatible; RetailTide/0.1; +https://www.tgb.cn/)"),
}
_next_public_request_at = 0.0


def _query_fingerprint(query: str) -> str:
    return hashlib.sha256(query.strip().encode("utf-8")).hexdigest()[:16]


def _taoguba_datetime(value: Any) -> datetime:
    if value in (None, ""):
        raise SourceError("taoguba topic has no publication time")
    try:
        if isinstance(value, (int, float)) or str(value).strip().isdigit():
            timestamp = float(value)
            if timestamp > 100_000_000_000:
                timestamp /= 1000
            return datetime.fromtimestamp(timestamp, tz=UTC)
        parsed = datetime.fromisoformat(str(value).strip())
    except (ValueError, TypeError, OSError) as exc:
        raise SourceError(f"taoguba returned an invalid publication time: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    result = as_utc(parsed)
    assert result is not None
    return result


def _search_periods(since: datetime, until: datetime, *, current: datetime) -> list[int]:
    if since >= current - timedelta(days=183):
        return [6]
    first_year = until.astimezone(SHANGHAI).year
    last_year = since.astimezone(SHANGHAI).year
    return list(range(first_year, last_year - 1, -1))


def _mapping_from_taoguba_record(record: dict[str, Any]) -> dict[str, Any]:
    topic_id = record.get("topicID")
    if topic_id in (None, ""):
        raise SourceError("taoguba topic has no stable topic id")
    published_at = _taoguba_datetime(record.get("postDate"))
    updated_at = _taoguba_datetime(record.get("lastReplyDate") or record.get("postDate"))
    title = html_to_text(record.get("subject"))
    body = html_to_text(record.get("body")) or title
    public_id = str(record.get("newTopicID") or topic_id)
    mapping: dict[str, Any] = {
        "id": str(topic_id),
        "published_at": published_at.isoformat(),
        "updated_at": updated_at.isoformat(),
        "timestamp_semantics": "published",
        "source_timestamp_field": "postDate",
        "source_timezone": "Asia/Shanghai_or_unix_epoch",
        "title": title or None,
        "body": body,
        "url": f"{TAOGUBA_BASE_URL}/a/{quote(public_id, safe='')}",
        "author_id": str(record.get("userID")) if record.get("userID") not in (None, "") else None,
        "author": html_to_text(record.get("userName")) or None,
        "views": record.get("totalViewNum"),
        "comments": record.get("totalReplyNum"),
        "likes": record.get("usefulNum"),
        "favorites": record.get("favoriteNum"),
        "catalog": html_to_text(record.get("catalogName")) or None,
        "stock_code": str(record.get("stockCode"))
        if record.get("stockCode") not in (None, "")
        else None,
        "stock_name": html_to_text(record.get("stockName")) or None,
        "language": "zh-CN",
    }
    return {key: value for key, value in mapping.items() if value is not None}


class TaogubaSource(FixtureSource):
    """淘股吧 collector using the site's public keyword-search response by default."""

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        credential: SourceCredential | None = None,
        use_fixture: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
        min_public_interval: float | None = None,
        access_retry_delays: tuple[float, ...] | None = None,
        session_file: Path | None = None,
        **kwargs,
    ):
        super().__init__("taoguba", **kwargs)
        self.credential = credential or SourceCredential("taoguba", endpoint=endpoint)
        self.endpoint = endpoint or self.credential.endpoint
        self.use_fixture = use_fixture
        self.transport = transport
        self.min_public_interval = (
            15.0
            if min_public_interval is None and transport is None
            else float(min_public_interval or 0)
        )
        self.access_retry_delays = (
            (60.0,)
            if access_retry_delays is None and transport is None
            else tuple(access_retry_delays or ())
        )
        self.session_file = Path(session_file) if session_file is not None else None

    async def _public_search_payload(
        self,
        client: httpx.AsyncClient,
        *,
        params: dict[str, Any],
        headers: dict[str, str],
    ) -> dict[str, Any]:
        global _next_public_request_at

        attempts = len(self.access_retry_delays) + 1
        payload: dict[str, Any] | None = None
        for attempt in range(attempts):
            wait = max(0.0, _next_public_request_at - monotonic())
            if wait:
                await asyncio.sleep(wait)
            _next_public_request_at = monotonic() + self.min_public_interval
            response = await public_get(
                client,
                TAOGUBA_SEARCH_URL,
                rate_source=self.name,
                min_interval=self.min_public_interval,
                params=params,
                headers=headers,
            )
            response.raise_for_status()
            value = response.json()
            if not isinstance(value, dict):
                raise SourceError("taoguba search response has an unexpected shape")
            payload = value
            message = str(payload.get("errorMessage") or "")
            access_limited = payload.get("status") not in {True, "true"} and "登录" in message
            if not access_limited or attempt >= len(self.access_retry_delays):
                return payload
            await asyncio.sleep(self.access_retry_delays[attempt])
        assert payload is not None
        return payload

    async def collect(self, query, since, cursor=None, *, until=None):
        if not self.endpoint and self.use_fixture:
            return await super().collect(query, since, cursor, until=until)
        if self.endpoint:
            return await self._collect_custom(query, since, cursor, until=until)
        return await self._collect_public(query, since, cursor, until=until)

    async def _collect_custom(self, query, since, cursor=None, *, until=None):
        if not self.credential.has_auth:
            raise SourceError("taoguba custom endpoint requires an API key or access token")
        params = {"query": query, "since": since.isoformat()}
        if until:
            params["until"] = until.isoformat()
        if cursor:
            params["cursor"] = cursor
        try:
            async with httpx.AsyncClient(
                timeout=15,
                headers=self.credential.headers(),
                transport=self.transport,
            ) as client:
                response = await client.get(self.endpoint, params=params)
                response.raise_for_status()
                return parse_paged_response(
                    self.name,
                    response.json(),
                    observation_kind="forum_post",
                    observed_at=self.clock(),
                )
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            raise SourceError(f"taoguba custom collection failed: {exc}") from exc

    async def _collect_public(self, query, since, cursor=None, *, until=None) -> CollectResult:
        since = as_utc(since) or since
        until = as_utc(until) or now_utc()
        current = as_utc(self.clock()) or now_utc()
        if since >= until:
            raise SourceError("taoguba collection window must have since before until")

        if cursor:
            state = decode_cursor(self.name, cursor)
            if state.get("q") != _query_fingerprint(query):
                raise SourceError("taoguba pagination cursor belongs to another query")
            periods = state.get("periods")
            period_index = state.get("period")
            page = state.get("page")
            if (
                not isinstance(periods, list)
                or not all(isinstance(value, int) for value in periods)
                or not isinstance(period_index, int)
                or not isinstance(page, int)
                or period_index < 0
                or page < 1
            ):
                raise SourceError("invalid taoguba pagination cursor")
        else:
            periods = _search_periods(since, until, current=current)
            period_index = 0
            page = 1

        if period_index >= len(periods):
            return CollectResult(items=[], exhausted=True)
        period = periods[period_index]
        params = {
            "pageNo": page,
            "searchDate": period,
            "subject": query,
            "type": 1,
        }
        session_cookie = None
        session_headers: dict[str, str] = {}
        if self.session_file is not None:
            try:
                session_cookie = source_session_cookie_header("taoguba", self.session_file)
                session_headers = source_session_request_headers("taoguba", self.session_file)
            except SourceSessionError as exc:
                raise SourceError(
                    "taoguba saved browser session is invalid; login required again: "
                    f"{exc}"
                ) from exc
        client_headers = dict(PUBLIC_HEADERS)
        client_headers.update(session_headers)
        if session_cookie:
            client_headers["Cookie"] = session_cookie
        try:
            async with httpx.AsyncClient(
                timeout=20,
                follow_redirects=True,
                headers=client_headers,
                # public_get already retries transient transport/status errors.
                transport=self.transport,
            ) as client:
                payload = await self._public_search_payload(
                    client,
                    params=params,
                    headers={
                        "Referer": (
                            f"{TAOGUBA_BASE_URL}/search/search?"
                            f"searchContent={quote(query, safe='')}&type=0"
                        )
                    },
                )
        except (httpx.HTTPError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise SourceError(f"taoguba public collection failed: {exc}") from exc

        if not isinstance(payload, dict) or payload.get("status") not in {True, "true"}:
            message = payload.get("errorMessage") if isinstance(payload, dict) else None
            if session_cookie and "登录" in str(message or ""):
                raise SourceError(
                    "taoguba authenticated browser session was rejected; "
                    "login required again"
                )
            raise SourceError(f"taoguba search rejected the request: {message or 'unknown error'}")
        dto = payload.get("dto")
        records = dto.get("topicAttr") if isinstance(dto, dict) else None
        if not isinstance(records, list):
            raise SourceError("taoguba search response has an unexpected shape")
        records = [record for record in records if isinstance(record, dict)]
        published = [(record, _taoguba_datetime(record.get("postDate"))) for record in records]
        observed_at = as_utc(self.clock()) or now_utc()
        items = [
            raw_from_mapping(
                self.name,
                _mapping_from_taoguba_record(record),
                observation_kind="forum_post",
                observed_at=observed_at,
            )
            for record, timestamp in published
            if since <= timestamp < until
        ]

        try:
            total_pages = int(dto.get("totalPageNum") or 0)
        except (TypeError, ValueError):
            total_pages = 0
        reached_since = bool(
            published and min(timestamp for _record, timestamp in published) < since
        )
        if reached_since:
            period_index = len(periods)
        elif not records or page >= total_pages:
            period_index += 1
            page = 1
        else:
            page += 1
        exhausted = period_index >= len(periods)
        next_cursor = None
        if not exhausted:
            next_cursor = encode_cursor(
                self.name,
                {
                    "q": _query_fingerprint(query),
                    "periods": periods,
                    "period": period_index,
                    "page": page,
                },
            )
        return CollectResult(items=items, next_cursor=next_cursor, exhausted=exhausted)

    def _phrases(self):
        return [
            "短线选手看黄金，今天要不要上车，求老师带一下",
            "情绪太一致了，大家都在喊涨停，不能因为怕踏空就满仓",
            "我有底仓，按纪律等待确认，不追高也不传播未经证实的消息",
            "回撤后心态崩了，担心亏损扩大，先降低仓位再观察",
            "题材扩散很快，注意成交量和止损，以上只是个人记录",
            "群里突然都在讨论黄金，想知道是不是新的热点",
        ]

    def probe(self) -> ProbeResult:
        return ProbeResult(
            source=self.name,
            source_type=self.source_type,
            checks={
                "transport": "public-read-only",
                "keyword_search": True,
                "stable_topic_id": True,
                "publication_time": True,
                "publication_time_field": "postDate",
                "body": True,
                "interaction_counts": True,
                "cursor_pagination": True,
                "historical_year_filter": True,
                "time_window_filter": True,
                "api_key_required": False,
            },
            notes=["Uses Taoguba's public discussion-search response in newest-first order."],
        )
