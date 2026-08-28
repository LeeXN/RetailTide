from __future__ import annotations

import random
import re
from datetime import datetime, timedelta
from time import monotonic
from typing import Any
from urllib.parse import quote

import httpx

from ..config import SourceCredential
from ..schemas import CollectResult, RawObservation
from ..time import SHANGHAI, UTC, as_utc, now_utc
from .base import (
    PUBLIC_RATE_LIMITER,
    ProbeResult,
    SourceError,
    decode_cursor,
    encode_cursor,
    html_to_text,
    raw_from_mapping,
)
from .fixture import FixtureSource

XIAOHONGSHU_WEB_URL = "https://www.xiaohongshu.com"
GENERIC_QUERY_TERMS = frozenset(
    {
        "etf",
        "买",
        "亏钱",
        "加仓",
        "回本",
        "基金",
        "定投",
        "投资",
        "抄底",
        "持仓",
        "散户",
        "概念股",
        "止盈",
        "清仓",
        "股价",
        "股市",
        "股票",
        "行情",
        "走势",
        "回调",
    }
)
XIAOHONGSHU_SORT_MODES = ("最新", "综合", "最多点赞", "最多评论", "最多收藏")


class CandidateUnavailable(SourceError):
    """A search hit that the source reports as deleted or no longer available."""


class SourceRequestError(SourceError):
    """A classified transport error safe to use for routing and cooldowns."""

    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        retryable: bool,
        retry_after_seconds: float | None = None,
        transport_name: str,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
        self.transport_name = transport_name


def xiaohongshu_strategy_cursor(sort_by: str) -> str:
    """Encode a resumable backfill strategy without pretending it is page cursored."""

    if sort_by not in XIAOHONGSHU_SORT_MODES:
        raise ValueError(f"unsupported xiaohongshu sort mode: {sort_by}")
    return encode_cursor(
        "xiaohongshu",
        {"kind": "search_strategy", "sort_by": sort_by},
    )


def xiaohongshu_spider_cursor(sort_by: str, backend_cursor: str) -> str:
    if sort_by not in XIAOHONGSHU_SORT_MODES or not backend_cursor:
        raise ValueError("invalid xiaohongshu spider cursor")
    return encode_cursor(
        "xiaohongshu",
        {
            "kind": "spider_page",
            "sort_by": sort_by,
            "backend_cursor": backend_cursor,
        },
    )


def _strategy_cursor_state(cursor: str | None) -> tuple[str, str | None, str]:
    if not cursor:
        return "最新", None, "initial"
    state = decode_cursor("xiaohongshu", cursor)
    sort_by = str(state.get("sort_by") or "")
    kind = str(state.get("kind") or "")
    if sort_by not in XIAOHONGSHU_SORT_MODES:
        raise SourceError("invalid xiaohongshu backfill strategy cursor")
    if kind == "search_strategy":
        return sort_by, None, kind
    backend_cursor = str(state.get("backend_cursor") or "").strip()
    if kind == "spider_page" and backend_cursor:
        return sort_by, backend_cursor, kind
    raise SourceError("invalid xiaohongshu backfill strategy cursor")


def _xiaohongshu_datetime(value: Any) -> datetime:
    try:
        timestamp = float(value)
        if timestamp > 100_000_000_000:
            timestamp /= 1000
        if timestamp <= 0:
            raise ValueError
        return datetime.fromtimestamp(timestamp, tz=UTC)
    except (TypeError, ValueError, OSError) as exc:
        raise SourceError(f"xiaohongshu note has an invalid publication time: {value!r}") from exc


def _social_count(value: Any) -> int | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace(",", "")
    multiplier = 1
    if text.endswith("万"):
        multiplier = 10_000
        text = text[:-1]
    elif text.endswith("千"):
        multiplier = 1_000
        text = text[:-1]
    try:
        return int(float(text) * multiplier)
    except (TypeError, ValueError):
        return None


def _response_data(payload: Any, *, operation: str) -> Any:
    if not isinstance(payload, dict):
        raise SourceError(f"xiaohongshu {operation} response has an unexpected shape")
    if payload.get("success") is False or payload.get("error"):
        code = html_to_text(payload.get("code")) or "unknown"
        message = html_to_text(payload.get("message") or payload.get("error")) or "unknown error"
        raise SourceError(f"xiaohongshu {operation} failed ({code}): {message}")
    return payload.get("data", payload)


def _feed_candidates(payload: Any, *, operation: str) -> list[dict[str, Any]]:
    data = _response_data(payload, operation=operation)
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        data = data["data"]
    records = data.get("feeds") if isinstance(data, dict) else None
    if not isinstance(records, list):
        raise SourceError(f"xiaohongshu {operation} response has no feed list")
    return [record for record in records if isinstance(record, dict)]


def _spider_search_page(
    payload: Any,
) -> tuple[list[dict[str, Any]], str | None, bool]:
    data = _response_data(payload, operation="spider search")
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        data = data["data"]
    if not isinstance(data, dict) or not isinstance(data.get("feeds"), list):
        raise SourceError("xiaohongshu spider search response has no feed list")
    candidates = [row for row in data["feeds"] if isinstance(row, dict)]
    next_cursor = str(data.get("next_cursor") or "").strip() or None
    has_more = bool(data.get("has_more"))
    if has_more and not next_cursor:
        raise SourceError("xiaohongshu spider search omitted a resumable cursor")
    return candidates, next_cursor, has_more


def _detail_note(payload: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    data = _response_data(payload, operation="detail")
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        data = data["data"]
    note = data.get("note") if isinstance(data, dict) else None
    comments = data.get("comments") if isinstance(data, dict) else None
    comment_rows = comments.get("list") if isinstance(comments, dict) else []
    if not isinstance(note, dict):
        raise SourceError("xiaohongshu detail response has no note")
    return note, [row for row in comment_rows if isinstance(row, dict)]


def _mapping_from_detail(
    note: dict[str, Any],
    comments: list[dict[str, Any]],
    *,
    query: str,
    sort_by: str,
    transport: str,
) -> dict[str, Any]:
    note_id = str(note.get("noteId") or note.get("id") or "").strip()
    if not note_id:
        raise SourceError("xiaohongshu note has no stable note id")
    published_at = _xiaohongshu_datetime(note.get("time"))
    title = html_to_text(note.get("title"))
    body = html_to_text(note.get("desc") or note.get("description")) or title
    if not body:
        raise SourceError(f"xiaohongshu note {note_id!r} has no title or body")
    user = note.get("user") if isinstance(note.get("user"), dict) else {}
    interaction = note.get("interactInfo") if isinstance(note.get("interactInfo"), dict) else {}
    sample_comments = [text for row in comments[:10] if (text := html_to_text(row.get("content")))]
    mapping: dict[str, Any] = {
        "id": note_id,
        "observation_kind": "xiaohongshu_note",
        "published_at": published_at.isoformat(),
        "updated_at": published_at.isoformat(),
        "timestamp_semantics": "published",
        "source_timestamp_field": "note.time",
        "source_timezone": "unix_epoch_utc",
        "title": title or None,
        "body": body,
        "url": f"{XIAOHONGSHU_WEB_URL}/explore/{quote(note_id, safe='')}",
        "author_id": str(user.get("userId") or user.get("id") or "").strip() or None,
        "author": html_to_text(user.get("nickname") or user.get("nickName")) or None,
        "likes": _social_count(interaction.get("likedCount")),
        "favorites": _social_count(interaction.get("collectedCount")),
        "comments": _social_count(interaction.get("commentCount")),
        "shares": _social_count(interaction.get("sharedCount")),
        "sample_comments": sample_comments or None,
        "query": query,
        "search_sort": sort_by,
        "source_role": "discovery",
        "sampled": True,
        "sampling_policy": "market-wide-fixed-query-even-page-v2",
        "collection_transport": transport,
        "language": "zh-CN",
    }
    return {key: value for key, value in mapping.items() if value is not None}


def _evenly_sample_candidates(
    candidates: list[dict[str, Any]], limit: int
) -> list[dict[str, Any]]:
    """Select stable positions across a ranked page instead of only its head."""

    if len(candidates) <= limit:
        return candidates
    if limit <= 1:
        return candidates[:1]
    indexes = [round(index * (len(candidates) - 1) / (limit - 1)) for index in range(limit)]
    return [candidates[index] for index in dict.fromkeys(indexes)]


def _query_terms(query: str) -> list[str]:
    terms = [term.casefold() for term in re.findall(r"[A-Za-z0-9\u4e00-\u9fff]+", query)]
    terms = [term.removesuffix("etf").removeprefix("买") or term for term in terms]
    specific = [term for term in terms if term not in GENERIC_QUERY_TERMS and len(term) >= 2]
    return specific or terms


def _candidate_matches_query(candidate: dict[str, Any], query: str) -> bool:
    note_card = candidate.get("noteCard") if isinstance(candidate.get("noteCard"), dict) else {}
    title = html_to_text(note_card.get("displayTitle") or candidate.get("title")).casefold()
    return bool(title and any(term in title for term in _query_terms(query)))


def _detail_matches_query(note: dict[str, Any], query: str) -> bool:
    text = " ".join(
        filter(
            None,
            (
                html_to_text(note.get("title")),
                html_to_text(note.get("desc") or note.get("description")),
            ),
        )
    ).casefold()
    return bool(text and any(term in text for term in _query_terms(query)))


def _publish_time_filter(
    since: datetime,
    until: datetime,
    *,
    current: datetime | None = None,
) -> str:
    """Choose the narrowest relative filter that still covers ``since``.

    Xiaohongshu's search filter is relative to the request time, not an exact
    historical range. Looking only at the requested span would make an old
    one-day backfill ask for "the last day" and then filter every result out.
    """

    current = as_utc(current) or now_utc()
    since = as_utc(since) or since
    until = as_utc(until) or until
    until = min(until, current)
    age = current - since
    if age <= timedelta(days=1):
        return "一天内"
    if age <= timedelta(days=7):
        return "一周内"
    if age <= timedelta(days=183):
        return "半年内"
    return "不限"


def _search_filters(
    sort_by: str,
    since: datetime,
    until: datetime,
    *,
    current: datetime | None = None,
) -> dict[str, str]:
    """Send only non-default filters to the browser-backed collector.

    The upstream API treats omitted values as ``不限``/``综合``. Explicitly
    clicking those defaults is both unnecessary and brittle when Xiaohongshu
    changes the filter-panel DOM, while omitting them preserves the same query
    semantics.
    """

    filters: dict[str, str] = {}
    if sort_by != "综合":
        filters["sort_by"] = sort_by
    publish_time = _publish_time_filter(since, until, current=current)
    if publish_time != "不限":
        filters["publish_time"] = publish_time
    return filters


def _newest_page_reached_since(
    publication_times: list[datetime],
    since: datetime,
) -> bool:
    """Conservatively detect when a newest-first page crossed the lower bound."""

    if not publication_times:
        return False
    older = sum(timestamp < since for timestamp in publication_times)
    if len(publication_times) < 5:
        return older == len(publication_times)
    return older / len(publication_times) >= 0.8


class XiaohongshuSource(FixtureSource):
    """Authorized, read-only adapter for a project-owned xiaohongshu-mcp service."""

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        credential: SourceCredential | None = None,
        spider_endpoint: str | None = None,
        spider_credential: SourceCredential | None = None,
        use_fixture: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
        spider_transport: httpx.AsyncBaseTransport | None = None,
        max_detail_requests: int = 5,
        spider_max_detail_requests: int = 5,
        known_source_item_ids: set[str] | frozenset[str] | None = None,
        known_source_published_at: dict[str, datetime] | None = None,
        min_request_interval: float | None = None,
        request_timeout: float = 50,
        total_budget: float = 300,
        request_jitter: float | None = None,
        **kwargs: Any,
    ):
        super().__init__("xiaohongshu", **kwargs)
        self.credential = credential or SourceCredential("xiaohongshu", endpoint=endpoint)
        configured_endpoint = endpoint or self.credential.endpoint
        self.endpoint = (configured_endpoint or "").rstrip("/").removesuffix("/mcp")
        self.spider_credential = spider_credential or SourceCredential(
            "xiaohongshu-spider", endpoint=spider_endpoint
        )
        configured_spider_endpoint = spider_endpoint or self.spider_credential.endpoint
        self.spider_endpoint = (configured_spider_endpoint or "").rstrip("/")
        self.use_fixture = use_fixture
        self.transport = transport
        self.spider_transport = spider_transport or transport
        self.max_detail_requests = max(1, max_detail_requests)
        self.spider_max_detail_requests = max(1, spider_max_detail_requests)
        self.known_source_item_ids = frozenset(known_source_item_ids or ())
        self.known_source_published_at = {
            str(item_id): timestamp
            for item_id, timestamp in (known_source_published_at or {}).items()
            if timestamp is not None
        }
        self.min_request_interval = (
            15.0
            if min_request_interval is None and transport is None
            else float(min_request_interval or 0)
        )
        self.request_jitter = (
            10.0
            if request_jitter is None and transport is None and spider_transport is None
            else max(0.0, float(request_jitter or 0))
        )
        self.request_timeout = max(1.0, request_timeout)
        self.total_budget = max(self.request_timeout, total_budget)

    async def _pace(self) -> float:
        interval = self.min_request_interval + (
            random.uniform(0, self.request_jitter) if self.request_jitter else 0.0
        )
        await PUBLIC_RATE_LIMITER.wait(self.name, interval)
        return interval

    async def _request(
        self,
        client: httpx.AsyncClient,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        endpoint: str | None = None,
        transport_name: str = "MCP",
    ) -> Any:
        interval = await self._pace()
        try:
            response = await client.request(
                method,
                f"{endpoint or self.endpoint}{path}",
                json=json_body,
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            try:
                payload = exc.response.json()
            except ValueError:
                payload = {}
            message = html_to_text(
                payload.get("message") or payload.get("error") or payload.get("detail")
                if isinstance(payload, dict)
                else ""
            )
            details = html_to_text(payload.get("details")) if isinstance(payload, dict) else ""
            if details and details != message:
                message = f"{message}: {details}" if message else details
            error_code = str(payload.get("error_code") or "").strip()
            retryable_value = payload.get("retryable")
            retry_after_value = payload.get("retry_after_seconds")
            try:
                retry_after = (
                    float(retry_after_value) if retry_after_value not in (None, "") else None
                )
            except (TypeError, ValueError):
                retry_after = None
            unavailable_markers = ("笔记不存在", "已删除", "note not found", "not exist")
            if code in {404, 410} or any(
                marker in message.casefold() for marker in unavailable_markers
            ):
                raise CandidateUnavailable("xiaohongshu candidate is unavailable") from exc
            if not error_code:
                if code == 401:
                    error_code = "auth_required"
                elif code == 403:
                    error_code = "upstream_rejected"
                elif code == 429:
                    error_code = "rate_limited"
                elif code == 504:
                    error_code = "upstream_timeout"
                else:
                    error_code = "transport_error"
            suffix = f": {message}" if message else ""
            raise SourceRequestError(
                f"xiaohongshu {transport_name} returned HTTP {code}{suffix}",
                error_code=error_code,
                retryable=(
                    bool(retryable_value)
                    if retryable_value is not None
                    else code in {408, 429, 500, 502, 503, 504}
                ),
                retry_after_seconds=retry_after,
                transport_name=transport_name,
            ) from exc
        except (httpx.TransportError, ValueError, TypeError) as exc:
            raise SourceRequestError(
                f"xiaohongshu {transport_name} request failed: {exc}",
                error_code="transport_error",
                retryable=True,
                transport_name=transport_name,
            ) from exc
        finally:
            # Leave a full cooldown after the browser operation completes. A
            # slow 60-second search must not make the next request immediate.
            await PUBLIC_RATE_LIMITER.defer(self.name, interval)

    async def collect(
        self,
        query: str,
        since: datetime,
        cursor: str | None = None,
        *,
        until: datetime | None = None,
    ) -> CollectResult:
        if self.use_fixture:
            return await super().collect(query, since, cursor, until=until)
        if not self.spider_endpoint:
            return await self._collect_mcp(query, since, cursor, until=until)
        if self.endpoint and self.transport is None:
            await self._require_mcp_login()
        try:
            return await self._collect_spider(query, since, cursor, until=until)
        except SourceError as exc:
            sort_by, backend_cursor, _ = _strategy_cursor_state(cursor)
            current = as_utc(self.clock()) or now_utc()
            effective_until = as_utc(until) or current
            normalized_since = as_utc(since) or since
            recent_single_day = (
                current - normalized_since <= timedelta(days=7)
                and effective_until - normalized_since <= timedelta(days=1)
            )
            spider_auth_unavailable = (
                isinstance(exc, SourceRequestError)
                and exc.error_code == "auth_required"
                and not exc.retryable
            )
            blocked_without_fallback = (
                isinstance(exc, SourceRequestError)
                and exc.error_code in {"rate_limited", "upstream_rejected"}
                and not exc.retryable
            )
            if not self.endpoint or blocked_without_fallback:
                raise
            if spider_auth_unavailable:
                if not recent_single_day:
                    raise
                # The MCP service is the account-login authority. Its session
                # may be healthy while the optional Spider bridge still has an
                # expired read-only mount. For a recent daily sample, restart
                # from MCP's bounded first page instead of touching or copying
                # either service's session material.
                fallback_cursor = xiaohongshu_strategy_cursor(sort_by)
                fallback_reason = "spider_auth_required"
                fallback_warning = (
                    f"spider session unavailable ({exc}); used logged-in MCP "
                    "first-page fallback"
                )
            else:
                if backend_cursor or not recent_single_day:
                    raise
                fallback_cursor = cursor
                fallback_reason = "spider_pagination_unavailable"
                fallback_warning = (
                    f"spider pagination unavailable ({exc}); used MCP first-page fallback"
                )
            fallback = await self._collect_mcp(
                query,
                since,
                fallback_cursor,
                until=until,
            )
            return fallback.model_copy(
                update={
                    "warnings": [
                        fallback_warning,
                        *fallback.warnings,
                    ],
                    "diagnostics": {
                        **fallback.diagnostics,
                        "search_transport": "mcp",
                        "fallback_searches": 1,
                        "fallback_reason": fallback_reason,
                        "discarded_spider_cursor": bool(
                            spider_auth_unavailable and backend_cursor
                        ),
                        "historical_pagination_complete": False,
                    },
                }
            )

    async def _require_mcp_login(self) -> None:
        """Treat the user-managed MCP status as the account-level authority."""

        headers = self.credential.headers()
        timeout = httpx.Timeout(min(30.0, self.request_timeout))
        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
                headers=headers,
            ) as client:
                response = await client.get(f"{self.endpoint}/api/v1/login/status")
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            raise SourceRequestError(
                f"xiaohongshu MCP login status check failed: {exc}",
                error_code="transport_error",
                retryable=True,
                transport_name="MCP",
            ) from exc
        data = payload.get("data", payload) if isinstance(payload, dict) else {}
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            data = data["data"]
        logged_in = (
            data.get("is_logged_in")
            if isinstance(data, dict) and "is_logged_in" in data
            else data.get("isLoggedIn") if isinstance(data, dict) else None
        )
        if logged_in is not True:
            raise SourceRequestError(
                "xiaohongshu MCP login is unavailable; user login is required",
                error_code="auth_required",
                retryable=False,
                transport_name="MCP",
            )

    async def _collect_spider(
        self,
        query: str,
        since: datetime,
        cursor: str | None = None,
        *,
        until: datetime | None = None,
    ) -> CollectResult:
        sort_by, backend_cursor, _ = _strategy_cursor_state(cursor)
        since = as_utc(since) or since
        until = as_utc(until) or now_utc()
        if since >= until:
            raise SourceError("xiaohongshu collection window must have since before until")

        headers = self.spider_credential.headers()
        headers["Content-Type"] = "application/json"
        timeout = httpx.Timeout(self.request_timeout)
        started = monotonic()
        warnings: list[str] = []
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=headers,
            transport=self.spider_transport,
        ) as spider_client:
            payload = await self._request(
                spider_client,
                "POST",
                "/api/v1/feeds/search",
                json_body={
                    "keyword": query,
                    "cursor": backend_cursor,
                    "filters": _search_filters(
                        sort_by,
                        since,
                        until,
                        current=as_utc(self.clock()) or now_utc(),
                    ),
                },
                endpoint=self.spider_endpoint,
                transport_name="spider bridge",
            )
            candidates, next_backend_cursor, has_more = _spider_search_page(payload)

            eligible: list[dict[str, Any]] = []
            seen_ids: set[str] = set()
            page_publication_times: list[datetime] = []
            for candidate in candidates:
                note_id = str(candidate.get("id") or "").strip()
                token = str(candidate.get("xsecToken") or "").strip()
                if (
                    not note_id
                    or not token
                    or note_id in seen_ids
                ):
                    continue
                seen_ids.add(note_id)
                if note_id in self.known_source_item_ids:
                    known_published_at = as_utc(self.known_source_published_at.get(note_id))
                    if known_published_at is not None:
                        page_publication_times.append(known_published_at)
                    continue
                eligible.append(candidate)
            selected = _evenly_sample_candidates(eligible, self.spider_max_detail_requests)

            items: list[RawObservation] = []
            detail_errors = 0
            detail_attempts = 0
            detail_successes = 0
            candidate_unavailable = 0
            off_topic = 0
            outside_window = 0
            fallback_details = 0
            budget_exhausted = False
            backup_candidates: list[dict[str, Any]] = []
            detail_error_codes: dict[str, int] = {}

            def record_detail_error(code: str) -> None:
                detail_error_codes[code] = detail_error_codes.get(code, 0) + 1

            def retain_detail(detail_payload: Any, *, transport_name: str) -> None:
                nonlocal detail_successes, off_topic, outside_window
                note, comments = _detail_note(detail_payload)
                published_at = _xiaohongshu_datetime(note.get("time"))
                page_publication_times.append(published_at)
                detail_successes += 1
                if not _detail_matches_query(note, query):
                    off_topic += 1
                    return
                if not since <= published_at < until:
                    outside_window += 1
                    return
                mapping = _mapping_from_detail(
                    note,
                    comments,
                    query=query,
                    sort_by=sort_by,
                    transport=transport_name,
                )
                items.append(
                    raw_from_mapping(
                        self.name,
                        mapping,
                        observation_kind="xiaohongshu_note",
                        observed_at=as_utc(self.clock()) or now_utc(),
                    )
                )

            for candidate in selected:
                if monotonic() - started >= self.total_budget:
                    budget_exhausted = True
                    break
                detail_body = {
                    "feed_id": str(candidate["id"]),
                    "xsec_token": str(candidate["xsecToken"]),
                    "xsec_source": str(candidate.get("xsecSource") or "pc_search"),
                    "load_all_comments": False,
                }
                detail_attempts += 1
                try:
                    detail_payload = await self._request(
                        spider_client,
                        "POST",
                        "/api/v1/feeds/detail",
                        json_body=detail_body,
                        endpoint=self.spider_endpoint,
                        transport_name="spider bridge",
                    )
                    retain_detail(detail_payload, transport_name="spider")
                except CandidateUnavailable:
                    candidate_unavailable += 1
                except SourceRequestError as exc:
                    if exc.error_code in {"auth_required", "rate_limited"} or not exc.retryable:
                        raise
                    record_detail_error(f"spider:{exc.error_code}")
                    backup_candidates.append(candidate)
                except SourceError:
                    record_detail_error("spider:source_error")
                    backup_candidates.append(candidate)

            # Capability-aware backup: finish the primary batch first, then let
            # MCP compensate each transport-local detail failure at most once.
            # Account/rate-limit failures above deliberately never fail over.
            if backup_candidates and self.endpoint and not budget_exhausted:
                mcp_headers = self.credential.headers()
                mcp_headers["Content-Type"] = "application/json"
                async with httpx.AsyncClient(
                    timeout=timeout,
                    follow_redirects=True,
                    headers=mcp_headers,
                    transport=self.transport,
                ) as mcp_client:
                    for candidate in backup_candidates:
                        if monotonic() - started >= self.total_budget:
                            budget_exhausted = True
                            break
                        detail_attempts += 1
                        try:
                            detail_payload = await self._request(
                                mcp_client,
                                "POST",
                                "/api/v1/feeds/detail",
                                json_body={
                                    "feed_id": str(candidate["id"]),
                                    "xsec_token": str(candidate["xsecToken"]),
                                    "xsec_source": str(
                                        candidate.get("xsecSource") or "pc_search"
                                    ),
                                    "load_all_comments": False,
                                },
                            )
                            fallback_details += 1
                            retain_detail(detail_payload, transport_name="mcp")
                        except CandidateUnavailable:
                            candidate_unavailable += 1
                        except SourceRequestError as exc:
                            if exc.error_code in {"auth_required", "rate_limited"} or not exc.retryable:
                                raise
                            record_detail_error(f"mcp:{exc.error_code}")
                            detail_errors += 1
                        except SourceError:
                            record_detail_error("mcp:source_error")
                            detail_errors += 1
            else:
                detail_errors += len(backup_candidates)

        if detail_errors:
            warnings.append(f"{detail_errors} candidate detail request(s) failed")
        if budget_exhausted:
            warnings.append("detail enrichment stopped at the configured total time budget")
        reached_since = sort_by == "最新" and _newest_page_reached_since(
            page_publication_times,
            since,
        )
        next_cursor = (
            xiaohongshu_spider_cursor(sort_by, next_backend_cursor)
            if has_more and not reached_since and next_backend_cursor
            else None
        )
        return CollectResult(
            items=items,
            next_cursor=next_cursor,
            exhausted=not has_more or reached_since,
            warnings=warnings,
            partial=bool(warnings),
            diagnostics={
                "sampling_policy": "market-wide-fixed-query-even-page-v2",
                "search_transport": "spider",
                "search_pages": 1,
                "candidates": len(candidates),
                "eligible_candidates": len(eligible),
                "selected_candidates": len(selected),
                "detail_attempts": detail_attempts,
                "detail_successes": detail_successes,
                "detail_failures": detail_errors,
                "detail_error_codes": detail_error_codes,
                "fallback_details": fallback_details,
                "candidate_unavailable": candidate_unavailable,
                "off_topic": off_topic,
                "outside_window": outside_window,
                "budget_exhausted": budget_exhausted,
                "reached_window_start": reached_since,
                "verified_publication_days": {
                    day: sum(
                        timestamp.astimezone(SHANGHAI).date().isoformat() == day
                        for timestamp in page_publication_times
                    )
                    for day in sorted(
                        {
                            timestamp.astimezone(SHANGHAI).date().isoformat()
                            for timestamp in page_publication_times
                        }
                    )
                },
                "retained_publication_days": {
                    day: sum(
                        item.published_at is not None
                        and item.published_at.astimezone(SHANGHAI).date().isoformat() == day
                        for item in items
                    )
                    for day in sorted(
                        {
                            item.published_at.astimezone(SHANGHAI).date().isoformat()
                            for item in items
                            if item.published_at is not None
                        }
                    )
                },
            },
        )

    async def _collect_mcp(
        self,
        query: str,
        since: datetime,
        cursor: str | None = None,
        *,
        until: datetime | None = None,
    ) -> CollectResult:
        if not self.endpoint:
            raise SourceError("xiaohongshu MCP endpoint is not configured")
        sort_by, backend_cursor, _ = _strategy_cursor_state(cursor)
        if backend_cursor:
            raise SourceError("xiaohongshu MCP cannot resume a spider pagination cursor")
        since = as_utc(since) or since
        until = as_utc(until) or now_utc()
        if since >= until:
            raise SourceError("xiaohongshu collection window must have since before until")

        warnings: list[str] = []
        started = monotonic()
        headers = self.credential.headers()
        headers["Content-Type"] = "application/json"
        timeout = httpx.Timeout(self.request_timeout)
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=headers,
            transport=self.transport,
        ) as client:
            filtered_body = {
                "keyword": query,
                "filters": _search_filters(
                    sort_by,
                    since,
                    until,
                    current=as_utc(self.clock()) or now_utc(),
                ),
            }
            candidates: list[dict[str, Any]] | None = None
            search_errors: list[str] = []
            used_unfiltered_search = False
            for attempt, body in enumerate((filtered_body, {"keyword": query})):
                try:
                    payload = await self._request(
                        client, "POST", "/api/v1/feeds/search", json_body=body
                    )
                    candidates = _feed_candidates(payload, operation="search")
                    used_unfiltered_search = attempt == 1
                    break
                except SourceRequestError as exc:
                    search_errors.append(str(exc))
                    if exc.error_code in {"auth_required", "rate_limited"} or not exc.retryable:
                        raise
                except SourceError as exc:
                    search_errors.append(str(exc))
            if candidates is None:
                combined = "; ".join(search_errors)
                raise SourceError(
                    f"xiaohongshu filtered and unfiltered search failed: {combined}"
                )
            if used_unfiltered_search:
                warnings.append(
                    f"filtered keyword search failed after {len(search_errors)} attempts "
                    f"({search_errors[-1]}); used one "
                    "unfiltered keyword search and validated topic and publication time "
                    "from note details locally"
                )

            candidates = candidates or []
            seen_ids: set[str] = set()
            eligible: list[dict[str, Any]] = []
            for candidate in candidates:
                note_id = str(candidate.get("id") or "").strip()
                token = str(candidate.get("xsecToken") or "").strip()
                if (
                    not note_id
                    or not token
                    or note_id in seen_ids
                    or note_id in self.known_source_item_ids
                ):
                    continue
                seen_ids.add(note_id)
                eligible.append(candidate)
            selected = _evenly_sample_candidates(eligible, self.max_detail_requests)

            items: list[RawObservation] = []
            detail_errors = 0
            detail_successes = 0
            off_topic = 0
            outside_window = 0
            candidate_unavailable = 0
            budget_exhausted = False
            for candidate in selected:
                if monotonic() - started >= self.total_budget:
                    budget_exhausted = True
                    break
                try:
                    detail_payload = await self._request(
                        client,
                        "POST",
                        "/api/v1/feeds/detail",
                        json_body={
                            "feed_id": str(candidate["id"]),
                            "xsec_token": str(candidate["xsecToken"]),
                            "xsec_source": str(candidate.get("xsecSource") or "pc_search"),
                            "load_all_comments": False,
                        },
                    )
                    note, comments = _detail_note(detail_payload)
                    detail_successes += 1
                    if not _detail_matches_query(note, query):
                        off_topic += 1
                        continue
                    published_at = _xiaohongshu_datetime(note.get("time"))
                    mapping = _mapping_from_detail(
                        note,
                        comments,
                        query=query,
                        sort_by=sort_by,
                        transport="mcp",
                    )
                    if not since <= published_at < until:
                        outside_window += 1
                        continue
                    items.append(
                        raw_from_mapping(
                            self.name,
                            mapping,
                            observation_kind="xiaohongshu_note",
                            observed_at=as_utc(self.clock()) or now_utc(),
                        )
                    )
                except CandidateUnavailable:
                    candidate_unavailable += 1
                except SourceRequestError as exc:
                    if exc.error_code in {"auth_required", "rate_limited"} or not exc.retryable:
                        raise
                    detail_errors += 1
                except SourceError:
                    detail_errors += 1
            if detail_errors:
                warnings.append(f"{detail_errors} candidate detail request(s) failed")
            if budget_exhausted:
                warnings.append("detail enrichment stopped at the configured total time budget")
            return CollectResult(
                items=items,
                exhausted=True,
                warnings=warnings,
                partial=bool(warnings),
                diagnostics={
                    "sampling_policy": "market-wide-fixed-query-even-page-v2",
                    "search_transport": "mcp",
                    "search_pages": 1,
                    "candidates": len(candidates),
                    "eligible_candidates": len(eligible),
                    "selected_candidates": len(selected),
                    "detail_attempts": min(len(selected), detail_successes + detail_errors + candidate_unavailable),
                    "detail_successes": detail_successes,
                    "detail_failures": detail_errors,
                    "fallback_details": 0,
                    "candidate_unavailable": candidate_unavailable,
                    "off_topic": off_topic,
                    "outside_window": outside_window,
                    "budget_exhausted": budget_exhausted,
                    "reached_window_start": False,
                    "historical_pagination_complete": False,
                    "verified_publication_days": {
                        day: sum(
                            item.published_at is not None
                            and item.published_at.astimezone(SHANGHAI).date().isoformat() == day
                            for item in items
                        )
                        for day in sorted(
                            {
                                item.published_at.astimezone(SHANGHAI).date().isoformat()
                                for item in items
                                if item.published_at is not None
                            }
                        )
                    },
                    "retained_publication_days": {
                        day: sum(
                            item.published_at is not None
                            and item.published_at.astimezone(SHANGHAI).date().isoformat() == day
                            for item in items
                        )
                        for day in sorted(
                            {
                                item.published_at.astimezone(SHANGHAI).date().isoformat()
                                for item in items
                                if item.published_at is not None
                            }
                        )
                    },
                },
            )

    def probe(self) -> ProbeResult:
        return ProbeResult(
            source=self.name,
            source_type=self.source_type,
            source_role="discovery",
            checks={
                "transport": "paginated-spider-primary-with-mcp-fallback",
                "login_managed_by_collector_services": True,
                "keyword_search": True,
                "feed_fallback": False,
                "detail_enrichment": True,
                "stable_note_id": True,
                "publication_time": "detail note.time",
                "interaction_counts": True,
                "comments": "disabled by default; post fields and interaction counts only",
                "cursor_pagination": bool(self.spider_endpoint),
                "backfill_strategy_cursor": list(XIAOHONGSHU_SORT_MODES),
                "candidate_dedup_before_detail": True,
                "write_operations": False,
            },
            notes=[
                "Cookies remain inside project-owned collector services; RetailTide only calls read-only endpoints.",
                "Search-list timestamps are not trusted; only successfully detailed notes enter metrics.",
            ],
        )
