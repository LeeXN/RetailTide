from __future__ import annotations

import asyncio
import hashlib
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import quote

import httpx
from curl_cffi.requests import AsyncSession as BrowserProfileSession
from curl_cffi.requests import RequestsError as BrowserProfileRequestError

from ..config import SourceCredential
from ..schemas import CollectResult
from ..source_sessions import (
    SourceSessionError,
    source_session_cookie_header,
    source_session_request_headers,
)
from ..time import SHANGHAI, as_utc, now_utc
from .base import (
    PUBLIC_RATE_LIMITER,
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

GUBA_BASE_URL = "https://guba.eastmoney.com"
GUBA_SUGGEST_URL = "https://searchadapter.eastmoney.com/api/suggest/get"
GUBA_LIST_API_URL = (
    "https://gbapi.eastmoney.com/webarticlelist/api/Article/WebArticleList"
)
GUBA_HOT_LIST_API_URL = (
    "https://gbapi.eastmoney.com/webarticlelist/api/Hot/Articlelist"
)
PUBLIC_HEADERS = {
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "User-Agent": ("Mozilla/5.0 (compatible; RetailTide/0.1; +https://guba.eastmoney.com/)"),
}
_next_public_list_request_at = 0.0


async def _browser_profile_get(
    url: str,
    *,
    headers: dict[str, str],
    timeout: float,
) -> str:
    """Fetch one authenticated list page with a Chrome TLS/HTTP2 profile."""

    async with BrowserProfileSession(
        impersonate="chrome",
        timeout=timeout,
        allow_redirects=True,
    ) as session:
        response = await session.get(url, headers=headers)
    if response.status_code >= 400:
        raise SourceError(
            f"guba browser-profile transport returned HTTP {response.status_code}"
        )
    return response.text


def _query_fingerprint(query: str) -> str:
    return hashlib.sha256(query.strip().encode("utf-8")).hexdigest()[:16]


def guba_hot_cursor(query: str, *, max_items: int = 200) -> str:
    """Seed a bounded, date-filtered collection from Eastmoney's hot feed."""

    if max_items < 1:
        raise ValueError("guba hot-feed max_items must be at least 1")
    return encode_cursor(
        "guba",
        {
            "q": _query_fingerprint(query),
            "mode": "hot",
            "max_items": max_items,
            "items_seen": 0,
        },
    )


def _match_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).strip().casefold()
    text = re.sub(r"[\s\-_/]+", "", text)
    return text.removesuffix("吧")


def select_guba_boards(
    records: list[dict[str, Any]], query: str, *, limit: int = 3
) -> list[dict[str, str]]:
    """Select deterministic, relevant boards from Eastmoney's official suggestions."""

    query_key = _match_text(query)
    candidates: list[tuple[int, int, dict[str, str]]] = []
    seen: set[str] = set()
    for position, record in enumerate(records):
        code = str(record.get("OuterCode") or "").strip()
        name = html_to_text(record.get("ShortName"))
        if not code or code in seen:
            continue
        seen.add(code)
        code_key = _match_text(code)
        name_key = _match_text(name)
        if code_key == query_key:
            score = 0
        elif name_key == query_key:
            score = 1
        elif query_key and (name_key.startswith(query_key) or name_key.endswith(query_key)):
            score = 2
        elif query_key and (query_key in name_key or query_key in code_key):
            score = 3
        else:
            score = 4
        candidates.append((score, position, {"code": code, "name": name or code}))

    candidates.sort(key=lambda item: (item[0], item[1]))
    code_matches = [item for item in candidates if item[0] == 0]
    if code_matches:
        return [code_matches[0][2]]

    exact_names = [item for item in candidates if item[0] == 1]
    selected = exact_names[:2]
    selected_codes = {item[2]["code"] for item in selected}
    for item in candidates:
        if len(selected) >= limit:
            break
        # The suggestion endpoint may mix popular but unrelated boards into
        # the tail. Never turn those generic suggestions into topic data.
        if item[0] > 3:
            continue
        if item[2]["code"] not in selected_codes:
            selected.append(item)
            selected_codes.add(item[2]["code"])
    return [item[2] for item in selected]


def _extract_javascript_value(document: str, pattern: str) -> Any:
    match = re.search(pattern, document)
    if not match:
        raise SourceError("guba response is missing its embedded article data")
    try:
        value, _end = json.JSONDecoder().raw_decode(document, match.end())
    except (ValueError, json.JSONDecodeError) as exc:
        raise SourceError("guba embedded article data is malformed") from exc
    return value


def parse_guba_page(document: str) -> dict[str, Any]:
    if "身份核实" in document or "fd_guba_validate" in document:
        raise SourceError(
            "guba returned an identity-verification page; pause this source and retry later"
        )
    value = _extract_javascript_value(document, r"var\s+article_list\s*=\s*")
    if not isinstance(value, dict) or not isinstance(value.get("re"), list):
        raise SourceError("guba article list has an unexpected shape")
    return value


def _guba_datetime(value: Any) -> datetime:
    if value in (None, ""):
        raise SourceError("guba post has no publication time")
    try:
        parsed = datetime.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise SourceError(f"guba returned an invalid publication time: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    result = as_utc(parsed)
    assert result is not None
    return result


def _guba_post_url(record: dict[str, Any]) -> str:
    post_id = str(record.get("post_id") or "")
    source_id = str(record.get("post_source_id") or "")
    if int(record.get("post_type") or 0) == 20 and source_id:
        return f"https://caifuhao.eastmoney.com/news/{quote(source_id, safe='')}"
    guba = record.get("post_guba") if isinstance(record.get("post_guba"), dict) else {}
    code = str(guba.get("stockbar_code") or record.get("stockbar_code") or "")
    return f"{GUBA_BASE_URL}/news,{quote(code, safe='')},{quote(post_id, safe='')}.html"


def _looks_truncated(value: Any) -> bool:
    return html_to_text(value).rstrip().endswith(("...", "…"))


def _mapping_from_guba_record(
    record: dict[str, Any],
    *,
    full_body: str | None = None,
    sampling_rank: int | None = None,
    sampling_limit: int | None = None,
) -> dict[str, Any]:
    post_id = record.get("post_id")
    if post_id in (None, ""):
        raise SourceError("guba post has no stable post id")
    published_at = _guba_datetime(record.get("post_publish_time"))
    updated_at = _guba_datetime(record.get("post_last_time") or record.get("post_publish_time"))
    title = html_to_text(record.get("post_title"))
    summary = html_to_text(
        record.get("post_content")
        or record.get("post_abstract")
        or record.get("source_post_content")
        or title
    )
    body = full_body or summary or title
    user = record.get("post_user") if isinstance(record.get("post_user"), dict) else {}
    guba = record.get("post_guba") if isinstance(record.get("post_guba"), dict) else {}
    mapping: dict[str, Any] = {
        "id": str(post_id),
        "published_at": published_at.isoformat(),
        "updated_at": updated_at.isoformat(),
        "timestamp_semantics": "published",
        "source_timestamp_field": "post_publish_time",
        "source_timezone": "Asia/Shanghai",
        "title": title or None,
        "body": body,
        "url": _guba_post_url(record),
        "author_id": str(user.get("user_id")) if user.get("user_id") not in (None, "") else None,
        "author": html_to_text(user.get("user_nickname")) or None,
        "views": record.get("post_click_count"),
        "comments": record.get("post_comment_count"),
        "likes": record.get("post_like_count"),
        "shares": record.get("post_forward_count"),
        "stockbar_code": str(guba.get("stockbar_code") or "") or None,
        "stockbar_name": html_to_text(guba.get("stockbar_name")) or None,
        "post_type": record.get("post_type"),
        "body_truncated": bool(_looks_truncated(summary) and not full_body),
        "language": "zh-CN",
    }
    if sampling_rank is not None:
        mapping.update(
            {
                "sampled": True,
                "sampling_policy": "eastmoney-hot-feed-time-bounded-top-n-v1",
                "sampling_mode": "hot",
                "sampling_rank": sampling_rank,
                "sampling_limit": sampling_limit,
            }
        )
    return {key: value for key, value in mapping.items() if value is not None}


class GubaSource(FixtureSource):
    """东方财富股吧 collector using its public, read-only web responses by default."""

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        credential: SourceCredential | None = None,
        use_fixture: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
        max_boards: int = 3,
        max_detail_requests: int = 4,
        min_public_interval: float | None = None,
        min_json_interval: float | None = None,
        access_retry_delays: tuple[float, ...] | None = None,
        session_file: Path | None = None,
        **kwargs,
    ):
        super().__init__("guba", **kwargs)
        self.credential = credential or SourceCredential("guba", endpoint=endpoint)
        self.endpoint = endpoint or self.credential.endpoint
        self.use_fixture = use_fixture
        self.transport = transport
        self.max_boards = max(1, max_boards)
        self.max_detail_requests = max(0, max_detail_requests)
        self.min_public_interval = (
            15.0
            if min_public_interval is None and transport is None
            else float(min_public_interval or 0)
        )
        self.min_json_interval = (
            5.0
            if min_json_interval is None and transport is None
            else float(min_json_interval or 0)
        )
        self.access_retry_delays = (
            (60.0,)
            if access_retry_delays is None and transport is None
            else tuple(access_retry_delays or ())
        )
        self.session_file = Path(session_file) if session_file is not None else None

    async def collect(self, query, since, cursor=None, *, until=None) -> CollectResult:
        if not self.endpoint and self.use_fixture:
            return await super().collect(query, since, cursor, until=until)
        if self.endpoint:
            return await self._collect_custom(query, since, cursor, until=until)
        return await self._collect_public(query, since, cursor, until=until)

    async def _collect_custom(self, query, since, cursor=None, *, until=None) -> CollectResult:
        if not self.credential.has_auth:
            raise SourceError("guba custom endpoint requires an API key or access token")
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
            raise SourceError(f"guba custom collection failed: {exc}") from exc

    async def _resolve_boards(self, client: httpx.AsyncClient, query: str) -> list[dict[str, str]]:
        response = await public_get(
            client,
            GUBA_SUGGEST_URL,
            rate_source=self.name,
            min_interval=self.min_public_interval,
            params={
                "input": query,
                "type": "8",
                "count": "20",
            },
            headers={"Referer": f"{GUBA_BASE_URL}/"},
        )
        response.raise_for_status()
        payload = response.json()
        table = payload.get("GubaCodeTable") if isinstance(payload, dict) else None
        records = table.get("Data") if isinstance(table, dict) else None
        if not isinstance(records, list):
            raise SourceError("guba board suggestions have an unexpected shape")
        boards = select_guba_boards(
            [item for item in records if isinstance(item, dict)],
            query,
            limit=self.max_boards,
        )
        if not boards:
            raise SourceError(f"guba found no board related to query {query!r}")
        return boards

    async def _full_body(self, client: httpx.AsyncClient, record: dict[str, Any]) -> str | None:
        try:
            response = await public_get(
                client,
                _guba_post_url(record),
                rate_source=self.name,
                min_interval=self.min_public_interval,
                headers={"Referer": f"{GUBA_BASE_URL}/"},
            )
            response.raise_for_status()
            if int(record.get("post_type") or 0) == 20:
                value = _extract_javascript_value(response.text, r"var\s+articleTxt\s*=\s*")
            else:
                article = _extract_javascript_value(response.text, r"var\s+post_article\s*=\s*")
                value = article.get("post_content") if isinstance(article, dict) else None
            body = html_to_text(value)
            return body or None
        except (httpx.HTTPError, SourceError, ValueError, TypeError):
            return None

    async def _public_page_data(
        self,
        client: httpx.AsyncClient,
        page_url: str,
        *,
        headers: dict[str, str],
        browser_profile: bool = False,
    ) -> dict[str, Any]:
        """Fetch a list page with source-aware pacing and bounded access-page retries."""

        global _next_public_list_request_at

        attempts = len(self.access_retry_delays) + 1
        last_error: SourceError | None = None
        for attempt in range(attempts):
            wait = max(0.0, _next_public_list_request_at - monotonic())
            if wait:
                await asyncio.sleep(wait)
            _next_public_list_request_at = monotonic() + self.min_public_interval
            try:
                if browser_profile:
                    await PUBLIC_RATE_LIMITER.wait(self.name, self.min_public_interval)
                    document = await _browser_profile_get(
                        page_url,
                        headers=headers,
                        timeout=20,
                    )
                else:
                    response = await public_get(
                        client,
                        page_url,
                        rate_source=self.name,
                        min_interval=self.min_public_interval,
                        headers=headers,
                    )
                    response.raise_for_status()
                    document = response.text
                return parse_guba_page(document)
            except (SourceError, BrowserProfileRequestError) as exc:
                if isinstance(exc, BrowserProfileRequestError):
                    current_error = SourceError(
                        f"guba browser-profile transport failed: {exc}"
                    )
                else:
                    current_error = exc
                last_error = current_error
                if attempt >= len(self.access_retry_delays):
                    raise current_error
                await asyncio.sleep(self.access_retry_delays[attempt])
        assert last_error is not None
        raise last_error

    async def _json_page_data(
        self,
        client: httpx.AsyncClient,
        *,
        code: str,
        page: int,
        page_size: int = 40,
        mode: str = "latest",
    ) -> dict[str, Any]:
        """Fetch the public list JSON used by Eastmoney's own web client."""

        try:
            endpoint = GUBA_LIST_API_URL
            params = {
                "code": code,
                "p": str(page),
                "ps": str(page_size),
                "plat": "web",
                "version": "300",
                "product": "guba",
            }
            if mode == "hot":
                endpoint = GUBA_HOT_LIST_API_URL
                # This is the account-type selector used by the web client's
                # "热门" tab: 0=all, 1=individual, 2=institution.
                params["type"] = "0"
            else:
                params["sorttype"] = "1"
            response = await public_get(
                client,
                endpoint,
                rate_source=self.name,
                min_interval=self.min_json_interval,
                params=params,
                headers={"Referer": f"{GUBA_BASE_URL}/list,{quote(code, safe='')}.html"},
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("re"), list):
                raise SourceError("guba JSON article list has an unexpected shape")
            return payload
        except SourceError:
            raise
        except (httpx.HTTPError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise SourceError(f"guba JSON article list failed: {exc}") from exc

    async def _collect_public(self, query, since, cursor=None, *, until=None) -> CollectResult:
        since = as_utc(since) or since
        until = as_utc(until) or now_utc()
        if since >= until:
            raise SourceError("guba collection window must have since before until")

        session_cookie = None
        session_headers: dict[str, str] = {}
        if self.session_file is not None:
            try:
                session_cookie = source_session_cookie_header("guba", self.session_file)
                session_headers = source_session_request_headers("guba", self.session_file)
            except SourceSessionError as exc:
                raise SourceError(
                    "guba saved browser session is invalid; login required again: "
                    f"{exc}"
                ) from exc
        list_cookie = (
            f"{session_cookie}; listtype=1" if session_cookie else "listtype=1"
        )

        try:
            async with httpx.AsyncClient(
                timeout=20,
                follow_redirects=True,
                headers=PUBLIC_HEADERS,
                # public_get owns the bounded retry budget. Layering transport
                # retries here would multiply one failed page into nine waits.
                transport=self.transport,
            ) as client:
                if cursor:
                    state = decode_cursor(self.name, cursor)
                    if state.get("q") != _query_fingerprint(query):
                        raise SourceError("guba pagination cursor belongs to another query")
                    mode = str(state.get("mode") or "latest")
                    if mode not in {"latest", "hot"}:
                        raise SourceError("invalid guba pagination cursor mode")
                    max_items = state.get("max_items") if mode == "hot" else None
                    items_seen = state.get("items_seen", 0)
                    if (
                        mode == "hot"
                        and (
                            not isinstance(max_items, int)
                            or max_items < 1
                            or not isinstance(items_seen, int)
                            or items_seen < 0
                        )
                    ):
                        raise SourceError("invalid guba hot-feed sampling cursor")
                    boards = state.get("boards")
                    board_index = state.get("board")
                    page = state.get("page")
                    if mode == "hot" and boards is None:
                        # A seed cursor deliberately contains no resolved board.
                        # Resolve at execution time and keep only the strongest
                        # match so the Top-N budget is not consumed by loosely
                        # related company boards returned by suggestions.
                        boards = (await self._resolve_boards(client, query))[:1]
                        board_index = 0
                        page = 1
                    if (
                        not isinstance(boards, list)
                        or not all(isinstance(item, dict) and item.get("code") for item in boards)
                        or not isinstance(board_index, int)
                        or not isinstance(page, int)
                        or board_index < 0
                        or page < 1
                    ):
                        raise SourceError("invalid guba pagination cursor")
                else:
                    mode = "latest"
                    max_items = None
                    items_seen = 0
                    boards = await self._resolve_boards(client, query)
                    board_index = 0
                    page = 1

                if board_index >= len(boards):
                    return CollectResult(items=[], exhausted=True)
                board = boards[board_index]
                code = quote(str(board["code"]), safe="")
                suffix = "f.html" if page == 1 else f"f_{page}.html"
                page_url = f"{GUBA_BASE_URL}/list,{code},{suffix}"
                try:
                    page_size = 40
                    if mode == "hot":
                        assert isinstance(max_items, int)
                        page_size = min(40, max_items - items_seen)
                        if page_size < 1:
                            return CollectResult(items=[], exhausted=True)
                        hot_code = str(board.get("hot_code") or "")
                        if not hot_code:
                            # Keyword aliases such as `huangjin` work on the
                            # normal list but the hot endpoint requires the
                            # canonical board code exposed in bar_info.
                            identity = await self._json_page_data(
                                client,
                                code=str(board["code"]),
                                page=1,
                            )
                            bar_info = identity.get("bar_info")
                            if isinstance(bar_info, dict):
                                hot_code = str(bar_info.get("OuterCode") or "")
                            hot_code = hot_code or str(board["code"])
                            board["hot_code"] = hot_code
                    page_data = await self._json_page_data(
                        client,
                        code=hot_code if mode == "hot" else str(board["code"]),
                        page=page,
                        page_size=page_size,
                        mode=mode,
                    )
                except SourceError as exc:
                    if mode == "hot":
                        # The ordinary HTML page does not implement the same
                        # sampled feed, so falling back would silently change
                        # the collection contract mid-cursor.
                        raise
                    try:
                        page_data = await self._public_page_data(
                            client,
                            page_url,
                            headers={
                                **session_headers,
                                "Cookie": list_cookie,
                                "Referer": f"{GUBA_BASE_URL}/",
                            },
                            browser_profile=bool(session_cookie and self.transport is None),
                        )
                    except SourceError as fallback_exc:
                        if session_cookie and "identity-verification page" in str(fallback_exc):
                            raise SourceError(
                                "guba JSON list failed and the authenticated HTML fallback "
                                "reached an identity-verification page"
                            ) from exc
                        raise
                records = [item for item in page_data["re"] if isinstance(item, dict)]

                published: list[tuple[dict[str, Any], datetime]] = []
                for record in records:
                    published.append((record, _guba_datetime(record.get("post_publish_time"))))
                in_window = [record for record, ts in published if since <= ts < until]

                truncated = [
                    record for record in in_window if _looks_truncated(record.get("post_content"))
                ][: self.max_detail_requests]
                bodies: dict[str, str] = {}
                if truncated:
                    # Keep request concurrency modest while still filling every
                    # truncated body selected from this page.
                    for offset in range(0, len(truncated), 4):
                        batch = truncated[offset : offset + 4]
                        results = await asyncio.gather(
                            *(self._full_body(client, record) for record in batch)
                        )
                        for record, body in zip(batch, results, strict=False):
                            if body:
                                bodies[str(record.get("post_id"))] = body

                observed_at = as_utc(self.clock()) or now_utc()
                items = []
                source_ranks = {
                    id(record): items_seen + index
                    for index, record in enumerate(records, start=1)
                }
                for record in in_window:
                    source_rank = source_ranks[id(record)] if mode == "hot" else None
                    items.append(
                        raw_from_mapping(
                            self.name,
                            _mapping_from_guba_record(
                                record,
                                full_body=bodies.get(str(record.get("post_id"))),
                                sampling_rank=source_rank,
                                sampling_limit=max_items,
                            ),
                            observation_kind="forum_post",
                            observed_at=observed_at,
                        )
                    )

                dated_non_pinned = [
                    ts for record, ts in published if int(record.get("post_top_status") or 0) != 1
                ]
                count = page_data.get("count")
                try:
                    total_count = int(count)
                except (TypeError, ValueError):
                    total_count = page * max(len(records), 1)
                reached_since = bool(dated_non_pinned and min(dated_non_pinned) < since)
                if mode == "hot":
                    items_seen += len(records)
                board_done = (
                    not records
                    or reached_since
                    or page * max(len(records), 1) >= total_count
                    or len(records) < page_size
                    or (mode == "hot" and items_seen >= int(max_items or 0))
                )
                if board_done:
                    board_index += 1
                    page = 1
                else:
                    page += 1
                exhausted = board_index >= len(boards)
                next_cursor = None
                if not exhausted:
                    next_cursor = encode_cursor(
                        self.name,
                        {
                            "q": _query_fingerprint(query),
                            "boards": boards,
                            "board": board_index,
                            "page": page,
                            "mode": mode,
                            "max_items": max_items,
                            "items_seen": items_seen,
                        },
                    )
                return CollectResult(
                    items=items,
                    next_cursor=next_cursor,
                    exhausted=exhausted,
                )
        except SourceError:
            raise
        except (httpx.HTTPError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise SourceError(f"guba public collection failed: {exc}") from exc

    def probe(self) -> ProbeResult:
        return ProbeResult(
            source=self.name,
            source_type=self.source_type,
            checks={
                "transport": "public-read-only",
                "keyword_to_board_resolution": True,
                "stable_post_id": True,
                "publication_time": True,
                "publication_time_field": "post_publish_time",
                "body_summary": True,
                "interaction_counts": True,
                "cursor_pagination": True,
                "time_window_filter": True,
                "api_key_required": False,
            },
            notes=[
                "Uses Eastmoney's public board suggestions and read-only WebArticleList JSON pagination.",
                "Configured high-volume topics may use Eastmoney's paginated hot feed with an explicit Top-N sampling limit.",
                "The authenticated Chrome-profile HTML path remains a fallback; cookies stay in the local session file.",
            ],
        )
