from __future__ import annotations

import base64
import hashlib
import json
import multiprocessing
import os
import threading
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any
from urllib.parse import quote

from apis.xhs_pc_apis import XHS_Apis
from fastapi import FastAPI, HTTPException, Request
from loguru import logger as upstream_logger
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse
from xhs_utils.xhs_pc import XHSPcAuth
from xhs_utils.xhs_pc.params import generate_search_id

COOKIE_FILE = Path(os.environ.get("XHS_COOKIE_FILE", "/run/secrets/xhs-cookies.json"))
API_KEY = os.environ.get("BRIDGE_API_KEY", "").strip()
API_KEY_HEADER = os.environ.get("BRIDGE_API_KEY_HEADER", "X-API-Key")
SORT_TYPES = {"综合": 0, "最新": 1, "最多点赞": 2, "最多评论": 3, "最多收藏": 4}
TIME_FILTERS = {"不限": 0, "一天内": 1, "一周内": 2, "半年内": 3}
SEARCH_TIMEOUT_SECONDS = max(1.0, float(os.environ.get("BRIDGE_SEARCH_TIMEOUT_SECONDS", "45")))
DETAIL_TIMEOUT_SECONDS = max(1.0, float(os.environ.get("BRIDGE_DETAIL_TIMEOUT_SECONDS", "30")))


class SearchRequest(BaseModel):
    keyword: str = Field(min_length=1, max_length=100)
    cursor: str | None = None
    filters: dict[str, Any] = Field(default_factory=dict)


class DetailRequest(BaseModel):
    feed_id: str = Field(min_length=1, max_length=128)
    xsec_token: str = Field(min_length=1, max_length=2048)
    xsec_source: str = Field(default="pc_search", min_length=1, max_length=64)
    load_all_comments: bool = False


class CandidateUnavailableError(RuntimeError):
    """The search hit no longer has a readable note-detail payload."""


class UpstreamTimeoutError(RuntimeError):
    """The replaceable Spider_XHS worker exceeded its hard deadline."""


class UpstreamResponseError(RuntimeError):
    """Spider_XHS returned a response shape that cannot prove note absence."""


def _cookie_header(path: Path) -> tuple[str, int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("cookies") if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        raise TypeError("cookie file must contain a cookie list")
    pairs: list[str] = []
    for row in records:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        value = str(row.get("value") or "").strip()
        if name and value:
            pairs.append(f"{name}={value}")
    names = {pair.partition("=")[0] for pair in pairs}
    if not {"a1", "web_session"}.issubset(names):
        raise ValueError("cookie file is missing a1 or web_session")
    return "; ".join(pairs), len(pairs)


def _cursor_signature(keyword: str, sort_by: str) -> str:
    return hashlib.sha256(f"{keyword}\0{sort_by}".encode()).hexdigest()[:16]


def _encode_cursor(*, page: int, root_search_id: str, keyword: str, sort_by: str) -> str:
    payload = {
        "v": 1,
        "page": page,
        "root": root_search_id,
        "sig": _cursor_signature(keyword, sort_by),
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(value: str, *, keyword: str, sort_by: str) -> tuple[int, str]:
    try:
        padded = value + "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
        page = int(payload["page"])
        root = str(payload["root"])
        valid = payload.get("v") == 1 and payload.get("sig") == _cursor_signature(keyword, sort_by)
        if not valid or page < 2 or not root:
            raise ValueError
        return page, root
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("invalid or mismatched search cursor") from exc


def _candidate(row: dict[str, Any]) -> dict[str, Any] | None:
    note_id = str(row.get("id") or row.get("note_id") or "").strip()
    token = str(row.get("xsec_token") or row.get("xsecToken") or "").strip()
    if not note_id or not token:
        return None
    card = row.get("note_card") if isinstance(row.get("note_card"), dict) else {}
    return {
        "id": note_id,
        "xsecToken": token,
        "xsecSource": row.get("xsec_source") or row.get("xsecSource") or "pc_search",
        "noteCard": {"displayTitle": card.get("display_title") or card.get("title") or ""},
    }


def _normalized_note(note_id: str, card: dict[str, Any]) -> dict[str, Any]:
    user = card.get("user") if isinstance(card.get("user"), dict) else {}
    interaction = card.get("interact_info") if isinstance(card.get("interact_info"), dict) else {}
    return {
        "noteId": str(card.get("note_id") or note_id),
        "time": card.get("time"),
        "title": card.get("title") or "",
        "desc": card.get("desc") or card.get("description") or "",
        "type": card.get("type"),
        "user": {
            "userId": user.get("user_id") or user.get("userId") or user.get("id"),
            "nickname": user.get("nickname") or user.get("nick_name"),
        },
        "interactInfo": {
            "likedCount": interaction.get("liked_count") or interaction.get("likedCount"),
            "collectedCount": interaction.get("collected_count")
            or interaction.get("collectedCount"),
            "commentCount": interaction.get("comment_count") or interaction.get("commentCount"),
            "sharedCount": interaction.get("share_count") or interaction.get("sharedCount"),
        },
    }


class SpiderRuntime:
    def __init__(self, cookie_file: Path):
        self.cookie_file = cookie_file
        self.lock = threading.Lock()
        self.mtime_ns: int | None = None
        self.cookie_count = 0
        self.auth: XHSPcAuth | None = None
        self.api: XHS_Apis | None = None

    def _client(self) -> XHS_Apis:
        mtime_ns = self.cookie_file.stat().st_mtime_ns
        if self.api is not None and self.mtime_ns == mtime_ns:
            return self.api
        cookie, count = _cookie_header(self.cookie_file)
        if self.auth is not None:
            self.auth.close()
        self.auth = XHSPcAuth.from_cookie(cookie)
        self.api = XHS_Apis(self.auth)
        self.mtime_ns = mtime_ns
        self.cookie_count = count
        return self.api

    def status(self) -> dict[str, Any]:
        with self.lock:
            try:
                _cookie, count = _cookie_header(self.cookie_file)
                return {"status": "ready", "cookie_count": count, "upstream": "Spider_XHS"}
            except Exception as exc:  # noqa: BLE001 - safe status text only
                return {"status": "not_ready", "error": str(exc), "upstream": "Spider_XHS"}

    def search(self, body: SearchRequest) -> dict[str, Any]:
        with self.lock:
            api = self._client()
            sort_by = str(body.filters.get("sort_by") or "最新")
            publish_time = str(body.filters.get("publish_time") or "不限")
            if sort_by not in SORT_TYPES or publish_time not in TIME_FILTERS:
                raise ValueError("unsupported search filter")
            if body.cursor:
                page, root_search_id = _decode_cursor(
                    body.cursor, keyword=body.keyword, sort_by=sort_by
                )
            else:
                page, root_search_id = 1, generate_search_id()
            success, message, response = api.search_note(
                body.keyword,
                page=page,
                sort_type_choice=SORT_TYPES[sort_by],
                note_time=TIME_FILTERS[publish_time],
                search_id=generate_search_id(root_search_id),
            )
            if not success or not isinstance(response, dict):
                raise RuntimeError(message or "search failed")
            data = response.get("data") if isinstance(response.get("data"), dict) else {}
            feeds = [
                item
                for row in data.get("items", [])
                if isinstance(row, dict)
                if (item := _candidate(row))
            ]
            has_more = bool(data.get("has_more"))
            next_cursor = (
                _encode_cursor(
                    page=page + 1,
                    root_search_id=root_search_id,
                    keyword=body.keyword,
                    sort_by=sort_by,
                )
                if has_more
                else None
            )
            return {"feeds": feeds, "has_more": has_more, "next_cursor": next_cursor}

    def detail(self, body: DetailRequest) -> dict[str, Any]:
        with self.lock:
            api = self._client()
            url = (
                f"https://www.xiaohongshu.com/explore/{quote(body.feed_id, safe='')}"
                f"?xsec_token={quote(body.xsec_token, safe='')}"
                f"&xsec_source={quote(body.xsec_source, safe='')}"
            )
            try:
                success, message, response = api.get_note_info(url)
            except KeyError as exc:
                # Spider_XHS currently indexes ``res_json["msg"]`` when the
                # upstream returns ``success=true`` with an empty ``data``
                # object. That response contains no usable note but does not
                # prove deletion, so classify it as a retryable transport-shape
                # failure and let RetailTide try its bounded MCP backup.
                if exc.args == ("msg",):
                    raise UpstreamResponseError("note detail response is empty") from exc
                raise
            missing_message_field = (
                "missing required field: msg" in str(message).casefold()
                or str(message).strip("'\"").casefold() == "msg"
            )
            if not success and missing_message_field:
                # Newer Spider_XHS wraps the same ``success=true, data={}``
                # response instead of re-raising the KeyError handled above.
                raise UpstreamResponseError("note detail response is empty")
            if not success or not isinstance(response, dict):
                raise RuntimeError(message or "detail failed")
            data = response.get("data") if isinstance(response.get("data"), dict) else {}
            records = data.get("items") if isinstance(data.get("items"), list) else []
            first = records[0] if records and isinstance(records[0], dict) else {}
            card = first.get("note_card") if isinstance(first.get("note_card"), dict) else {}
            if not card:
                raise CandidateUnavailableError("note detail is unavailable")
            return {"note": _normalized_note(body.feed_id, card), "comments": {"list": []}}


def _worker_main(connection: Connection, cookie_file: str) -> None:
    """Serve one serial Spider_XHS client inside a killable process.

    Requests/urllib calls inside Spider_XHS are not reliably cancellable.  A
    process boundary lets the parent enforce a real deadline without leaving a
    timed-out thread holding the source lock and stalling every later request.
    """

    # Spider_XHS logs exception tracebacks containing the full detail URL,
    # including its transient xsec token. The bridge returns sanitized errors
    # to the parent, so disable the upstream sink rather than persisting that
    # request material in container logs.
    upstream_logger.remove()
    runtime = SpiderRuntime(Path(cookie_file))
    try:
        while True:
            message = connection.recv()
            if message is None:
                return
            operation = str(message.get("operation") or "")
            payload = message.get("payload") or {}
            try:
                if operation == "search":
                    result = runtime.search(SearchRequest.model_validate(payload))
                elif operation == "detail":
                    result = runtime.detail(DetailRequest.model_validate(payload))
                else:
                    raise ValueError(f"unsupported worker operation: {operation}")
                connection.send({"ok": True, "data": result})
            except CandidateUnavailableError as exc:
                connection.send(
                    {"ok": False, "error_code": "candidate_unavailable", "message": str(exc)}
                )
            except UpstreamResponseError as exc:
                connection.send(
                    {"ok": False, "error_code": "response_invalid", "message": str(exc)}
                )
            except Exception as exc:  # noqa: BLE001 - parent maps safe text to an API error
                connection.send(
                    {
                        "ok": False,
                        "error_code": "upstream_rejected",
                        "message": str(exc) or type(exc).__name__,
                    }
                )
    except (EOFError, BrokenPipeError):
        return
    finally:
        connection.close()


class SpiderWorker:
    """A lazy, serial, replaceable process around ``SpiderRuntime``."""

    def __init__(self, cookie_file: Path):
        self.cookie_file = cookie_file
        self.lock = threading.Lock()
        self.context = multiprocessing.get_context("spawn")
        self.process: multiprocessing.Process | None = None
        self.connection: Connection | None = None

    def _stop(self) -> None:
        connection, process = self.connection, self.process
        self.connection = None
        self.process = None
        if connection is not None:
            connection.close()
        if process is not None and process.is_alive():
            process.terminate()
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join(timeout=2)

    def _start(self) -> None:
        self._stop()
        parent, child = self.context.Pipe()
        process = self.context.Process(
            target=_worker_main,
            args=(child, str(self.cookie_file)),
            name="xhs-spider-worker",
            daemon=True,
        )
        process.start()
        child.close()
        self.connection = parent
        self.process = process

    def _ensure_started(self) -> None:
        if (
            self.process is None
            or self.connection is None
            or not self.process.is_alive()
        ):
            self._start()

    def call(self, operation: str, payload: dict[str, Any], *, timeout: float) -> dict[str, Any]:
        with self.lock:
            self._ensure_started()
            assert self.connection is not None
            try:
                self.connection.send({"operation": operation, "payload": payload})
                if not self.connection.poll(timeout):
                    self._stop()
                    raise UpstreamTimeoutError(
                        f"Spider_XHS {operation} exceeded {timeout:.0f}s hard deadline"
                    )
                response = self.connection.recv()
            except (EOFError, BrokenPipeError, OSError) as exc:
                self._stop()
                raise RuntimeError("Spider_XHS worker exited unexpectedly") from exc
            if response.get("ok"):
                return dict(response.get("data") or {})
            if response.get("error_code") == "candidate_unavailable":
                raise CandidateUnavailableError(str(response.get("message") or "unavailable"))
            if response.get("error_code") == "response_invalid":
                raise UpstreamResponseError(str(response.get("message") or "invalid response"))
            raise RuntimeError(str(response.get("message") or "Spider_XHS request failed"))

    def status(self) -> dict[str, Any]:
        with self.lock:
            try:
                _cookie, count = _cookie_header(self.cookie_file)
                return {
                    "status": "ready",
                    "cookie_count": count,
                    "upstream": "Spider_XHS",
                    "worker_running": bool(self.process and self.process.is_alive()),
                }
            except Exception as exc:  # noqa: BLE001 - safe status text only
                return {"status": "not_ready", "error": str(exc), "upstream": "Spider_XHS"}


worker = SpiderWorker(COOKIE_FILE)
app = FastAPI(title="RetailTide Spider_XHS bridge", docs_url=None, redoc_url=None)


@app.middleware("http")
async def authorize(request: Request, call_next):
    if API_KEY and request.headers.get(API_KEY_HEADER) != API_KEY:
        return JSONResponse({"success": False, "message": "unauthorized"}, status_code=401)
    return await call_next(request)


@app.get("/health")
async def health():
    result = await run_in_threadpool(worker.status)
    return {"success": result["status"] == "ready", "data": result}


def _classified_error(exc: Exception) -> tuple[int, str, bool, int | None]:
    message = str(exc)
    lowered = message.casefold()
    if isinstance(exc, UpstreamTimeoutError):
        return 504, "upstream_timeout", True, 900
    if isinstance(exc, UpstreamResponseError):
        return 502, "response_invalid", True, 900
    if any(marker in lowered for marker in ("登录已过期", "未登录", "login expired")):
        return 401, "auth_required", False, None
    if any(marker in lowered for marker in ("429", "请求过于频繁", "rate limit")):
        return 429, "rate_limited", True, 1800
    if any(marker in lowered for marker in ("安全限制", "风控", "forbidden")):
        return 403, "upstream_rejected", False, None
    return 502, "upstream_rejected", True, 900


def _error_response(exc: Exception) -> JSONResponse:
    status_code, error_code, retryable, retry_after = _classified_error(exc)
    return JSONResponse(
        {
            "success": False,
            "error_code": error_code,
            "message": str(exc),
            "retryable": retryable,
            "retry_after_seconds": retry_after,
            "transport": "spider",
        },
        status_code=status_code,
        headers={"Retry-After": str(retry_after)} if retry_after else None,
    )


@app.post("/api/v1/feeds/search")
async def search(body: SearchRequest):
    try:
        data = await run_in_threadpool(
            worker.call,
            "search",
            body.model_dump(),
            timeout=SEARCH_TIMEOUT_SECONDS,
        )
        return {"success": True, "data": data}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert worker failures to safe API errors
        return _error_response(exc)


@app.post("/api/v1/feeds/detail")
async def detail(body: DetailRequest):
    try:
        data = await run_in_threadpool(
            worker.call,
            "detail",
            body.model_dump(),
            timeout=DETAIL_TIMEOUT_SECONDS,
        )
        return {"success": True, "data": data}
    except CandidateUnavailableError as exc:
        return JSONResponse(
            {
                "success": False,
                "error_code": "candidate_unavailable",
                "message": str(exc),
                "retryable": False,
                "retry_after_seconds": None,
                "transport": "spider",
            },
            status_code=404,
        )
    except Exception as exc:  # noqa: BLE001 - convert worker failures to safe API errors
        if "笔记不存在" in str(exc):
            return JSONResponse(
                {
                    "success": False,
                    "error_code": "candidate_unavailable",
                    "message": str(exc),
                    "retryable": False,
                    "retry_after_seconds": None,
                    "transport": "spider",
                },
                status_code=404,
            )
        return _error_response(exc)
