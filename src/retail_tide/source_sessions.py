from __future__ import annotations

import json
import os
import re
import shlex
import stat
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SESSION_VERSION = 1
MAX_SESSION_FILE_BYTES = 256 * 1024
MAX_SESSION_COOKIES = 128
MAX_COOKIE_HEADER_BYTES = 32 * 1024
MAX_SESSION_HEADER_VALUE_BYTES = 2 * 1024
_COOKIE_NAME = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_SESSION_HEADER_NAMES = {
    "accept",
    "accept-language",
    "cache-control",
    "dnt",
    "pragma",
    "priority",
    "sec-ch-ua",
    "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
    "sec-fetch-dest",
    "sec-fetch-mode",
    "sec-fetch-site",
    "sec-fetch-user",
    "upgrade-insecure-requests",
    "user-agent",
}
_TARGETS = {
    "guba": {
        "host": "guba.eastmoney.com",
        "default_domain": ".eastmoney.com",
        "request_path": "/list",
    },
    "taoguba": {
        "host": "www.tgb.cn",
        "default_domain": ".tgb.cn",
        "request_path": "/search/getSearchTopicResult",
    }
}


class SourceSessionError(ValueError):
    """An authorized browser session cannot be imported or used safely."""


def _target(source: str) -> dict[str, str]:
    normalized = source.lower().replace("_", "-")
    try:
        return _TARGETS[normalized]
    except KeyError as exc:
        raise SourceSessionError(
            f"browser-session reuse is not supported for source {source!r}"
        ) from exc


def _read_secret_file(path: Path) -> str:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise SourceSessionError("session input file does not exist") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SourceSessionError("session input must be a regular, non-symlink file")
    if info.st_size > MAX_SESSION_FILE_BYTES:
        raise SourceSessionError("session input exceeds the 256 KiB safety limit")
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise SourceSessionError("session input could not be read as UTF-8") from exc


def _domain_matches(host: str, domain: str) -> bool:
    normalized = str(domain or "").strip().lstrip(".").lower()
    return bool(normalized and (host == normalized or host.endswith(f".{normalized}")))


def _valid_cookie_value(value: str) -> bool:
    # Empty cookie values are valid and occur in real browser sessions.  The
    # surrounding parser still requires a non-empty, valid cookie name and an
    # explicit ``=`` separator, so accepting ``name=`` does not turn a blank
    # header or a stray delimiter into a cookie.
    return ";" not in value and not any(
        ord(character) < 0x20 or ord(character) == 0x7F for character in value
    )


def _cookie_pairs(header: str, *, domain: str) -> list[dict[str, Any]]:
    if "\r" in header or "\n" in header:
        raise SourceSessionError("cookie header contains a forbidden line break")
    cookies: list[dict[str, Any]] = []
    for part in header.split(";"):
        name, separator, value = part.strip().partition("=")
        if not separator or not _COOKIE_NAME.fullmatch(name) or not _valid_cookie_value(value):
            raise SourceSessionError("cookie header contains an invalid cookie pair")
        cookies.append(
            {
                "name": name,
                "value": value,
                "domain": domain,
                "path": "/",
                "expires": None,
            }
        )
    if not cookies:
        raise SourceSessionError("session input contains no cookies")
    return cookies


def _curl_cookie_header(text: str) -> str | None:
    try:
        tokens = shlex.split(text, posix=True)
    except ValueError as exc:
        raise SourceSessionError("cURL session input has invalid shell quoting") from exc
    if not tokens or Path(tokens[0]).name != "curl":
        return None
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token in {"-b", "--cookie"} and index + 1 < len(tokens):
            return tokens[index + 1].strip()
        if token.startswith("--cookie="):
            return token.partition("=")[2].strip()
        if token in {"-H", "--header"} and index + 1 < len(tokens):
            header = tokens[index + 1]
            name, separator, value = header.partition(":")
            if separator and name.strip().casefold() == "cookie":
                return value.strip()
            index += 1
        index += 1
    raise SourceSessionError("cURL session input does not contain a Cookie header")


def _valid_session_header_value(value: str) -> bool:
    return (
        bool(value)
        and "\r" not in value
        and "\n" not in value
        and len(value.encode("utf-8")) <= MAX_SESSION_HEADER_VALUE_BYTES
    )


def _filtered_session_headers(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    headers: dict[str, str] = {}
    for raw_name, raw_value in value.items():
        name = str(raw_name).strip().casefold()
        if name not in _SESSION_HEADER_NAMES or not isinstance(raw_value, str):
            continue
        header_value = raw_value.strip()
        if _valid_session_header_value(header_value):
            headers[name] = header_value
    return headers


def _curl_session_headers(text: str) -> dict[str, str]:
    try:
        tokens = shlex.split(text, posix=True)
    except ValueError as exc:
        raise SourceSessionError("cURL session input has invalid shell quoting") from exc
    if not tokens or Path(tokens[0]).name != "curl":
        return {}
    candidates: dict[str, str] = {}
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token in {"-A", "--user-agent"} and index + 1 < len(tokens):
            candidates["user-agent"] = tokens[index + 1]
            index += 1
        elif token.startswith("--user-agent="):
            candidates["user-agent"] = token.partition("=")[2]
        elif token in {"-H", "--header"} and index + 1 < len(tokens):
            name, separator, header_value = tokens[index + 1].partition(":")
            if separator:
                candidates[name.strip()] = header_value.strip()
            index += 1
        index += 1
    return _filtered_session_headers(candidates)


def _cookie_from_mapping(value: Any, *, host: str) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    name = value.get("name")
    cookie_value = value.get("value")
    domain = value.get("domain")
    path = value.get("path") or "/"
    if (
        not isinstance(name, str)
        or not _COOKIE_NAME.fullmatch(name)
        or not isinstance(cookie_value, str)
        or not _valid_cookie_value(cookie_value)
        or not isinstance(domain, str)
        or not _domain_matches(host, domain)
        or not isinstance(path, str)
        or not path.startswith("/")
    ):
        return None
    expires = value.get("expires")
    if expires in (None, "", -1, 0):
        normalized_expires: float | None = None
    else:
        try:
            normalized_expires = float(expires)
        except (TypeError, ValueError):
            return None
    return {
        "name": name,
        "value": cookie_value,
        "domain": domain.lower(),
        "path": path,
        "expires": normalized_expires,
    }


def _parse_session_input(
    source: str, text: str
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    target = _target(source)
    stripped = text.strip()
    if not stripped:
        raise SourceSessionError("session input is empty")
    try:
        value = json.loads(stripped)
    except json.JSONDecodeError:
        value = None
    if value is not None:
        raw_cookies = value.get("cookies") if isinstance(value, dict) else value
        if not isinstance(raw_cookies, list):
            raise SourceSessionError("session JSON must contain a cookies array")
        cookies = [
            parsed
            for item in raw_cookies
            if (parsed := _cookie_from_mapping(item, host=target["host"])) is not None
        ]
        headers = _filtered_session_headers(
            value.get("headers") if isinstance(value, dict) else None
        )
    else:
        header = _curl_cookie_header(stripped)
        headers = _curl_session_headers(stripped)
        if header is None:
            name, separator, possible_header = stripped.partition(":")
            header = (
                possible_header.strip()
                if separator and name.strip().casefold() == "cookie"
                else stripped
            )
        cookies = _cookie_pairs(header, domain=target["default_domain"])
    now = datetime.now(timezone.utc).timestamp()
    active = [
        cookie
        for cookie in cookies
        if cookie["expires"] is None or float(cookie["expires"]) > now
    ]
    if not active:
        raise SourceSessionError(
            f"session input contains no active cookies for {target['host']}"
        )
    if len(active) > MAX_SESSION_COOKIES:
        raise SourceSessionError("session input contains too many cookies")
    return active, headers


def _safe_parent(path: Path) -> None:
    for candidate in (path.parent, *path.parent.parents):
        if candidate.exists() and candidate.is_symlink():
            raise SourceSessionError("session directory must not traverse a symlink")
    existed = path.parent.exists()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.parent.is_symlink() or not path.parent.is_dir():
        raise SourceSessionError("session directory must be a real directory")
    if not existed:
        path.parent.chmod(0o700)


def import_source_session(source: str, input_path: Path, output_path: Path) -> dict[str, Any]:
    normalized = source.lower().replace("_", "-")
    _target(normalized)
    cookies, headers = _parse_session_input(normalized, _read_secret_file(input_path))
    output_path = Path(output_path)
    if output_path.is_symlink():
        raise SourceSessionError("session output must not be a symlink")
    _safe_parent(output_path)
    payload = {
        "version": SESSION_VERSION,
        "source": normalized,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "cookies": cookies,
        "headers": headers,
    }
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            os.fchmod(handle.fileno(), 0o600)
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, output_path)
        output_path.chmod(0o600)
    except OSError as exc:
        if temporary_name:
            try:
                Path(temporary_name).unlink(missing_ok=True)
            except OSError:
                pass
        raise SourceSessionError("authorized session could not be saved") from exc
    return source_session_status(normalized, output_path)


def _load_payload(source: str, path: Path) -> dict[str, Any] | None:
    _target(source)
    path = Path(path)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SourceSessionError("saved session must be a regular, non-symlink file")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise SourceSessionError("saved session permissions are too broad; require mode 600")
    if hasattr(os, "getuid") and info.st_uid != os.getuid():
        raise SourceSessionError("saved session is owned by another user")
    if info.st_size > MAX_SESSION_FILE_BYTES:
        raise SourceSessionError("saved session exceeds the 256 KiB safety limit")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SourceSessionError("saved session is unreadable or malformed") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("version") != SESSION_VERSION
        or payload.get("source") != source
        or not isinstance(payload.get("cookies"), list)
    ):
        raise SourceSessionError("saved session has an unsupported format")
    return payload


def source_session_cookie_header(source: str, path: Path) -> str | None:
    normalized = source.lower().replace("_", "-")
    target = _target(normalized)
    payload = _load_payload(normalized, path)
    if payload is None:
        return None
    now = datetime.now(timezone.utc).timestamp()
    candidates: list[dict[str, Any]] = []
    for item in payload["cookies"]:
        cookie = _cookie_from_mapping(item, host=target["host"])
        if cookie is None:
            continue
        if cookie["expires"] is not None and float(cookie["expires"]) <= now:
            continue
        if not target["request_path"].startswith(cookie["path"]):
            continue
        candidates.append(cookie)
    candidates.sort(key=lambda item: len(item["path"]), reverse=True)
    unique: dict[str, str] = {}
    for cookie in candidates:
        unique.setdefault(cookie["name"], cookie["value"])
    if not unique:
        raise SourceSessionError("saved session has no active cookies for the search endpoint")
    header = "; ".join(f"{name}={value}" for name, value in unique.items())
    if len(header.encode("utf-8")) > MAX_COOKIE_HEADER_BYTES:
        raise SourceSessionError("saved session cookie header exceeds the 32 KiB safety limit")
    return header


def source_session_request_headers(source: str, path: Path) -> dict[str, str]:
    normalized = source.lower().replace("_", "-")
    payload = _load_payload(normalized, path)
    if payload is None:
        return {}
    return _filtered_session_headers(payload.get("headers"))


def source_session_status(source: str, path: Path) -> dict[str, Any]:
    normalized = source.lower().replace("_", "-")
    _target(normalized)
    try:
        payload = _load_payload(normalized, path)
        if payload is None:
            return {"source": normalized, "state": "missing", "configured": False}
        header = source_session_cookie_header(normalized, path)
        return {
            "source": normalized,
            "state": "ready",
            "configured": bool(header),
            "cookie_count": len(payload["cookies"]),
            "request_header_names": sorted(
                source_session_request_headers(normalized, path)
            ),
            "created_at": payload.get("created_at"),
        }
    except SourceSessionError as exc:
        return {
            "source": normalized,
            "state": "invalid",
            "configured": False,
            "error": str(exc),
        }


def delete_source_session(source: str, path: Path) -> dict[str, Any]:
    normalized = source.lower().replace("_", "-")
    _target(normalized)
    target = Path(path)
    if target.is_symlink():
        raise SourceSessionError("session output must not be a symlink")
    try:
        target.unlink()
        removed = True
    except FileNotFoundError:
        removed = False
    except OSError as exc:
        raise SourceSessionError("authorized session could not be removed") from exc
    return {"source": normalized, "state": "missing", "configured": False, "removed": removed}
