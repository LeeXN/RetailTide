from __future__ import annotations

import gzip
import hashlib
import json
import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlsplit, urlunsplit

import httpx

from ..time import now_utc
from .base import ProbeResult, SourceError, html_to_text, public_get

COMMON_CRAWL_INDEX = "https://index.commoncrawl.org"
COMMON_CRAWL_DATA = "https://data.commoncrawl.org"


def canonical_url(value: str) -> str:
    parsed = urlsplit(str(value).strip())
    if not parsed.scheme or not parsed.netloc:
        return str(value).strip()
    host = parsed.hostname.lower() if parsed.hostname else parsed.netloc.lower()
    port = parsed.port
    if port and not ((parsed.scheme == "http" and port == 80) or (parsed.scheme == "https" and port == 443)):
        host = f"{host}:{port}"
    path = parsed.path or "/"
    query = "&".join(
        part
        for part in parsed.query.split("&")
        if part
        and not re.match(
            r"^(utm_[^=]+|spm|from|share_source)=", part, re.IGNORECASE
        )
    )
    return urlunsplit((parsed.scheme.lower(), host, path, query, ""))


@dataclass(frozen=True)
class ArchiveCapture:
    url: str
    crawl_id: str
    captured_at: datetime
    digest: str
    filename: str
    offset: int
    length: int
    status: int | None
    mime: str | None
    body: str | None = None
    body_truncated: bool = False


def _capture_datetime(value: str) -> datetime:
    text = str(value).strip()
    try:
        # CDX timestamps are UTC in YYYYMMDDhhmmss form.
        return datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(f"invalid Common Crawl timestamp: {value!r}") from exc


class CommonCrawlSource:
    source_type = "archive"

    def __init__(
        self,
        *,
        user_agent: str | None = None,
        min_interval: float = 1.0,
        use_fixture: bool = False,
        transport: httpx.AsyncBaseTransport | None = None,
        max_body_bytes: int = 5 * 1024 * 1024,
        **_kwargs,
    ):
        self.name = "common-crawl"
        self.user_agent = user_agent or "RetailTide/0.1 (Common Crawl enrichment)"
        self.min_interval = max(0.0, float(min_interval))
        self.use_fixture = use_fixture
        self.transport = transport
        self.max_body_bytes = max(1024, int(max_body_bytes))
        self._crawl_cache: tuple[float, list[str]] | None = None

    async def collect(self, *_args, **_kwargs):
        raise SourceError(
            "common-crawl is an archive enricher; collect existing content URLs through refresh"
        )

    async def _get_json(self, client: httpx.AsyncClient, url: str, **kwargs):
        response = await public_get(
            client,
            url,
            rate_source=self.name,
            min_interval=self.min_interval,
            **kwargs,
        )
        try:
            return response.json()
        except ValueError as exc:
            raise SourceError("Common Crawl response is not JSON") from exc

    async def crawl_ids(self) -> list[str]:
        if self.use_fixture:
            return ["CC-MAIN-FIXTURE"]
        if self._crawl_cache is not None:
            cached_at, values = self._crawl_cache
            if (now_utc().timestamp() - cached_at) < 86400:
                return values
        async with httpx.AsyncClient(
            timeout=30,
            headers={"Accept": "application/json", "User-Agent": self.user_agent},
            transport=self.transport,
        ) as client:
            payload = await self._get_json(client, f"{COMMON_CRAWL_INDEX}/collinfo.json")
        if not isinstance(payload, list):
            raise SourceError("Common Crawl collinfo response has an unexpected shape")
        values = [
            str(item.get("id"))
            for item in payload
            if isinstance(item, dict) and item.get("id")
        ]
        if not values:
            raise SourceError("Common Crawl returned no crawl indexes")
        self._crawl_cache = (now_utc().timestamp(), values)
        return values

    async def lookup_url(self, url: str, *, crawl_id: str | None = None) -> ArchiveCapture | None:
        target = canonical_url(url)
        crawl_id = crawl_id or (await self.crawl_ids())[0]
        if self.use_fixture:
            digest = hashlib.sha1(target.encode("utf-8")).hexdigest()
            return ArchiveCapture(
                url=target,
                crawl_id=crawl_id,
                captured_at=now_utc() - timedelta(hours=1),
                digest=digest,
                filename="fixture.warc.gz",
                offset=0,
                length=1,
                status=200,
                mime="text/html",
            )
        endpoint = f"{COMMON_CRAWL_INDEX}/{quote(crawl_id, safe='')}-index"
        params = {
            "url": target,
            "output": "json",
            # CDX auto matching treats punctuation in some source URLs (for
            # example Eastmoney's comma-separated post paths) as a pattern and
            # may reject the query with HTTP 400.  We only enrich known content
            # URLs, so an explicit exact lookup is both correct and cheaper.
            "matchType": "exact",
            "filter": ["status:200", "mime:text/html"],
            "collapse": "digest",
        }
        async with httpx.AsyncClient(
            timeout=30,
            headers={"Accept": "application/json", "User-Agent": self.user_agent},
            transport=self.transport,
        ) as client:
            try:
                response = await public_get(
                    client,
                    endpoint,
                    rate_source=self.name,
                    min_interval=self.min_interval,
                    params=params,
                )
            except httpx.HTTPStatusError as exc:
                # The CDX API uses 404 when this crawl has no matching capture.
                # That is an ordinary negative lookup, not a source failure.
                if exc.response.status_code == 404:
                    return None
                raise
        captures: list[ArchiveCapture] = []
        for line in response.text.splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                capture = ArchiveCapture(
                    url=str(item.get("url") or target),
                    crawl_id=crawl_id,
                    captured_at=_capture_datetime(str(item["timestamp"])),
                    digest=str(item.get("digest") or ""),
                    filename=str(item["filename"]),
                    offset=int(item["offset"]),
                    length=int(item["length"]),
                    status=int(item["status"]) if item.get("status") else None,
                    mime=str(item.get("mime") or "") or None,
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise SourceError("Common Crawl CDX response contains malformed capture") from exc
            captures.append(capture)
        if not captures:
            return None
        return max(captures, key=lambda item: item.captured_at)

    async def fetch_body(self, capture: ArchiveCapture) -> ArchiveCapture:
        if self.use_fixture:
            return replace(
                capture,
                body=f"Common Crawl archived snapshot for {capture.url}",
                body_truncated=False,
            )
        end = capture.offset + capture.length - 1
        url = f"{COMMON_CRAWL_DATA}/{capture.filename}"
        async with httpx.AsyncClient(
            timeout=60,
            headers={"Accept": "application/octet-stream", "User-Agent": self.user_agent},
            transport=self.transport,
        ) as client:
            response = await public_get(
                client,
                url,
                rate_source=self.name,
                min_interval=self.min_interval,
                headers={"Range": f"bytes={capture.offset}-{end}"},
            )
        compressed = response.content
        if len(compressed) > self.max_body_bytes * 2:
            raise SourceError("Common Crawl WARC range exceeds the configured size limit")
        try:
            record = gzip.decompress(compressed)
        except OSError as exc:
            raise SourceError("Common Crawl WARC range is not a valid gzip member") from exc
        first_end = record.find(b"\r\n\r\n")
        if first_end < 0:
            raise SourceError("Common Crawl WARC record has no header boundary")
        http_start = first_end + 4
        second_end = record.find(b"\r\n\r\n", http_start)
        if second_end < 0:
            raise SourceError("Common Crawl HTTP response has no header boundary")
        body = record[second_end + 4 :]
        truncated = len(body) > self.max_body_bytes
        body = body[: self.max_body_bytes]
        text = html_to_text(body.decode("utf-8", errors="replace"))
        return replace(capture, body=text, body_truncated=truncated)

    def probe(self) -> ProbeResult:
        return ProbeResult(
            source=self.name,
            source_type=self.source_type,
            source_role="archive-enrichment",
            checks={
                "transport": "commoncrawl-cdx-and-warc",
                "keyword_search": False,
                "known_url_lookup": True,
                "free_http_access": True,
                "body_format": "WARC HTTP Range",
            },
            notes=[
                "Common Crawl is queried only for URLs already discovered by content sources.",
                "Crawl availability is periodic and must not be treated as real-time publication time.",
            ],
        )
