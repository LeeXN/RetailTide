from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta
from typing import Any

import httpx

from ..config import SourceCredential
from ..schemas import CollectResult
from ..time import SHANGHAI, UTC, as_utc, now_utc, parse_datetime
from .base import ProbeResult, SourceError, html_to_text, public_get, raw_from_mapping
from .fixture import FixtureSource

ZHIHU_SEARCH_URL = "https://developer.zhihu.com/api/v1/content/zhihu_search"
_MARKET_QUESTION_DATE = re.compile(
    r"如何看待(?:(?P<year>\d{4})年)?(?P<month>\d{1,2})月(?P<day>\d{1,2})日"
)


def zhihu_market_question_query(market_scope: str, until: datetime) -> tuple[str, date]:
    """Build a natural market-review question for the latest completed session."""

    local_until = (as_utc(until) or until).astimezone(SHANGHAI)
    session_date = local_until.date()
    # Before the A-share close, ask about the preceding completed session. A
    # date-only inclusive ``until`` lands at the next midnight and follows the
    # same rule naturally.
    if local_until.timetz().replace(tzinfo=None) < time(15):
        session_date -= timedelta(days=1)
    while session_date.weekday() >= 5:
        session_date -= timedelta(days=1)

    label = f"{session_date.year}年{session_date.month}月{session_date.day}日"
    normalized_scope = str(market_scope).strip().upper()
    subject = {
        "A股": "A股市场",
        "A股市场": "A股市场",
        "港股": "港股市场",
        "港股市场": "港股市场",
        "美股": "美股市场",
        "美股市场": "美股市场",
    }.get(normalized_scope)
    if subject is None:
        raise ValueError(f"unsupported Zhihu market scope: {market_scope!r}")
    return f"如何看待{label}{subject}行情走势？", session_date


def _market_question_session_date(query: str, observed_at: datetime) -> date | None:
    match = _MARKET_QUESTION_DATE.search(str(query))
    if match is None:
        return None
    year = int(match.group("year") or observed_at.astimezone(SHANGHAI).year)
    try:
        return date(year, int(match.group("month")), int(match.group("day")))
    except ValueError as exc:
        raise SourceError(f"zhihu market question has an invalid date: {query!r}") from exc


def _engagement_value(item: dict[str, Any], *fields: str) -> int:
    for field in fields:
        value = item.get(field)
        if value in (None, ""):
            continue
        try:
            return max(0, int(float(value)))
        except (TypeError, ValueError):
            continue
    return 0


def zhihu_answer_reference_eligibility(
    payload: dict[str, Any], *, session_date: date | None = None
) -> tuple[bool, str]:
    """Admit only answers that can be tied to the requested market session.

    Zhihu search may rank an old answer for a query that shares only the month
    and day.  An explicit matching year is strongest; a yearless title is
    accepted only when the official edit time is close to the target session.
    """

    if session_date is None:
        try:
            session_date = date.fromisoformat(str(payload.get("market_session_date") or ""))
        except ValueError:
            return False, "missing_market_session_date"
    text = " ".join(str(payload.get(field) or "") for field in ("title", "body"))
    month = session_date.month
    day = session_date.day
    if re.search(rf"(?<!\d)0?{month}月0?{day}日", text) is None:
        return False, "market_session_day_not_mentioned"

    explicit_years = {
        int(year)
        for year in re.findall(
            rf"(?<!\d)(20\d{{2}})年\s*0?{month}月\s*0?{day}日",
            text,
        )
    }
    explicit_years.update(
        int(year)
        for year in re.findall(
            rf"(?<!\d)(20\d{{2}})[-/]0?{month}[-/]0?{day}(?!\d)",
            text,
        )
    )
    if explicit_years:
        if explicit_years == {session_date.year}:
            return True, "explicit_market_session_date"
        return False, "conflicting_market_session_year"

    edited_at = parse_datetime(
        payload.get("answer_edit_time") or payload.get("updated_at")
    )
    if edited_at is not None:
        edit_date = edited_at.astimezone(SHANGHAI).date()
        if abs((edit_date - session_date).days) <= 7:
            return True, "recent_edit_matches_market_session"
    return False, "market_session_year_unverified"


def _zhihu_datetime(value: Any) -> datetime:
    try:
        timestamp = float(value)
        if timestamp > 100_000_000_000:
            timestamp /= 1000
        if timestamp <= 0:
            raise ValueError
        return datetime.fromtimestamp(timestamp, tz=UTC)
    except (TypeError, ValueError, OSError) as exc:
        raise SourceError(f"zhihu item has an invalid timestamp: {value!r}") from exc


def _observation_kind(content_type: Any) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(content_type or "content").lower()).strip("_")
    return f"zhihu_{normalized or 'content'}"


def _mapping_from_zhihu_item(
    item: dict[str, Any],
    *,
    query: str,
    created_at: datetime,
    updated_at: datetime,
    source_timestamp_field: str,
) -> dict[str, Any]:
    content_id = str(item.get("ContentID") or "").strip()
    content_type = str(item.get("ContentType") or "content").strip()
    if not content_id:
        raise SourceError("zhihu item has no stable ContentID")
    title = html_to_text(item.get("Title"))
    body = html_to_text(item.get("ContentText")) or title
    if not body:
        raise SourceError(f"zhihu item {content_id!r} has no title or content summary")

    comments = item.get("CommentInfoList")
    featured_comments = []
    if comments is not None:
        if not isinstance(comments, list):
            raise SourceError("zhihu CommentInfoList has an unexpected shape")
        featured_comments = [
            text
            for comment in comments
            if isinstance(comment, dict) and (text := html_to_text(comment.get("Content")))
        ]

    mapping: dict[str, Any] = {
        "id": f"{content_type.lower()}:{content_id}",
        "observation_kind": _observation_kind(content_type),
        "published_at": created_at.isoformat(),
        "updated_at": updated_at.isoformat(),
        "timestamp_semantics": "created",
        "source_timestamp_field": source_timestamp_field,
        "source_timezone": "unix_epoch_utc",
        "title": title or None,
        "body": body,
        "body_is_summary": True,
        "url": str(item.get("Url") or "").strip() or None,
        # The official API exposes only a display name, not a stable author ID.
        "author_name": html_to_text(item.get("AuthorName")) or None,
        "author_badge_text": html_to_text(item.get("AuthorBadgeText")) or None,
        "comments": item.get("CommentCount"),
        "likes": item.get("VoteUpCount"),
        "favorites": next(
            (
                item.get(field)
                for field in ("FavoriteCount", "CollectionCount", "CollectCount")
                if item.get(field) not in (None, "")
            ),
            None,
        ),
        "authority_level": str(item.get("AuthorityLevel"))
        if item.get("AuthorityLevel") not in (None, "")
        else None,
        "featured_comments": featured_comments or None,
        "content_type": content_type,
        "query": query,
        "source_role": "discovery",
        "language": "zh-CN",
    }
    return {key: value for key, value in mapping.items() if value is not None}


def _mapping_from_ranked_answer_snapshot(
    item: dict[str, Any],
    *,
    query: str,
    observed_at: datetime,
    updated_at: datetime,
    session_date: date,
    answer_rank: int,
) -> dict[str, Any]:
    """Map an official top-answer snapshot without inventing a publish time."""

    mapping = _mapping_from_zhihu_item(
        item,
        query=query,
        created_at=observed_at,
        updated_at=updated_at,
        source_timestamp_field="observation_time",
    )
    mapping.pop("published_at", None)
    mapping.update(
        {
            "observation_kind": "zhihu_answer_snapshot",
            "timestamp_semantics": "observed_rank_snapshot",
            "publication_time_verified": False,
            "market_session_date": session_date.isoformat(),
            "question_query": query,
            "answer_rank": answer_rank,
            "answer_rank_method": "official_top_results_then_vote_favorite_comment_desc",
            "answer_edit_time": updated_at.isoformat(),
        }
    )
    eligible, reason = zhihu_answer_reference_eligibility(
        mapping, session_date=session_date
    )
    mapping["reference_eligible"] = eligible
    mapping["reference_reason"] = reason
    return mapping


class ZhihuSource(FixtureSource):
    """Official Zhihu top-results search, admitted only as a discovery source."""

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        credential: SourceCredential | None = None,
        use_fixture: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
        count: int = 10,
        min_public_interval: float | None = None,
        **kwargs,
    ):
        super().__init__("zhihu", **kwargs)
        self.credential = credential or SourceCredential("zhihu")
        # The documented production endpoint is fixed. ``endpoint`` exists only
        # for deterministic tests and explicitly embedded deployments.
        self.endpoint = endpoint or ZHIHU_SEARCH_URL
        self.use_fixture = use_fixture
        self.transport = transport
        self.count = max(1, min(10, count))
        self.min_public_interval = (
            1.0
            if min_public_interval is None and transport is None
            else float(min_public_interval or 0)
        )

    async def collect(self, query, since, cursor=None, *, until=None) -> CollectResult:
        if self.use_fixture:
            return await super().collect(query, since, cursor, until=until)
        if cursor:
            raise SourceError("zhihu search does not support pagination cursors")
        if not self.credential.access_token:
            raise SourceError("zhihu is not configured: Access Secret is required")

        try:
            async with httpx.AsyncClient(
                timeout=15,
                headers=self.credential.headers(),
                # Keep a single retry owner so an unavailable API fails within
                # the documented public_get budget instead of multiplying it.
                transport=self.transport,
            ) as client:
                response = await public_get(
                    client,
                    self.endpoint,
                    rate_source=self.name,
                    min_interval=self.min_public_interval,
                    params={"Query": query, "Count": self.count},
                )
                payload = response.json()
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            raise SourceError(f"zhihu collection failed: {exc}") from exc

        if not isinstance(payload, dict):
            raise SourceError("zhihu response has an unexpected shape")
        try:
            code = int(payload.get("Code"))
        except (TypeError, ValueError) as exc:
            raise SourceError("zhihu response has no valid business status code") from exc
        if code != 0:
            message = html_to_text(payload.get("Message")) or "unknown error"
            raise SourceError(f"zhihu API rejected the request (code={code}): {message}")

        data = payload.get("Data")
        if not isinstance(data, dict) or not isinstance(data.get("Items"), list):
            raise SourceError("zhihu response Data.Items has an unexpected shape")
        if data.get("HasMore") is True:
            # The documented API currently has no pagination input. Failing is
            # safer than silently dropping an undiscoverable next page.
            raise SourceError("zhihu reported more results but exposes no pagination input")

        records = data["Items"]
        if not all(isinstance(item, dict) for item in records):
            raise SourceError("zhihu response contains a malformed search item")
        observed_at = as_utc(self.clock()) or now_utc()
        items = []
        warnings: list[str] = []
        market_session_date = _market_question_session_date(query, observed_at)
        if market_session_date is not None:
            answer_records = [
                item
                for item in records
                if str(item.get("ContentType") or "").strip().lower() == "answer"
            ]
            answer_records.sort(
                key=lambda item: (
                    _engagement_value(item, "VoteUpCount"),
                    _engagement_value(item, "FavoriteCount", "CollectionCount", "CollectCount"),
                    _engagement_value(item, "CommentCount"),
                ),
                reverse=True,
            )
            if not answer_records:
                warnings.append("official top results contained no answers for the market question")
            for answer_rank, item in enumerate(answer_records, start=1):
                updated_at = _zhihu_datetime(item.get("EditTime"))
                mapping = _mapping_from_ranked_answer_snapshot(
                    item,
                    query=query,
                    observed_at=observed_at,
                    updated_at=updated_at,
                    session_date=market_session_date,
                    answer_rank=answer_rank,
                )
                if not mapping["reference_eligible"]:
                    warnings.append(
                        f"excluded answer {mapping['id']} from reference analysis: "
                        f"{mapping['reference_reason']}"
                    )
                items.append(
                    raw_from_mapping(
                        self.name,
                        mapping,
                        observation_kind="zhihu_answer_snapshot",
                        observed_at=observed_at,
                    )
                )
            return CollectResult(items=items, exhausted=True, warnings=warnings)

        # Official search guarantees only EditTime, documented as publication
        # or update time. It must never be treated as first publication. An item
        # enters date-sensitive storage only if the official payload explicitly
        # adds an unambiguous creation field.
        for item in records:
            content_type = str(item.get("ContentType") or "content").strip().lower()
            updated_at = _zhihu_datetime(item.get("EditTime"))
            created_value = (
                item.get("CreateTime") or item.get("CreatedTime") or item.get("PublishTime")
            )
            source_timestamp_field = next(
                (
                    field
                    for field in ("CreateTime", "CreatedTime", "PublishTime")
                    if item.get(field) not in (None, "")
                ),
                "CreateTime",
            )
            if created_value in (None, ""):
                stable_id = str(item.get("ContentID") or "unknown")
                warnings.append(
                    f"skipped {content_type} {stable_id}: official search only returned EditTime"
                )
                continue
            created_at = _zhihu_datetime(created_value)
            mapping = _mapping_from_zhihu_item(
                item,
                query=query,
                created_at=created_at,
                updated_at=updated_at,
                source_timestamp_field=source_timestamp_field,
            )
            items.append(
                raw_from_mapping(
                    self.name,
                    mapping,
                    observation_kind="zhihu_content",
                    observed_at=observed_at,
                )
            )
        return CollectResult(items=items, exhausted=True, warnings=warnings)

    def probe(self) -> ProbeResult:
        return ProbeResult(
            source=self.name,
            source_type=self.source_type,
            source_role="discovery",
            checks={
                "transport": "official-zhihu-search-api",
                "official_endpoint_built_in": True,
                "access_secret_required": True,
                "keyword_search": True,
                "cursor_pagination": False,
                "single_request_max_results": 10,
                "stable_content_id": True,
                "publication_time": "verified explicit creation field only",
                "edit_time_never_used_as_publication_time": True,
                "body": "summary",
                "stable_author_id": False,
                "vote_count": True,
                "favorite_count": "retained when the official payload exposes it",
                "comment_count": True,
                "historical_depth": "top-results only",
                "time_filter": False,
                "top_results_only": True,
                "market_question_mode": "rank answers by vote, favorite and comment counts",
            },
            notes=[
                (
                    "Official API exposes a fixed endpoint and top 10 results only; it requires "
                    "a Bearer Access Secret and remains discovery-only."
                ),
                (
                    "The documented EditTime may be publication or update time; ambiguous rows "
                    "are excluded from date-sensitive storage."
                ),
                (
                    "Date-specific market questions retain answer snapshots at observation time; "
                    "the target session date is explicit and never presented as answer publish time."
                ),
            ],
        )
