from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

UTC = timezone.utc
SHANGHAI = ZoneInfo("Asia/Shanghai")


def as_utc(value: datetime | date | None, *, default: datetime | None = None) -> datetime | None:
    if value is None:
        return default
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, time.min)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def now_utc() -> datetime:
    return datetime.now(UTC)


def start_of_current_day_utc(value: datetime | None = None) -> datetime:
    """Return the current Asia/Shanghai calendar-day boundary in UTC."""

    value = as_utc(value) or now_utc()
    local = value.astimezone(SHANGHAI)
    return datetime.combine(local.date(), time.min, tzinfo=SHANGHAI).astimezone(UTC)


def scheduled_post_window(
    *,
    current: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Resolve the daily scheduler to the previous closed Shanghai date."""

    now = as_utc(current) or now_utc()
    today_start = start_of_current_day_utc(now)
    return today_start - timedelta(days=1), today_start


def resolve_collection_window(
    *,
    since: datetime | None = None,
    until: datetime | None = None,
    days: int | None = None,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Resolve an explicit or default collection window.

    No arguments means the current Asia/Shanghai calendar day up to now. A
    historical range must be explicit via ``days`` or ``since``/``until``.
    """

    current = as_utc(now) or now_utc()
    if days is not None:
        if days < 1:
            raise ValueError("days must be at least 1")
        if since is not None or until is not None:
            raise ValueError("days cannot be combined with since or until")
        start = current - timedelta(days=days)
        end = current
    elif since is None and until is None:
        start = start_of_current_day_utc(current)
        end = current
    elif since is None:
        raise ValueError("since is required when until is provided")
    else:
        start = as_utc(since)
        assert start is not None
        end = as_utc(until) or current

    if end > current:
        raise ValueError("until cannot be in the future")
    if start >= end:
        raise ValueError("collection window must have since before until")
    return start, end


def parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return as_utc(value)
    if isinstance(value, date):
        return as_utc(value)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(text, fmt).replace(tzinfo=SHANGHAI)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Unsupported datetime: {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    return as_utc(parsed)


def parse_collection_bound(value: Any, *, end: bool = False) -> datetime | None:
    """Parse CLI collection bounds using Shanghai date-only semantics.

    A date-only end is inclusive for users and therefore becomes the next
    Shanghai midnight internally. Full timestamps remain exact instants.
    """

    if value is None or value == "":
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        parsed = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=SHANGHAI)
        if end:
            parsed += timedelta(days=1)
        return parsed.astimezone(UTC)
    return parse_datetime(value)


def shanghai_datetime(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=SHANGHAI).astimezone(UTC)


def floor_bucket(value: datetime, bucket_size: str) -> datetime:
    value = as_utc(value)  # type: ignore[assignment]
    assert value is not None
    if bucket_size == "1h":
        return value.replace(minute=0, second=0, microsecond=0)
    if bucket_size == "1d":
        # Daily product metrics follow the user's market calendar rather than
        # UTC.  Store the Asia/Shanghai midnight as UTC so database timestamps
        # remain comparable while 00:00-23:59 local content stays in one day.
        local = value.astimezone(SHANGHAI)
        return datetime.combine(local.date(), time.min, tzinfo=SHANGHAI).astimezone(UTC)
    raise ValueError("bucket_size must be 1h or 1d")


def bucket_delta(bucket_size: str) -> timedelta:
    if bucket_size == "1h":
        return timedelta(hours=1)
    if bucket_size == "1d":
        return timedelta(days=1)
    raise ValueError("bucket_size must be 1h or 1d")
