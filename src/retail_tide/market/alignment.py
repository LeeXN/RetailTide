from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ..models import MarketBar
from ..time import SHANGHAI, as_utc
from .calendar import AShareTradingCalendar


@dataclass(frozen=True)
class EntryPoint:
    entry_at: datetime
    entry_price: float | None
    source: str


def align_daily_signal(
    signal_at: datetime,
    *,
    calendar: AShareTradingCalendar | None = None,
) -> EntryPoint:
    """Return the next safe daily entry, never a bar at/before the signal.

    A pre-open signal may use that day's open; a signal during the session,
    during lunch, or after close uses the next trading day's open.
    """
    calendar = calendar or AShareTradingCalendar()
    signal_at = as_utc(signal_at)
    assert signal_at is not None
    local_day = signal_at.astimezone(SHANGHAI).date()
    current = calendar.session(local_day)
    if current.is_open and current.open_at and current.close_at and signal_at < current.open_at:
        return EntryPoint(current.open_at, None, "next_trading_session_open")
    next_day = calendar.next_trading_day(local_day, 1)
    next_session = calendar.session(next_day)
    assert next_session.open_at is not None
    return EntryPoint(next_session.open_at, None, "next_trading_session_open")


def first_tradable_bar_after(signal_at: datetime, bars: list[MarketBar]) -> MarketBar | None:
    signal_at = as_utc(signal_at)
    assert signal_at is not None
    return min((bar for bar in bars if bar.ts > signal_at), key=lambda bar: bar.ts, default=None)
