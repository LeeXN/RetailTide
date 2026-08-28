from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta

from ..schemas import TradingSessionSchema
from ..time import SHANGHAI, UTC, as_utc

# Officially observed mainland China holiday dates used by the local V0
# calendar. The explicit set matters: weekdays alone are not a trading calendar.
CN_HOLIDAYS = {
    # 2024
    date.fromisoformat(day)
    for day in (
        "2024-01-01",
        "2024-02-09",
        "2024-02-12",
        "2024-02-13",
        "2024-02-14",
        "2024-02-15",
        "2024-02-16",
        "2024-04-04",
        "2024-04-05",
        "2024-05-01",
        "2024-05-02",
        "2024-05-03",
        "2024-06-10",
        "2024-09-16",
        "2024-09-17",
        "2024-10-01",
        "2024-10-02",
        "2024-10-03",
        "2024-10-04",
        "2024-10-07",
        # 2025
        "2025-01-01",
        "2025-01-28",
        "2025-01-29",
        "2025-01-30",
        "2025-01-31",
        "2025-04-04",
        "2025-05-01",
        "2025-05-02",
        "2025-05-05",
        "2025-05-31",
        "2025-06-02",
        "2025-10-01",
        "2025-10-02",
        "2025-10-03",
        "2025-10-06",
        "2025-10-07",
        "2025-10-08",
        # 2026 provisional public-holiday closure dates for V0 local runs
        "2026-01-01",
        "2026-02-16",
        "2026-02-17",
        "2026-02-18",
        "2026-04-06",
        "2026-05-01",
        "2026-06-19",
        "2026-09-25",
        "2026-10-01",
        "2026-10-02",
        "2026-10-05",
        "2026-10-06",
        "2026-10-07",
    )
}


class AShareTradingCalendar:
    market = "CN"

    def __init__(self, holidays: Iterable[date] | None = None):
        self.holidays = set(holidays or CN_HOLIDAYS)

    def is_open(self, trade_date: date) -> bool:
        return trade_date.weekday() < 5 and trade_date not in self.holidays

    def session(self, trade_date: date) -> TradingSessionSchema:
        if not self.is_open(trade_date):
            return TradingSessionSchema(
                market=self.market, trade_date=trade_date.isoformat(), is_open=False
            )
        open_at = datetime(
            trade_date.year, trade_date.month, trade_date.day, 9, 30, tzinfo=SHANGHAI
        ).astimezone(UTC)
        close_at = datetime(
            trade_date.year, trade_date.month, trade_date.day, 15, 0, tzinfo=SHANGHAI
        ).astimezone(UTC)
        return TradingSessionSchema(
            market=self.market,
            trade_date=trade_date.isoformat(),
            is_open=True,
            open_at=open_at,
            close_at=close_at,
            metadata={"morning_close": "11:30", "afternoon_open": "13:00"},
        )

    def sessions(self, start: date, end: date) -> list[TradingSessionSchema]:
        result = []
        current = start
        while current <= end:
            result.append(self.session(current))
            current += timedelta(days=1)
        return result

    def next_open_session(
        self, value: datetime, *, include_same_day_preopen: bool = True
    ) -> TradingSessionSchema:
        value = as_utc(value)
        assert value is not None
        current = value.astimezone(SHANGHAI).date()
        for _ in range(370):
            session = self.session(current)
            if session.is_open and session.open_at and session.close_at:
                if include_same_day_preopen and value < session.open_at:
                    return session
                if value < session.open_at:
                    return session
            current += timedelta(days=1)
        raise RuntimeError("no open A-share session within one year")

    def next_trading_day(self, trade_date: date, n: int = 1) -> date:
        current = trade_date
        found = 0
        while found < n:
            current += timedelta(days=1)
            if self.is_open(current):
                found += 1
        return current
