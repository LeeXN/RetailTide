from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta
from itertools import pairwise
from typing import Any, Protocol
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Asset, MarketBar, TradingSession
from ..schemas import Bar
from ..time import UTC, now_utc, parse_datetime
from .calendar import AShareTradingCalendar


class MarketProvider(Protocol):
    name: str

    def daily_bars(self, asset: Asset, start: date, end: date) -> list[Bar]: ...

    def minute_bars(self, asset: Asset, start: datetime, end: datetime) -> list[Bar]: ...

    def trading_calendar(self, market: str, start: date, end: date): ...


class SyntheticAShareProvider:
    """Deterministic market fixture for local research and tests.

    It is deliberately not presented as live prices. A real provider can be
    plugged in behind the MarketProvider protocol without changing alignment or
    return calculations.
    """

    name = "synthetic-a-share"

    def __init__(self, calendar: AShareTradingCalendar | None = None):
        self.calendar = calendar or AShareTradingCalendar()

    def trading_calendar(self, market: str, start: date, end: date):
        return self.calendar.sessions(start, end)

    def daily_bars(self, asset: Asset, start: date, end: date) -> list[Bar]:
        base = 100.0 if asset.symbol == "000001" else 20.0 if asset.symbol == "518880" else 50.0
        bars = []
        index = 0
        for session in self.calendar.sessions(start, end):
            if not session.is_open or session.open_at is None:
                continue
            # Stable, non-random variation makes examples reproducible and gives
            # research enough movement without pretending to be market data.
            close = base * (1 + 0.0015 * index + 0.012 * math.sin(index / 3.7))
            open_price = close * (1 - 0.002 * math.cos(index / 5.0))
            high = max(open_price, close) * 1.006
            low = min(open_price, close) * 0.994
            bars.append(
                Bar(
                    ts=session.open_at,
                    open=round(open_price, 6),
                    high=round(high, 6),
                    low=round(low, 6),
                    close=round(close, 6),
                    volume=100000 + index * 1000,
                    amount=close * (100000 + index * 1000),
                )
            )
            index += 1
        return bars

    def minute_bars(self, asset: Asset, start: datetime, end: datetime) -> list[Bar]:
        # V0 stores daily bars by default; this explicit empty implementation
        # prevents accidental synthetic minute precision.
        return []


def _first(item: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if item.get(key) is not None:
            return item[key]
    return default


def _records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        raise TypeError("market response must be an object or list")
    records = _first(payload, "bars", "items", "data", "results", default=[])
    if isinstance(records, dict):
        records = _first(records, "bars", "items", "results", default=[])
    if not isinstance(records, list):
        raise TypeError("market response bars must be a list")
    return [item for item in records if isinstance(item, dict)]


class HttpJsonMarketProvider:
    """Authorized JSON market adapter with a deliberately small contract.

    The provider expects an endpoint that accepts ``symbol``, ``market``,
    ``from``, ``to`` and ``interval`` query parameters and returns bars under
    ``bars``, ``items``, ``data`` or ``results``. This keeps provider-specific
    credentials and transport outside return alignment and research code.
    """

    name = "http-json"

    def __init__(
        self,
        endpoint: str,
        *,
        headers: dict[str, str] | None = None,
        calendar: AShareTradingCalendar | None = None,
    ):
        self.endpoint = endpoint
        self.headers = headers or {"Accept": "application/json"}
        self.calendar = calendar or AShareTradingCalendar()

    def trading_calendar(self, market: str, start: date, end: date):
        return self.calendar.sessions(start, end)

    def _fetch(
        self, asset: Asset, start: datetime | date, end: datetime | date, interval: str
    ) -> list[dict[str, Any]]:
        start_value = start.date().isoformat() if isinstance(start, datetime) else start.isoformat()
        end_value = end.date().isoformat() if isinstance(end, datetime) else end.isoformat()
        params = {
            "symbol": asset.symbol,
            "market": asset.market,
            "from": start_value,
            "to": end_value,
            "interval": interval,
        }
        try:
            with httpx.Client(timeout=20, headers=self.headers) as client:
                response = client.get(self.endpoint, params=params)
                response.raise_for_status()
                return _records(response.json())
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            raise ValueError(f"market provider request failed: {exc}") from exc

    def daily_bars(self, asset: Asset, start: date, end: date) -> list[Bar]:
        bars = []
        for item in self._fetch(asset, start, end, "1d"):
            timestamp = parse_datetime(_first(item, "ts", "timestamp", "datetime", "time", "date"))
            if timestamp is None:
                raise ValueError("market bar has no timestamp")
            try:
                bars.append(
                    Bar(
                        ts=timestamp,
                        open=float(_first(item, "open", "o")),
                        high=float(_first(item, "high", "h")),
                        low=float(_first(item, "low", "l")),
                        close=float(_first(item, "close", "c")),
                        volume=float(_first(item, "volume", "v", default=0)),
                        amount=float(_first(item, "amount", "turnover", "a", default=0)),
                        adjustment=str(_first(item, "adjustment", "adj", default="none")),
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(f"market bar has invalid OHLC fields: {item}") from exc
        return bars

    def minute_bars(self, asset: Asset, start: datetime, end: datetime) -> list[Bar]:
        return []


class EastmoneyMarketProvider:
    """Public daily-bar adapter for registered CN and US representative assets.

    The endpoint does not require user credentials. Returned provider metadata
    is stored on every MarketBar so the dashboard never presents it as an
    exchange-certified or synthetic series.
    """

    name = "eastmoney"
    endpoint = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

    def __init__(self, calendar: AShareTradingCalendar | None = None):
        self.calendar = calendar or AShareTradingCalendar()

    def trading_calendar(self, market: str, start: date, end: date):
        return self.calendar.sessions(start, end)

    @staticmethod
    def _secid(asset: Asset) -> str:
        symbol = asset.symbol.upper()
        market = asset.market.upper()
        if market == "US":
            # Eastmoney uses market 100 for major US indexes and 105 for
            # Nasdaq-listed equities.
            prefix = "100" if asset.asset_type == "index" else "105"
            return f"{prefix}.{symbol}"
        if market == "HK":
            return f"116.{symbol.lstrip('0') or '0'}"
        if market != "CN":
            raise ValueError(f"eastmoney provider does not support market {asset.market}")
        is_shanghai = asset.asset_type == "index" and symbol.startswith("000")
        is_shanghai = is_shanghai or symbol.startswith(("5", "6", "9"))
        return f"{'1' if is_shanghai else '0'}.{symbol}"

    @staticmethod
    def _bar_time(asset: Asset, value: str) -> datetime:
        trade_date = date.fromisoformat(value)
        timezone = ZoneInfo(asset.timezone or "UTC")
        return datetime.combine(trade_date, time(9, 30), timezone).astimezone(UTC)

    def daily_bars(self, asset: Asset, start: date, end: date) -> list[Bar]:
        params = {
            "secid": self._secid(asset),
            "klt": "101",
            "fqt": "1",
            "beg": start.strftime("%Y%m%d"),
            "end": end.strftime("%Y%m%d"),
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        }
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; RetailTide/0.1)",
        }
        try:
            with httpx.Client(timeout=20, headers=headers) as client:
                response = client.get(self.endpoint, params=params)
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ValueError(f"eastmoney market request failed: {exc}") from exc
        data = payload.get("data") if isinstance(payload, dict) else None
        lines = data.get("klines") if isinstance(data, dict) else None
        if lines is None:
            message = payload.get("message") if isinstance(payload, dict) else None
            raise ValueError(message or f"eastmoney returned no bars for {asset.symbol}")
        bars = []
        for line in lines:
            fields = str(line).split(",")
            if len(fields) < 7:
                continue
            try:
                bars.append(
                    Bar(
                        ts=self._bar_time(asset, fields[0]),
                        open=float(fields[1]),
                        close=float(fields[2]),
                        high=float(fields[3]),
                        low=float(fields[4]),
                        volume=float(fields[5] or 0),
                        amount=float(fields[6] or 0),
                        adjustment="forward",
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(f"eastmoney returned an invalid bar: {line}") from exc
        return bars

    def minute_bars(self, asset: Asset, start: datetime, end: datetime) -> list[Bar]:
        return []


class TencentMarketProvider:
    """Public no-key daily bars for representative CN, HK and US assets."""

    name = "tencent"
    endpoint = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"

    def __init__(
        self,
        calendar: AShareTradingCalendar | None = None,
        transport: httpx.BaseTransport | None = None,
    ):
        self.calendar = calendar or AShareTradingCalendar()
        self.transport = transport

    def trading_calendar(self, market: str, start: date, end: date):
        return self.calendar.sessions(start, end)

    @staticmethod
    def _code(asset: Asset) -> str:
        symbol = asset.symbol.upper()
        market = asset.market.upper()
        if market == "US":
            return f"us.{symbol}"
        if market == "HK":
            return f"hk{symbol.zfill(5)}"
        if market != "CN":
            raise ValueError(f"tencent provider does not support market {asset.market}")
        is_shanghai = asset.asset_type == "index" and symbol.startswith("000")
        is_shanghai = is_shanghai or symbol.startswith(("5", "6", "9"))
        return f"{'sh' if is_shanghai else 'sz'}{symbol}"

    @staticmethod
    def _bar_time(asset: Asset, value: str) -> datetime:
        trade_date = date.fromisoformat(value)
        timezone = ZoneInfo(asset.timezone or "UTC")
        return datetime.combine(trade_date, time(9, 30), timezone).astimezone(UTC)

    def daily_bars(self, asset: Asset, start: date, end: date) -> list[Bar]:
        code = self._code(asset)
        limit = min(1000, max(60, (end - start).days + 20))
        params = {"param": f"{code},day,{start.isoformat()},{end.isoformat()},{limit},qfq"}
        try:
            with httpx.Client(timeout=20, transport=self.transport) as client:
                response = client.get(self.endpoint, params=params)
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ValueError(f"tencent market request failed: {exc}") from exc
        data = payload.get("data") if isinstance(payload, dict) else None
        result = data.get(code) if isinstance(data, dict) else None
        if not isinstance(result, dict):
            message = payload.get("msg") if isinstance(payload, dict) else None
            raise TypeError(message or f"tencent returned no bars for {asset.symbol}")
        adjustment = "forward" if result.get("qfqday") else "none"
        lines = result.get("qfqday") or result.get("day") or []
        bars = []
        for fields in lines:
            if not isinstance(fields, list) or len(fields) < 6:
                continue
            if not (start.isoformat() <= str(fields[0]) <= end.isoformat()):
                continue
            try:
                bars.append(
                    Bar(
                        ts=self._bar_time(asset, str(fields[0])),
                        open=float(fields[1]),
                        close=float(fields[2]),
                        high=float(fields[3]),
                        low=float(fields[4]),
                        volume=float(fields[5] or 0),
                        amount=0,
                        adjustment=adjustment,
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(f"tencent returned an invalid bar: {fields}") from exc
        if not bars:
            raise ValueError(f"tencent returned no bars for {asset.symbol} in requested period")
        bars.sort(key=lambda item: item.ts)
        latest_trade_date = bars[-1].ts.astimezone(ZoneInfo(asset.timezone or "UTC")).date()
        if (end - start).days > 7 and latest_trade_date < end:
            # Tencent's long-range cache can lag its short-range cache by one
            # session for ETFs. Merge a bounded tail request so a 120-day sync
            # does not silently omit the newest completed trading day.
            tail_start = max(start, end - timedelta(days=7))
            try:
                tail = self.daily_bars(asset, tail_start, end)
            except (TypeError, ValueError):
                tail = []
            bars = sorted({bar.ts: bar for bar in [*bars, *tail]}.values(), key=lambda item: item.ts)
        lower, upper = (0.5, 1.5) if asset.asset_type in {"index", "etf"} else (0.1, 10.0)
        for previous, current in pairwise(bars):
            if previous.close <= 0 or current.close <= 0:
                raise ValueError(f"tencent returned a non-positive close for {asset.symbol}")
            ratio = current.close / previous.close
            if not lower <= ratio <= upper:
                raise ValueError(
                    "tencent returned a discontinuous close for "
                    f"{asset.symbol}: {previous.close:g} -> {current.close:g}; "
                    "possible provider symbol collision"
                )
        return bars

    def minute_bars(self, asset: Asset, start: datetime, end: datetime) -> list[Bar]:
        return []


class NasdaqMarketProvider:
    """Official Nasdaq daily history for registered US representative assets."""

    name = "nasdaq"
    endpoint_template = "https://api.nasdaq.com/api/quote/{symbol}/historical"

    def __init__(
        self,
        calendar: AShareTradingCalendar | None = None,
        transport: httpx.BaseTransport | None = None,
    ):
        self.calendar = calendar or AShareTradingCalendar()
        self.transport = transport

    def trading_calendar(self, market: str, start: date, end: date):
        return self.calendar.sessions(start, end)

    @staticmethod
    def _asset_class(asset: Asset) -> str:
        return {
            "etf": "etf",
            "index": "index",
            "stock": "stocks",
        }.get(asset.asset_type.lower(), "stocks")

    @staticmethod
    def _number(value: Any, *, default: float | None = None) -> float:
        normalized = str(value or "").strip().replace(",", "").replace("$", "")
        if not normalized or normalized.lower() in {"n/a", "na", "--"}:
            if default is not None:
                return default
            raise ValueError("market value is missing")
        return float(normalized)

    @staticmethod
    def _bar_time(asset: Asset, value: str) -> datetime:
        month, day, year = (int(part) for part in value.split("/"))
        trade_date = date(year, month, day)
        timezone = ZoneInfo(asset.timezone or "UTC")
        return datetime.combine(trade_date, time(9, 30), timezone).astimezone(UTC)

    def daily_bars(self, asset: Asset, start: date, end: date) -> list[Bar]:
        if asset.market.upper() != "US":
            raise ValueError(f"nasdaq provider does not support market {asset.market}")
        params = {
            "assetclass": self._asset_class(asset),
            "fromdate": start.isoformat(),
            "todate": end.isoformat(),
            "limit": "5000",
        }
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.nasdaq.com",
            "Referer": "https://www.nasdaq.com/",
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "Chrome/139.0 Safari/537.36"
            ),
        }
        endpoint = self.endpoint_template.format(symbol=asset.symbol.upper())
        try:
            with httpx.Client(timeout=20, headers=headers, transport=self.transport) as client:
                response = client.get(endpoint, params=params)
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ValueError(f"nasdaq market request failed: {exc}") from exc
        status = payload.get("status") if isinstance(payload, dict) else None
        if isinstance(status, dict) and status.get("rCode") not in {None, 200}:
            raise ValueError(f"nasdaq market request failed: {status.get('bCodeMessage')}")
        data = payload.get("data") if isinstance(payload, dict) else None
        table = data.get("tradesTable") if isinstance(data, dict) else None
        rows = table.get("rows") if isinstance(table, dict) else None
        if not isinstance(rows, list):
            raise TypeError(f"nasdaq returned no bars for {asset.symbol}")
        bars = []
        for row in rows:
            if not isinstance(row, dict) or not row.get("date"):
                continue
            try:
                timestamp = self._bar_time(asset, str(row["date"]))
                trade_date = timestamp.astimezone(ZoneInfo(asset.timezone or "UTC")).date()
                if not start <= trade_date <= end:
                    continue
                bars.append(
                    Bar(
                        ts=timestamp,
                        open=self._number(row.get("open")),
                        high=self._number(row.get("high")),
                        low=self._number(row.get("low")),
                        close=self._number(row.get("close")),
                        volume=self._number(row.get("volume"), default=0),
                        amount=0,
                        adjustment="none",
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(f"nasdaq returned an invalid bar: {row}") from exc
        if not bars:
            raise ValueError(f"nasdaq returned no bars for {asset.symbol} in requested period")
        bars.sort(key=lambda item: item.ts)
        return bars

    def minute_bars(self, asset: Asset, start: datetime, end: datetime) -> list[Bar]:
        return []


class PublicMarketProvider:
    """Route public market data to a reliable, market-specific upstream."""

    name = "public"

    def __init__(
        self,
        *,
        cn_provider: MarketProvider | None = None,
        us_provider: MarketProvider | None = None,
    ):
        self.cn_provider = cn_provider or TencentMarketProvider()
        self.us_provider = us_provider or NasdaqMarketProvider()

    def _delegate(self, market: str) -> MarketProvider:
        return self.us_provider if market.upper() == "US" else self.cn_provider

    def provider_name(self, asset: Asset) -> str:
        return self._delegate(asset.market).name

    def trading_calendar(self, market: str, start: date, end: date):
        return self._delegate(market).trading_calendar(market, start, end)

    def daily_bars(self, asset: Asset, start: date, end: date) -> list[Bar]:
        return self._delegate(asset.market).daily_bars(asset, start, end)

    def minute_bars(self, asset: Asset, start: datetime, end: datetime) -> list[Bar]:
        return self._delegate(asset.market).minute_bars(asset, start, end)


def provider_for_name(
    name: str | None,
    *,
    endpoint: str | None = None,
    headers: dict[str, str] | None = None,
) -> MarketProvider:
    if not name:
        raise ValueError("market provider must be configured explicitly")
    normalized = name.lower()
    if normalized in {"synthetic-a-share", "fixture", "demo"}:
        return SyntheticAShareProvider()
    if normalized in {"http-json", "real"}:
        if not endpoint:
            raise ValueError("http-json market provider requires an endpoint")
        return HttpJsonMarketProvider(endpoint, headers=headers)
    if normalized == "eastmoney":
        return EastmoneyMarketProvider()
    if normalized in {"tencent", "qq"}:
        return TencentMarketProvider()
    if normalized == "nasdaq":
        return NasdaqMarketProvider()
    if normalized == "public":
        return PublicMarketProvider()
    raise ValueError(f"unknown market provider: {name}")


def provider_name_for_asset(name: str, asset: Asset) -> str:
    """Resolve a configured route to the upstream name persisted on each bar."""

    normalized = name.lower()
    if normalized == "public":
        return "nasdaq" if asset.market.upper() == "US" else "tencent"
    return {
        "qq": "tencent",
        "fixture": "synthetic-a-share",
        "demo": "synthetic-a-share",
        "real": "http-json",
    }.get(normalized, normalized)


def _daily_bar_is_closed(asset: Asset, bar: Bar, *, as_of: datetime) -> bool:
    """Reject a provider's in-progress daily candle before the local close."""

    timezone = ZoneInfo(asset.timezone or "UTC")
    trade_date = bar.ts.astimezone(timezone).date()
    close_time = {
        "CN": time(15, 0),
        "HK": time(16, 0),
        "US": time(16, 0),
    }.get(asset.market.upper(), time(23, 59, 59))
    close_at = datetime.combine(trade_date, close_time, timezone).astimezone(UTC)
    return as_of >= close_at


def sync_market(
    session: Session,
    asset: Asset,
    start: date,
    end: date,
    *,
    provider: MarketProvider | None = None,
    interval: str = "1d",
    as_of: datetime | None = None,
) -> int:
    if provider is None:
        raise ValueError("market provider must be configured explicitly")
    for calendar_session in provider.trading_calendar(asset.market, start, end):
        trade_date = date.fromisoformat(calendar_session.trade_date)
        existing_session = session.scalar(
            select(TradingSession).where(
                TradingSession.market == asset.market, TradingSession.trade_date == trade_date
            )
        )
        if existing_session is None:
            session.add(
                TradingSession(
                    market=asset.market,
                    trade_date=trade_date,
                    is_open=calendar_session.is_open,
                    open_at=calendar_session.open_at,
                    close_at=calendar_session.close_at,
                    metadata_json=calendar_session.metadata,
                )
            )
    bars = provider.daily_bars(asset, start, end) if interval == "1d" else []
    current = as_of or now_utc()
    bars = [bar for bar in bars if _daily_bar_is_closed(asset, bar, as_of=current)]
    provider_name_resolver = getattr(provider, "provider_name", None)
    stored_provider = (
        provider_name_resolver(asset)
        if callable(provider_name_resolver)
        else provider_name_for_asset(provider.name, asset)
    )
    inserted = 0
    for bar in bars:
        existing = session.scalar(
            select(MarketBar).where(
                MarketBar.asset_id == asset.id,
                MarketBar.interval == interval,
                MarketBar.ts == bar.ts,
                MarketBar.provider == stored_provider,
            )
        )
        if existing is None:
            session.add(
                MarketBar(
                    asset_id=asset.id,
                    interval=interval,
                    ts=bar.ts,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    amount=bar.amount,
                    adjustment=bar.adjustment,
                    provider=stored_provider,
                )
            )
            inserted += 1
    session.commit()
    return inserted
