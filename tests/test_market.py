from __future__ import annotations

from datetime import date, datetime

import httpx
import pytest
from sqlalchemy import select

from retail_tide.market import (
    AShareTradingCalendar,
    NasdaqMarketProvider,
    PublicMarketProvider,
    SyntheticAShareProvider,
    TencentMarketProvider,
    align_daily_signal,
    provider_name_for_asset,
    sync_market,
)
from retail_tide.models import Asset, AssetTopic, MarketBar, Topic
from retail_tide.registry import sync_registry
from retail_tide.time import SHANGHAI, UTC


def local(day: str, hour: int, minute: int = 0):
    return (
        datetime.fromisoformat(f"{day}T{hour:02d}:{minute:02d}:00")
        .replace(tzinfo=SHANGHAI)
        .astimezone(UTC)
    )


def test_calendar_handles_weekend_holiday_and_sessions():
    calendar = AShareTradingCalendar()
    assert not calendar.is_open(datetime.fromisoformat("2024-10-01").date())
    assert not calendar.is_open(datetime.fromisoformat("2024-01-06").date())
    assert calendar.is_open(datetime.fromisoformat("2024-01-08").date())
    assert (
        calendar.session(datetime.fromisoformat("2024-01-08").date()).metadata["afternoon_open"]
        == "13:00"
    )


def test_daily_alignment_has_no_lookahead():
    calendar = AShareTradingCalendar()
    assert align_daily_signal(local("2024-01-08", 9), calendar=calendar).entry_at == local(
        "2024-01-08", 9, 30
    )
    assert align_daily_signal(local("2024-01-08", 10), calendar=calendar).entry_at == local(
        "2024-01-09", 9, 30
    )
    assert align_daily_signal(local("2024-01-08", 12), calendar=calendar).entry_at == local(
        "2024-01-09", 9, 30
    )
    assert align_daily_signal(local("2024-01-06", 10), calendar=calendar).entry_at == local(
        "2024-01-08", 9, 30
    )
    assert align_daily_signal(local("2024-10-01", 10), calendar=calendar).entry_at == local(
        "2024-10-08", 9, 30
    )


def test_tencent_provider_parses_public_daily_bars_without_credentials():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["param"].startswith(
            "sh518880,day,2026-08-13,2026-08-14,"
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "",
                "data": {
                    "sh518880": {
                        "qfqday": [
                            ["2026-08-13", "9.126", "9.042", "9.144", "9.016", "100"],
                            ["2026-08-14", "8.906", "8.946", "8.949", "8.891", "120"],
                        ]
                    }
                },
            },
        )

    asset = Asset(
        market="CN",
        symbol="518880",
        name="黄金ETF",
        asset_type="etf",
        currency="CNY",
        timezone="Asia/Shanghai",
    )
    provider = TencentMarketProvider(transport=httpx.MockTransport(handler))
    bars = provider.daily_bars(asset, date(2026, 8, 13), date(2026, 8, 14))

    assert len(bars) == 2
    assert bars[-1].open == 8.906
    assert bars[-1].close == 8.946
    assert bars[-1].high == 8.949
    assert bars[-1].low == 8.891
    assert bars[-1].adjustment == "forward"
    assert bars[-1].ts == local("2026-08-14", 9, 30)


def test_tencent_provider_uses_distinct_market_codes():
    assert TencentMarketProvider._code(
        Asset(market="CN", symbol="159992", asset_type="etf")
    ) == "sz159992"
    assert TencentMarketProvider._code(
        Asset(market="CN", symbol="000001", asset_type="index")
    ) == "sh000001"
    assert TencentMarketProvider._code(
        Asset(market="US", symbol="NDX", asset_type="index")
    ) == "us.NDX"


def test_tencent_provider_rejects_symbol_collision_discontinuity():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "us.NDX": {
                        "day": [
                            ["2026-08-14", "30167.13", "30046.14", "30179.82", "29934.66", "1"],
                            ["2026-08-17", "13.09", "13.07", "13.12", "13.02", "1"],
                        ]
                    }
                },
            },
        )

    asset = Asset(
        market="US",
        symbol="NDX",
        name="Nasdaq 100",
        asset_type="index",
        currency="USD",
        timezone="America/New_York",
    )
    provider = TencentMarketProvider(transport=httpx.MockTransport(handler))

    with pytest.raises(ValueError, match="possible provider symbol collision"):
        provider.daily_bars(asset, date(2026, 8, 14), date(2026, 8, 18))


def test_tencent_provider_merges_recent_tail_when_long_range_cache_lags():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        param = request.url.params["param"]
        requests.append(param)
        lines = [["2026-08-20", "9.2", "9.3", "9.4", "9.1", "100"]]
        if ",2026-08-14,2026-08-21," in param:
            lines.append(["2026-08-21", "9.3", "9.4", "9.5", "9.2", "120"])
        return httpx.Response(
            200,
            json={"code": 0, "msg": "", "data": {"sh518880": {"qfqday": lines}}},
        )

    asset = Asset(
        market="CN",
        symbol="518880",
        name="黄金ETF",
        asset_type="etf",
        currency="CNY",
        timezone="Asia/Shanghai",
    )
    provider = TencentMarketProvider(transport=httpx.MockTransport(handler))
    bars = provider.daily_bars(asset, date(2026, 7, 22), date(2026, 8, 21))

    assert len(requests) == 2
    assert len(bars) == 2
    assert bars[-1].ts == local("2026-08-21", 9, 30)
    assert bars[-1].close == 9.4


def test_nasdaq_provider_parses_official_daily_history():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/quote/QQQ/historical"
        assert request.url.params["assetclass"] == "etf"
        assert request.url.params["fromdate"] == "2026-07-22"
        assert request.url.params["todate"] == "2026-08-21"
        return httpx.Response(
            200,
            json={
                "data": {
                    "tradesTable": {
                        "rows": [
                            {
                                "date": "08/20/2026",
                                "close": "710.93",
                                "volume": "33,396,420",
                                "open": "712.09",
                                "high": "714.94",
                                "low": "708.52",
                            },
                            {
                                "date": "07/22/2026",
                                "close": "$705.35",
                                "volume": "23,692,180",
                                "open": "$703.62",
                                "high": "$709.65",
                                "low": "$703.62",
                            },
                        ]
                    }
                },
                "status": {"rCode": 200, "bCodeMessage": None},
            },
        )

    asset = Asset(
        market="US",
        symbol="QQQ",
        name="纳指100ETF-Invesco",
        asset_type="etf",
        currency="USD",
        timezone="America/New_York",
    )
    provider = NasdaqMarketProvider(transport=httpx.MockTransport(handler))
    bars = provider.daily_bars(asset, date(2026, 7, 22), date(2026, 8, 21))

    assert len(bars) == 2
    assert bars[0].close == 705.35
    assert bars[-1].close == 710.93
    assert bars[-1].volume == 33_396_420


def test_public_provider_routes_and_persists_real_upstream_name(session):
    qqq = session.scalar(select(Asset).where(Asset.symbol == "QQQ"))
    gold = session.scalar(select(Asset).where(Asset.symbol == "518880"))
    provider = PublicMarketProvider(
        us_provider=NasdaqMarketProvider(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    200,
                    json={
                        "data": {
                            "tradesTable": {
                                "rows": [
                                    {
                                        "date": "08/20/2026",
                                        "close": "710.93",
                                        "volume": "1",
                                        "open": "712.09",
                                        "high": "714.94",
                                        "low": "708.52",
                                    }
                                ]
                            }
                        },
                        "status": {"rCode": 200},
                    },
                )
            )
        ),
    )

    assert provider_name_for_asset(provider.name, qqq) == "nasdaq"
    assert provider_name_for_asset(provider.name, gold) == "tencent"
    assert sync_market(
        session,
        qqq,
        date(2026, 8, 20),
        date(2026, 8, 20),
        provider=provider,
    ) == 1
    stored = session.scalar(select(MarketBar).where(MarketBar.asset_id == qqq.id))
    assert stored.provider == "nasdaq"


def test_sync_market_does_not_persist_an_unclosed_daily_bar(session):
    asset = session.scalar(select(Asset).where(Asset.symbol == "518880"))
    provider = SyntheticAShareProvider()
    trade_date = date(2026, 8, 25)

    assert sync_market(
        session,
        asset,
        trade_date,
        trade_date,
        provider=provider,
        as_of=local("2026-08-25", 10),
    ) == 0
    assert session.scalar(select(MarketBar).where(MarketBar.asset_id == asset.id)) is None

    assert sync_market(
        session,
        asset,
        trade_date,
        trade_date,
        provider=provider,
        as_of=local("2026-08-25", 15, 1),
    ) == 1


def test_registry_replaces_stale_representative_asset_link(session, settings):
    nasdaq = session.scalar(select(Topic).where(Topic.slug == "nasdaq"))
    ndx = session.scalar(select(Asset).where(Asset.symbol == "NDX"))
    qqq = session.scalar(select(Asset).where(Asset.symbol == "QQQ"))
    session.add(AssetTopic(asset_id=ndx.id, topic_id=nasdaq.id))
    session.commit()

    result = sync_registry(session, settings.config_dir)

    linked_symbols = set(
        session.scalars(
            select(Asset.symbol)
            .join(AssetTopic, AssetTopic.asset_id == Asset.id)
            .where(AssetTopic.topic_id == nasdaq.id)
        ).all()
    )
    assert linked_symbols == {qqq.symbol}
    assert result["links_removed"] == 1
