from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import Settings, get_settings
from ..market import first_tradable_bar_after, provider_name_for_asset
from ..models import (
    Asset,
    AssetTopic,
    EventReturn,
    MarketBar,
    PlatformMetric,
    SignalEvent,
)
from ..time import bucket_delta, now_utc

HORIZONS = ("1d", "3d", "5d", "10d", "20d")


def _horizon_days(horizon: str) -> int:
    if not horizon.endswith("d"):
        raise ValueError(f"unsupported horizon: {horizon}")
    return int(horizon[:-1])


def event_signal_time(session: Session, event: SignalEvent):
    if event.trigger_metric_id is None:
        return event.peaked_at
    metric = session.get(PlatformMetric, event.trigger_metric_id)
    if metric is None:
        return event.peaked_at
    return metric.bucket_at + bucket_delta(metric.bucket_size)


def _assets_for_event(session: Session, event: SignalEvent) -> list[Asset]:
    if event.asset_id is not None:
        asset = session.get(Asset, event.asset_id)
        return [asset] if asset else []
    if event.topic_id is None:
        return []
    ids = session.scalars(
        select(AssetTopic.asset_id).where(AssetTopic.topic_id == event.topic_id)
    ).all()
    return session.scalars(select(Asset).where(Asset.id.in_(ids))).all() if ids else []


def _bars(session: Session, asset_id: int, provider: str) -> list[MarketBar]:
    return session.scalars(
        select(MarketBar)
        .where(
            MarketBar.asset_id == asset_id,
            MarketBar.interval == "1d",
            MarketBar.provider == provider,
        )
        .order_by(MarketBar.ts)
    ).all()


def _benchmark_return(benchmark_bars: list[MarketBar], entry_at, exit_at) -> float | None:
    entry = next((bar for bar in benchmark_bars if bar.ts == entry_at), None)
    exit_bar = next((bar for bar in benchmark_bars if bar.ts == exit_at), None)
    if entry is None or exit_bar is None or not entry.open:
        return None
    return exit_bar.close / entry.open - 1


def evaluate_events(
    session: Session,
    *,
    settings: Settings | None = None,
    provider_name: str | None = None,
    event_id: int | None = None,
) -> int:
    settings = settings or get_settings()
    configured_provider = provider_name or settings.market_provider
    query = select(SignalEvent).order_by(SignalEvent.id)
    if event_id is not None:
        query = query.where(SignalEvent.id == event_id)
    events = session.scalars(query).all()
    touched = 0
    for event in events:
        signal_at = event_signal_time(session, event)
        for asset in _assets_for_event(session, event):
            asset_provider = provider_name_for_asset(configured_provider, asset)
            bars = _bars(session, asset.id, asset_provider)
            if not bars:
                continue
            entry_bar = first_tradable_bar_after(signal_at, bars)
            if entry_bar is None:
                # No future market data yet; keep no misleading return row.
                continue
            if asset.benchmark_asset_id:
                benchmark = session.get(Asset, asset.benchmark_asset_id)
            else:
                benchmark = None
            benchmark_bars = (
                _bars(
                    session,
                    benchmark.id,
                    provider_name_for_asset(configured_provider, benchmark),
                )
                if benchmark
                else []
            )
            if asset.sector_benchmark_asset_id:
                sector_benchmark = session.get(Asset, asset.sector_benchmark_asset_id)
            else:
                sector_benchmark = None
            sector_bars = (
                _bars(
                    session,
                    sector_benchmark.id,
                    provider_name_for_asset(configured_provider, sector_benchmark),
                )
                if sector_benchmark
                else []
            )
            entry_index = bars.index(entry_bar)
            for horizon in HORIZONS:
                horizon_index = entry_index + _horizon_days(horizon) - 1
                exit_bar = bars[horizon_index] if horizon_index < len(bars) else None
                raw_return = (
                    exit_bar.close / entry_bar.open - 1 if exit_bar and entry_bar.open else None
                )
                market_return = (
                    _benchmark_return(benchmark_bars, entry_bar.ts, exit_bar.ts)
                    if exit_bar and benchmark_bars
                    else None
                )
                sector_return = (
                    _benchmark_return(sector_bars, entry_bar.ts, exit_bar.ts)
                    if exit_bar and sector_bars
                    else None
                )
                existing = session.scalar(
                    select(EventReturn).where(
                        EventReturn.event_id == event.id,
                        EventReturn.asset_id == asset.id,
                        EventReturn.horizon == horizon,
                    )
                )
                values = {
                    "entry_at": entry_bar.ts,
                    "entry_price": entry_bar.open,
                    "exit_at": exit_bar.ts if exit_bar else None,
                    "exit_price": exit_bar.close if exit_bar else None,
                    "raw_return": raw_return,
                    "market_return": market_return,
                    "sector_return": sector_return,
                    "market_abnormal_return": raw_return - market_return
                    if raw_return is not None and market_return is not None
                    else None,
                    "sector_abnormal_return": raw_return - sector_return
                    if raw_return is not None and sector_return is not None
                    else None,
                    "calculated_at": now_utc(),
                }
                if existing is None:
                    session.add(
                        EventReturn(
                            event_id=event.id,
                            asset_id=asset.id,
                            horizon=horizon,
                            **values,
                        )
                    )
                else:
                    for key, value in values.items():
                        setattr(existing, key, value)
                touched += 1
    session.commit()
    return touched
