from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import DEFAULT_ENABLED_SOURCES
from .models import Asset, AssetAlias, AssetTopic, Source, Topic, TopicAlias
from .time import now_utc

SOURCE_DEFINITIONS = [
    {"name": "guba", "source_type": "content"},
    {"name": "taoguba", "source_type": "content"},
    {"name": "zhihu", "source_type": "content"},
    {"name": "xiaohongshu", "source_type": "content"},
    {"name": "common-crawl", "source_type": "archive"},
    {"name": "wikimedia-pageviews", "source_type": "trend"},
]


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def sync_registry(
    session: Session,
    config_dir: str | Path = "config",
    *,
    enabled_sources: tuple[str, ...] | None = None,
    collector_version: str = "collector-v2",
) -> dict[str, int]:
    config_dir = Path(config_dir)
    if enabled_sources is None:
        enabled_sources = DEFAULT_ENABLED_SOURCES
    created = {
        "topics": 0,
        "assets": 0,
        "aliases": 0,
        "sources": 0,
        "links": 0,
        "links_removed": 0,
    }
    now = now_utc()

    for definition in SOURCE_DEFINITIONS:
        source = session.scalar(select(Source).where(Source.name == definition["name"]))
        if source is None:
            source = Source(
                name=definition["name"],
                source_type=definition["source_type"],
                enabled=definition["name"] in enabled_sources,
                collector_version=collector_version,
                health_status="healthy",
                created_at=now,
            )
            session.add(source)
            created["sources"] += 1
        else:
            source.enabled = definition["name"] in enabled_sources
            source.collector_version = collector_version

    topic_by_slug: dict[str, Topic] = {}
    topics = _load_yaml(config_dir / "topics.yaml").get("topics", [])
    for item in topics:
        topic = session.scalar(select(Topic).where(Topic.slug == item["slug"]))
        if topic is None:
            topic = Topic(
                slug=item["slug"],
                name=item["name"],
                status=item.get("status", "active"),
                created_at=now,
            )
            session.add(topic)
            session.flush()
            created["topics"] += 1
        else:
            topic.name = item["name"]
            topic.status = item.get("status", topic.status)
        topic_by_slug[topic.slug] = topic
        existing_aliases = {a.alias.casefold() for a in topic.aliases}
        for alias in item.get("aliases", []):
            if str(alias).casefold() not in existing_aliases:
                session.add(TopicAlias(topic_id=topic.id, alias=str(alias)))
                existing_aliases.add(str(alias).casefold())
                created["aliases"] += 1

    asset_by_symbol: dict[str, Asset] = {}
    assets = _load_yaml(config_dir / "assets.yaml").get("assets", [])
    for item in assets:
        asset = session.scalar(
            select(Asset).where(Asset.market == item["market"], Asset.symbol == str(item["symbol"]))
        )
        if asset is None:
            asset = Asset(
                market=item["market"],
                symbol=str(item["symbol"]),
                name=item["name"],
                asset_type=item["asset_type"],
                currency=item["currency"],
                timezone=item["timezone"],
            )
            session.add(asset)
            session.flush()
            created["assets"] += 1
        else:
            asset.name = item["name"]
        asset_by_symbol[asset.symbol] = asset
        existing_aliases = {a.alias.casefold() for a in asset.aliases}
        for alias in item.get("aliases", []):
            if isinstance(alias, str):
                alias = {"value": alias}
            value = str(alias["value"])
            if value.casefold() not in existing_aliases:
                session.add(
                    AssetAlias(
                        asset_id=asset.id,
                        alias=value,
                        alias_type=alias.get("type", "name"),
                        priority=int(alias.get("priority", 100)),
                    )
                )
                existing_aliases.add(value.casefold())
                created["aliases"] += 1

    session.flush()
    # Fill benchmark references after all assets exist.
    for item in assets:
        asset = asset_by_symbol.get(str(item["symbol"]))
        benchmark = asset_by_symbol.get(str(item.get("benchmark", "")))
        sector = asset_by_symbol.get(str(item.get("sector_benchmark", "")))
        if asset is not None:
            asset.benchmark_asset_id = benchmark.id if benchmark else asset.benchmark_asset_id
            asset.sector_benchmark_asset_id = (
                sector.id if sector else asset.sector_benchmark_asset_id
            )

    # Registry links are intentionally explicit; aliases do not create topics.
    # Keeping links next to each asset makes representative ETF/leader changes
    # configuration-only instead of requiring code edits.
    topic_links: dict[str, list[str]] = {}
    for item in assets:
        for slug in item.get("topics", []):
            topic_links.setdefault(str(slug), []).append(str(item["symbol"]))
    desired_links: set[tuple[int, int]] = set()
    for slug, symbols in topic_links.items():
        topic = topic_by_slug.get(slug)
        if topic is None:
            continue
        for symbol in symbols:
            asset = asset_by_symbol.get(symbol)
            if asset is None:
                continue
            desired_links.add((asset.id, topic.id))
            exists = session.scalar(
                select(AssetTopic).where(
                    AssetTopic.asset_id == asset.id, AssetTopic.topic_id == topic.id
                )
            )
            if exists is None:
                session.add(
                    AssetTopic(asset_id=asset.id, topic_id=topic.id, valid_from=date(2000, 1, 1))
                )
                created["links"] += 1

    # For YAML-managed assets and topics, the registry is authoritative. This
    # allows a broken representative symbol to be replaced without leaving the
    # old AssetTopic link visible in charts.
    managed_asset_ids = [asset.id for asset in asset_by_symbol.values()]
    managed_topic_ids = [topic.id for topic in topic_by_slug.values()]
    existing_links = session.scalars(
        select(AssetTopic).where(
            AssetTopic.asset_id.in_(managed_asset_ids),
            AssetTopic.topic_id.in_(managed_topic_ids),
        )
    ).all()
    for link in existing_links:
        if (link.asset_id, link.topic_id) not in desired_links:
            session.delete(link)
            created["links_removed"] += 1

    session.commit()
    return created
