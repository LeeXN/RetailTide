from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import (
    Asset,
    AssetTopic,
    Content,
    ContentEntity,
    RawObservation,
    RawObservationTopic,
    Topic,
)
from ..time import now_utc

DISCOVERY_SOURCE_NAMES = frozenset({"zhihu", "xiaohongshu"})
MARKET_CONTEXT_TERMS = (
    "股票",
    "股市",
    "个股",
    "股价",
    "a股",
    "港股",
    "美股",
    "大盘",
    "上证",
    "深证",
    "沪指",
    "深指",
    "创业板",
    "科创板",
    "北证",
    "纳斯达克",
    "纳指",
    "标普",
    "道琼斯",
    "恒生指数",
    "恒生科技指数",
    "etf",
    "基金",
    "期货",
    "期权",
    "证券",
    "券商",
    "板块",
    "概念股",
    "龙头股",
    "涨停",
    "跌停",
    "市值",
    "估值",
    "市盈率",
    "市净率",
    "财报",
    "净利润",
    "分红",
    "仓位",
    "调仓",
    "建仓",
    "加仓",
    "减仓",
    "清仓",
    "持仓",
    "被套",
    "解套",
    "主力",
    "北向资金",
    "成交量",
    "换手率",
    "k线",
    "做多",
    "做空",
    "收益率",
    "回撤",
    "金价",
    "交易日",
    "stock",
    "equity",
    "share price",
    "market cap",
    "nasdaq",
    "portfolio",
)
MARKET_TICKER_RE = re.compile(
    r"(?i)(?<![a-z0-9])(?:sh|sz|bj)?[03689]\d{5}(?:\.(?:sh|sz|bj))?(?![a-z0-9])"
)
BODY_STRONG_MARKET_TERMS = frozenset(
    {
        "股票",
        "个股",
        "股价",
        "etf",
        "基金",
        "期货",
        "期权",
        "证券",
        "券商",
        "概念股",
        "龙头股",
        "涨停",
        "跌停",
        "估值",
        "市盈率",
        "市净率",
        "财报",
        "基本面",
        "净利润",
        "仓位",
        "调仓",
        "建仓",
        "加仓",
        "减仓",
        "清仓",
        "持仓",
        "被套",
        "解套",
        "北向资金",
        "成交量",
        "换手率",
        "k线",
        "做多",
        "做空",
        "收益率",
        "回撤",
        "金价",
        "交易日",
        "stock",
        "equity",
        "share price",
        "market cap",
        "portfolio",
    }
)


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKC", value or "").strip().casefold()
    return re.sub(r"[\s\-_/]+", "", value)


def is_market_relevant_text(text: str) -> bool:
    """Return whether discovery content contains explicit securities-market context.

    Relevance-ranked discovery APIs can return consumer articles for ambiguous
    themes such as 白酒、黄金 or AI. A topic keyword alone is not evidence that
    the article reflects retail-investor sentiment.
    """

    return bool(_market_context_hits(text))


def _market_context_hits(text: str) -> set[str]:
    lowered = (text or "").casefold()
    hits = {term for term in MARKET_CONTEXT_TERMS if term in lowered}
    if MARKET_TICKER_RE.search(lowered):
        hits.add("ticker")
    return hits


def is_market_relevant_content(title: str | None, body: str) -> bool:
    """Require strong body evidence when a discovery title is non-financial."""

    # A financial term in the title is deliberate. In a long consumer article,
    # one incidental sentence such as “茅台是 A 股市值第一” is not enough.
    body_hits = _market_context_hits(body)
    return (
        bool(_market_context_hits(title or ""))
        or "ticker" in body_hits
        or bool(body_hits & BODY_STRONG_MARKET_TERMS)
    )


def _topic_mentioned(topic: Topic, title: str | None, body: str, *, strict: bool) -> bool:
    terms = [topic.slug, topic.name, *(alias.alias for alias in topic.aliases)]
    title_text = normalize_text(title or "")
    body_text = normalize_text(body or "")
    for term in terms:
        normalized = normalize_text(term)
        if not normalized:
            continue
        if normalized in title_text:
            return True
        if strict:
            if body_text.count(normalized) >= 2:
                return True
        elif normalized in body_text:
            return True
    return False


@dataclass(frozen=True)
class EntityMatch:
    entity_type: str
    entity_id: int
    method: str
    confidence: float
    label: str


class EntityResolver:
    """Deterministic registry-first resolver; ambiguity returns no asset match."""

    def __init__(self, session: Session):
        self.session = session

    def resolve_asset(self, text: str) -> EntityMatch | None:
        normalized = normalize_text(text)
        assets = self.session.scalars(select(Asset)).all()
        # A ticker is only exact when it appears as a token, avoiding 6005190 -> 600519.
        for asset in assets:
            if re.search(
                rf"(?<![A-Za-z0-9]){re.escape(asset.symbol.casefold())}(?![A-Za-z0-9])",
                text.casefold(),
            ):
                return EntityMatch("asset", asset.id, "ticker", 1.0, asset.name)

        candidates: list[tuple[int, int, Asset, str]] = []
        for asset in assets:
            for alias in asset.aliases:
                if normalize_text(alias.alias) == normalized:
                    candidates.append((alias.priority, len(alias.alias), asset, alias.alias))
        unique_assets = {candidate[2].id for candidate in candidates}
        if len(unique_assets) == 1:
            _priority, _length, asset, alias = min(candidates, key=lambda x: (x[0], -x[1]))
            method = (
                "exact_alias"
                if alias.casefold().strip() == text.casefold().strip()
                else "normalized_alias"
            )
            return EntityMatch("asset", asset.id, method, 0.98, asset.name)
        # The helper is also useful for a short title/body fragment. Resolve a
        # contained alias only when it maps to exactly one asset; ambiguous
        # aliases deliberately remain unresolved.
        contained: list[tuple[int, int, Asset, str]] = []
        for asset in assets:
            for alias in asset.aliases:
                alias_norm = normalize_text(alias.alias)
                if alias_norm and alias_norm in normalized:
                    contained.append((alias.priority, len(alias.alias), asset, alias.alias))
        contained_assets = {candidate[2].id for candidate in contained}
        if len(contained_assets) != 1:
            return None
        _priority, _length, asset, alias = min(contained, key=lambda x: (x[0], -x[1]))
        return EntityMatch("asset", asset.id, "normalized_alias", 0.92, asset.name)

    def resolve_topic(self, text: str) -> EntityMatch | None:
        normalized = normalize_text(text)
        topics = self.session.scalars(select(Topic).where(Topic.status == "active")).all()
        for topic in topics:
            if normalize_text(topic.slug) == normalized or normalize_text(topic.name) == normalized:
                return EntityMatch("topic", topic.id, "exact_alias", 1.0, topic.name)
        candidates: list[Topic] = []
        for topic in topics:
            if any(normalize_text(alias.alias) == normalized for alias in topic.aliases):
                candidates.append(topic)
        if len(candidates) == 1:
            return EntityMatch("topic", candidates[0].id, "exact_alias", 0.98, candidates[0].name)
        return None

    def resolve_content(self, content: Content) -> list[EntityMatch]:
        text = " ".join(part for part in (content.title or "", content.body or "") if part)
        discovery_content = content.source.name in DISCOVERY_SOURCE_NAMES
        if discovery_content and not is_market_relevant_content(content.title, content.body):
            # RawObservation remains immutable and query provenance remains in
            # RawObservationTopic. Only mutable derived topic/asset links are
            # removed so consumer/product content cannot enter market metrics.
            existing_links = self.session.scalars(
                select(ContentEntity).where(
                    ContentEntity.content_id == content.id,
                    ContentEntity.entity_type.in_(("topic", "asset")),
                )
            ).all()
            for link in existing_links:
                self.session.delete(link)
            return []
        if discovery_content:
            # Search APIs rank by relevance but may return a broad-market item
            # for a thematic query. Rebuild topic links from explicit text or
            # an associated asset instead of trusting the query alone.
            derived_links = self.session.scalars(
                select(ContentEntity).where(
                    ContentEntity.content_id == content.id,
                    ContentEntity.entity_type.in_(("topic", "asset")),
                )
            ).all()
            for link in derived_links:
                self.session.delete(link)
            self.session.flush()
        matches: list[EntityMatch] = []
        # Match canonical registry values inside text. Exact aliases are preferred
        # and each entity is emitted once.
        assets = self.session.scalars(select(Asset)).all()
        alias_assets: dict[str, set[int]] = {}
        for asset in assets:
            for alias in asset.aliases:
                alias_assets.setdefault(normalize_text(alias.alias), set()).add(asset.id)
        for asset in assets:
            found: EntityMatch | None = None
            if re.search(
                rf"(?<![A-Za-z0-9]){re.escape(asset.symbol.casefold())}(?![A-Za-z0-9])",
                text.casefold(),
            ):
                found = EntityMatch("asset", asset.id, "ticker", 1.0, asset.name)
            else:
                aliases = sorted(asset.aliases, key=lambda a: (a.priority, -len(a.alias)))
                for alias in aliases:
                    alias_norm = normalize_text(alias.alias)
                    if (
                        alias_norm
                        and alias_norm in normalize_text(text)
                        and len(alias_assets.get(alias_norm, set())) == 1
                    ):
                        method = (
                            "exact_alias"
                            if alias.alias.casefold().strip() in text.casefold()
                            else "normalized_alias"
                        )
                        found = EntityMatch("asset", asset.id, method, 0.98, asset.name)
                        break
            if found:
                matches.append(found)
        if not discovery_content:
            collection_topics = self.session.scalars(
                select(Topic)
                .join(RawObservationTopic, RawObservationTopic.topic_id == Topic.id)
                .join(
                    RawObservation,
                    RawObservation.id == RawObservationTopic.raw_observation_id,
                )
                .where(
                    RawObservation.source_id == content.source_id,
                    RawObservation.source_item_id == content.source_item_id,
                    Topic.status == "active",
                )
                .distinct()
            ).all()
            matches.extend(
                EntityMatch("topic", topic.id, "collection_query", 0.9, topic.name)
                for topic in collection_topics
            )
        matched_asset_ids = [
            match.entity_id for match in matches if match.entity_type == "asset"
        ]
        if matched_asset_ids:
            asset_topics = self.session.scalars(
                select(Topic)
                .join(AssetTopic, AssetTopic.topic_id == Topic.id)
                .where(
                    AssetTopic.asset_id.in_(matched_asset_ids),
                    Topic.status == "active",
                )
                .distinct()
            ).all()
            matches.extend(
                EntityMatch("topic", topic.id, "asset_topic", 0.96, topic.name)
                for topic in asset_topics
            )
        for topic in self.session.scalars(select(Topic).where(Topic.status == "active")).all():
            if _topic_mentioned(
                topic,
                content.title,
                content.body,
                strict=discovery_content,
            ):
                matches.append(EntityMatch("topic", topic.id, "exact_alias", 0.98, topic.name))
        unique: dict[tuple[str, int], EntityMatch] = {
            (m.entity_type, m.entity_id): m for m in matches
        }
        for match in unique.values():
            exists = self.session.scalar(
                select(ContentEntity).where(
                    ContentEntity.content_id == content.id,
                    ContentEntity.entity_type == match.entity_type,
                    ContentEntity.entity_id == match.entity_id,
                )
            )
            if exists is None:
                self.session.add(
                    ContentEntity(
                        content_id=content.id,
                        entity_type=match.entity_type,
                        entity_id=match.entity_id,
                        method=match.method,
                        confidence=match.confidence,
                        created_at=now_utc(),
                    )
                )
        return list(unique.values())


def resolve_pending_entities(session: Session, *, limit: int = 500) -> int:
    resolver = EntityResolver(session)
    # Entity links are derived and may change when aliases change, so this stage
    # intentionally rechecks content. Prefer newest rows when a caller supplies
    # a bounded limit; otherwise a growing database would repeatedly scan only
    # the oldest rows and newly collected content would never become visible.
    contents = session.scalars(select(Content).order_by(Content.id.desc()).limit(limit)).all()
    for content in contents:
        resolver.resolve_content(content)
    session.flush()
    from .dedup import cluster_contents

    cluster_contents(session, limit=limit)
    session.commit()
    return len(contents)
