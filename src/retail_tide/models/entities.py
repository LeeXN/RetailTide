from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    event,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, JSONType, UTCDateTime


class Source(Base):
    __tablename__ = "source"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    source_type: Mapped[str] = mapped_column(String(20), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    collector_version: Mapped[str] = mapped_column(
        String(50), default="collector-v2", nullable=False
    )
    health_status: Mapped[str] = mapped_column(String(20), default="healthy", nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)

    raw_observations: Mapped[list[RawObservation]] = relationship(back_populates="source")
    contents: Mapped[list[Content]] = relationship(back_populates="source")


class RawObservation(Base):
    __tablename__ = "raw_observation"
    __table_args__ = (
        UniqueConstraint(
            "source_id", "source_item_id", "payload_hash", name="uq_raw_observation_version"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("source.id"), nullable=False, index=True)
    source_item_id: Mapped[str] = mapped_column(String(255), nullable=False)
    observation_kind: Mapped[str] = mapped_column(String(50), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    collected_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    collector_version: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)

    source: Mapped[Source] = relationship(back_populates="raw_observations")
    trend_observations: Mapped[list[TrendObservation]] = relationship(
        back_populates="raw_observation"
    )
    topic_matches: Mapped[list[RawObservationTopic]] = relationship(
        back_populates="raw_observation", cascade="all, delete-orphan"
    )


class RawObservationTopic(Base):
    """Topic context recorded by the collection query.

    A source item can be returned for more than one configured topic. Keeping
    that relationship outside the immutable source payload preserves both the
    original observation and every query that discovered it.
    """

    __tablename__ = "raw_observation_topic"
    __table_args__ = (
        UniqueConstraint("raw_observation_id", "topic_id", name="uq_raw_observation_topic"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_observation_id: Mapped[int] = mapped_column(
        ForeignKey("raw_observation.id"), nullable=False, index=True
    )
    topic_id: Mapped[int] = mapped_column(ForeignKey("topic.id"), nullable=False, index=True)
    collection_query: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)

    raw_observation: Mapped[RawObservation] = relationship(back_populates="topic_matches")


@event.listens_for(RawObservation, "before_update")
def _raw_observation_is_append_only(_mapper, _connection, _target):
    raise ValueError("raw_observation is append-only; insert a new payload version instead")


@event.listens_for(RawObservation, "before_delete")
def _raw_observation_cannot_be_deleted(_mapper, _connection, _target):
    raise ValueError("raw_observation is append-only and cannot be deleted")


class Content(Base):
    __tablename__ = "content"
    __table_args__ = (
        UniqueConstraint("source_id", "source_item_id", name="uq_content_source_item"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("source.id"), nullable=False, index=True)
    source_item_id: Mapped[str] = mapped_column(String(255), nullable=False)
    kind: Mapped[str] = mapped_column(String(50), nullable=False)
    parent_content_id: Mapped[int | None] = mapped_column(ForeignKey("content.id"), nullable=True)
    author_id: Mapped[int | None] = mapped_column(ForeignKey("author.id"), nullable=True)
    published_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, index=True)
    first_collected_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    body: Mapped[str] = mapped_column(Text, default="", nullable=False)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    likes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    favorites: Mapped[int | None] = mapped_column(Integer, nullable=True)
    comments: Mapped[int | None] = mapped_column(Integer, nullable=True)
    shares: Mapped[int | None] = mapped_column(Integer, nullable=True)
    views: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    language: Mapped[str | None] = mapped_column(String(20), nullable=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    source: Mapped[Source] = relationship(back_populates="contents")
    author: Mapped[Author | None] = relationship(
        back_populates="contents", foreign_keys=[author_id]
    )
    parent: Mapped[Content | None] = relationship(remote_side=[id], back_populates="children")
    children: Mapped[list[Content]] = relationship(back_populates="parent")
    entities: Mapped[list[ContentEntity]] = relationship(back_populates="content")
    analyses: Mapped[list[ContentAnalysis]] = relationship(back_populates="content")
    archive_snapshots: Mapped[list[ArchiveSnapshot]] = relationship(
        back_populates="content", cascade="all, delete-orphan"
    )


class ContentCluster(Base):
    __tablename__ = "content_cluster"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(16), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class ContentClusterMember(Base):
    __tablename__ = "content_cluster_member"
    __table_args__ = (UniqueConstraint("content_id", name="uq_content_cluster_member_content"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    content_id: Mapped[int] = mapped_column(ForeignKey("content.id"), nullable=False)
    cluster_id: Mapped[int] = mapped_column(ForeignKey("content_cluster.id"), nullable=False)
    hamming_distance: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class TrendObservation(Base):
    __tablename__ = "trend_observation"
    __table_args__ = (
        UniqueConstraint(
            "raw_observation_id", "keyword", "observed_at", name="uq_trend_observation_point"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_observation_id: Mapped[int] = mapped_column(
        ForeignKey("raw_observation.id"), nullable=False, index=True
    )
    source_id: Mapped[int] = mapped_column(ForeignKey("source.id"), nullable=False)
    keyword: Mapped[str] = mapped_column(String(255), nullable=False)
    topic_id: Mapped[int | None] = mapped_column(ForeignKey("topic.id"), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, index=True)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str | None] = mapped_column(String(30), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONType, default=dict, nullable=False
    )

    raw_observation: Mapped[RawObservation] = relationship(back_populates="trend_observations")


class TrendSignal(Base):
    """Derived, independently displayed signals such as Wikimedia pageviews."""

    __tablename__ = "trend_signal"
    __table_args__ = (
        UniqueConstraint(
            "trend_observation_id", "metric_name", "metric_version", name="uq_trend_signal_version"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trend_observation_id: Mapped[int] = mapped_column(
        ForeignKey("trend_observation.id"), nullable=False, index=True
    )
    metric_name: Mapped[str] = mapped_column(String(80), nullable=False)
    raw_value: Mapped[float] = mapped_column(Float, nullable=False)
    change_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    zscore: Mapped[float | None] = mapped_column(Float, nullable=True)
    robust_z: Mapped[float | None] = mapped_column(Float, nullable=True)
    percentile: Mapped[float | None] = mapped_column(Float, nullable=True)
    baseline_window: Mapped[str] = mapped_column(String(20), default="30d", nullable=False)
    metric_version: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class CollectionCheckpoint(Base):
    """Successful collection watermarks for one source/query scope."""

    __tablename__ = "collection_checkpoint"
    __table_args__ = (
        UniqueConstraint(
            "source_id",
            "scope_kind",
            "scope_key",
            "query_fingerprint",
            name="uq_collection_checkpoint_scope",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("source.id"), nullable=False, index=True)
    topic_id: Mapped[int | None] = mapped_column(ForeignKey("topic.id"), nullable=True, index=True)
    scope_kind: Mapped[str] = mapped_column(String(30), nullable=False)
    scope_key: Mapped[str] = mapped_column(String(255), nullable=False)
    query_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    last_successful_until: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    last_status: Mapped[str] = mapped_column(String(30), default="new", nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class CollectionTask(Base):
    """Resumable work for one source, topic/query and immutable time window."""

    __tablename__ = "collection_task"
    __table_args__ = (
        UniqueConstraint(
            "source_id",
            "topic_id",
            "run_key",
            "query_fingerprint",
            name="uq_collection_task_window",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("source.id"), nullable=False, index=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topic.id"), nullable=False, index=True)
    run_key: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    query_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    window_start: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    window_end: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    explicit_window: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    page_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    cursor: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(30), default="pending", nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    pages: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    items_collected: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    duplicates: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    topic_links_added: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class ArchiveSnapshot(Base):
    """A Common Crawl capture linked to an existing Content, not a new post."""

    __tablename__ = "archive_snapshot"
    __table_args__ = (
        UniqueConstraint(
            "content_id", "crawl_id", "captured_at", "digest", name="uq_archive_snapshot_capture"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    content_id: Mapped[int] = mapped_column(ForeignKey("content.id"), nullable=False, index=True)
    raw_observation_id: Mapped[int] = mapped_column(
        ForeignKey("raw_observation.id"), nullable=False, index=True
    )
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False)
    crawl_id: Mapped[str] = mapped_column(String(80), nullable=False)
    captured_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, index=True)
    digest: Mapped[str] = mapped_column(String(255), nullable=False)
    body: Mapped[str] = mapped_column(Text, default="", nullable=False)
    body_truncated: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONType, default=dict, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)

    content: Mapped[Content] = relationship(back_populates="archive_snapshots")


class ArchiveLookupState(Base):
    """Persistent Common Crawl queue state for one known URL."""

    __tablename__ = "archive_lookup_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    content_id: Mapped[int] = mapped_column(
        ForeignKey("content.id"), nullable=False, unique=True, index=True
    )
    last_crawl_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    status: Mapped[str] = mapped_column(String(30), default="pending", nullable=False)
    checked_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class AnalysisTask(Base):
    """Retryable per-content/per-topic semantic analysis work item."""

    __tablename__ = "analysis_task"
    __table_args__ = (
        UniqueConstraint(
            "content_id",
            "topic_id",
            "input_hash",
            "model",
            "prompt_version",
            "schema_version",
            name="uq_analysis_task_version",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    content_id: Mapped[int] = mapped_column(ForeignKey("content.id"), nullable=False, index=True)
    topic_id: Mapped[int | None] = mapped_column(ForeignKey("topic.id"), nullable=True, index=True)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    model: Mapped[str] = mapped_column(String(100), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class Author(Base):
    __tablename__ = "author"
    __table_args__ = (UniqueConstraint("source_id", "author_hash", name="uq_author_source_hash"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("source.id"), nullable=False)
    author_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    actor_type: Mapped[str | None] = mapped_column(String(30), nullable=True)
    actor_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    followers_bucket: Mapped[str | None] = mapped_column(String(30), nullable=True)
    following_bucket: Mapped[str | None] = mapped_column(String(30), nullable=True)
    account_age_bucket: Mapped[str | None] = mapped_column(String(30), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)

    contents: Mapped[list[Content]] = relationship(
        back_populates="author", foreign_keys=[Content.author_id]
    )


class Topic(Base):
    __tablename__ = "topic"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="active", nullable=False)
    parent_id: Mapped[int | None] = mapped_column(ForeignKey("topic.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)

    aliases: Mapped[list[TopicAlias]] = relationship(
        back_populates="topic", cascade="all, delete-orphan"
    )


class TopicAlias(Base):
    __tablename__ = "topic_alias"
    __table_args__ = (UniqueConstraint("topic_id", "alias", name="uq_topic_alias"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topic.id"), nullable=False)
    alias: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    topic: Mapped[Topic] = relationship(back_populates="aliases")


class Asset(Base):
    __tablename__ = "asset"
    __table_args__ = (UniqueConstraint("market", "symbol", name="uq_asset_market_symbol"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market: Mapped[str] = mapped_column(String(30), nullable=False)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    asset_type: Mapped[str] = mapped_column(String(30), nullable=False)
    currency: Mapped[str] = mapped_column(String(10), nullable=False)
    timezone: Mapped[str] = mapped_column(String(50), nullable=False)
    benchmark_asset_id: Mapped[int | None] = mapped_column(ForeignKey("asset.id"), nullable=True)
    sector_benchmark_asset_id: Mapped[int | None] = mapped_column(
        ForeignKey("asset.id"), nullable=True
    )
    active_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    active_to: Mapped[date | None] = mapped_column(Date, nullable=True)

    aliases: Mapped[list[AssetAlias]] = relationship(
        back_populates="asset", cascade="all, delete-orphan"
    )


class AssetAlias(Base):
    __tablename__ = "asset_alias"
    __table_args__ = (UniqueConstraint("asset_id", "alias", name="uq_asset_alias"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset.id"), nullable=False)
    alias: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    alias_type: Mapped[str] = mapped_column(String(30), default="name", nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=100, nullable=False)

    asset: Mapped[Asset] = relationship(back_populates="aliases")


class AssetTopic(Base):
    __tablename__ = "asset_topic"
    __table_args__ = (
        UniqueConstraint("asset_id", "topic_id", "valid_from", name="uq_asset_topic_period"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset.id"), nullable=False)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topic.id"), nullable=False)
    valid_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    valid_to: Mapped[date | None] = mapped_column(Date, nullable=True)


class ContentEntity(Base):
    __tablename__ = "content_entity"
    __table_args__ = (
        UniqueConstraint("content_id", "entity_type", "entity_id", name="uq_content_entity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    content_id: Mapped[int] = mapped_column(ForeignKey("content.id"), nullable=False, index=True)
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False)
    entity_id: Mapped[int] = mapped_column(Integer, nullable=False)
    method: Mapped[str] = mapped_column(String(30), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)

    content: Mapped[Content] = relationship(back_populates="entities")


class ContentAnalysis(Base):
    __tablename__ = "content_analysis"
    __table_args__ = (
        UniqueConstraint(
            "content_id",
            "topic_id",
            "input_hash",
            "model",
            "prompt_version",
            "schema_version",
            name="uq_content_analysis_version",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    content_id: Mapped[int] = mapped_column(ForeignKey("content.id"), nullable=False, index=True)
    topic_id: Mapped[int | None] = mapped_column(ForeignKey("topic.id"), nullable=True, index=True)
    input_hash: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    model: Mapped[str] = mapped_column(String(100), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(100), nullable=False)
    actor_type: Mapped[str] = mapped_column(String(30), nullable=False)
    actor_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    investor_level: Mapped[str] = mapped_column(String(30), nullable=False)
    investor_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    direction: Mapped[str] = mapped_column(String(30), nullable=False)
    direction_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    intent: Mapped[str] = mapped_column(String(30), nullable=False)
    intent_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    position: Mapped[str] = mapped_column(String(30), nullable=False)
    position_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    novice_signals: Mapped[list[str]] = mapped_column(JSONType, default=list, nullable=False)
    emotion_signals: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict, nullable=False)
    spam: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    spam_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    promotion: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    promotion_confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)

    content: Mapped[Content] = relationship(back_populates="analyses")
    review: Mapped[ContentAnalysisReview | None] = relationship(
        back_populates="analysis", uselist=False
    )


class ContentAnalysisReview(Base):
    """Human-readable evidence attached to a semantic analysis version."""

    __tablename__ = "content_analysis_review"
    __table_args__ = (
        UniqueConstraint("content_analysis_id", name="uq_content_analysis_review_analysis"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    content_analysis_id: Mapped[int] = mapped_column(
        ForeignKey("content_analysis.id"), nullable=False, index=True
    )
    reviewer: Mapped[str] = mapped_column(String(100), nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    intent_basis: Mapped[str] = mapped_column(String(50), nullable=False)
    intent_evidence: Mapped[str] = mapped_column(Text, default="", nullable=False)
    rationale: Mapped[str] = mapped_column(Text, default="", nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)

    analysis: Mapped[ContentAnalysis] = relationship(back_populates="review")


class PlatformMetric(Base):
    __tablename__ = "platform_metric"
    __table_args__ = (
        UniqueConstraint(
            "bucket_at",
            "bucket_size",
            "source_id",
            "topic_id",
            "asset_id",
            name="uq_platform_metric_bucket",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bucket_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, index=True)
    bucket_size: Mapped[str] = mapped_column(String(5), nullable=False)
    source_id: Mapped[int] = mapped_column(ForeignKey("source.id"), nullable=False)
    topic_id: Mapped[int | None] = mapped_column(ForeignKey("topic.id"), nullable=True)
    asset_id: Mapped[int | None] = mapped_column(ForeignKey("asset.id"), nullable=True)
    post_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    comment_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    unique_author_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    retail_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    novice_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    bullish_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    bearish_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    buy_intent_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    sell_intent_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    fomo_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    panic_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    engagement_sum: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class MetricSignal(Base):
    __tablename__ = "metric_signal"
    __table_args__ = (
        UniqueConstraint(
            "platform_metric_id", "metric_name", "metric_version", name="uq_metric_signal_version"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform_metric_id: Mapped[int] = mapped_column(
        ForeignKey("platform_metric.id"), nullable=False, index=True
    )
    metric_name: Mapped[str] = mapped_column(String(80), nullable=False)
    raw_value: Mapped[float] = mapped_column(Float, nullable=False)
    zscore: Mapped[float | None] = mapped_column(Float, nullable=True)
    robust_z: Mapped[float | None] = mapped_column(Float, nullable=True)
    percentile: Mapped[float | None] = mapped_column(Float, nullable=True)
    baseline_window: Mapped[str] = mapped_column(String(20), default="30d", nullable=False)
    metric_version: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class SignalEvent(Base):
    __tablename__ = "signal_event"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int | None] = mapped_column(ForeignKey("source.id"), nullable=True)
    topic_id: Mapped[int | None] = mapped_column(ForeignKey("topic.id"), nullable=True)
    asset_id: Mapped[int | None] = mapped_column(ForeignKey("asset.id"), nullable=True)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    peaked_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    peak_value: Mapped[float] = mapped_column(Float, nullable=False)
    peak_zscore: Mapped[float | None] = mapped_column(Float, nullable=True)
    peak_percentile: Mapped[float | None] = mapped_column(Float, nullable=True)
    rule_version: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="discovered", nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    trigger_metric_id: Mapped[int | None] = mapped_column(
        ForeignKey("platform_metric.id"), nullable=True
    )


class EventMetricLink(Base):
    __tablename__ = "event_metric_link"
    __table_args__ = (
        UniqueConstraint("event_id", "metric_signal_id", name="uq_event_metric_link"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("signal_event.id"), nullable=False)
    metric_signal_id: Mapped[int] = mapped_column(ForeignKey("metric_signal.id"), nullable=False)


class DiffusionEvent(Base):
    __tablename__ = "diffusion_event"
    __table_args__ = (UniqueConstraint("signature_hash", name="uq_diffusion_signature"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    topic_id: Mapped[int | None] = mapped_column(ForeignKey("topic.id"), nullable=True)
    asset_id: Mapped[int | None] = mapped_column(ForeignKey("asset.id"), nullable=True)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    first_source_id: Mapped[int] = mapped_column(ForeignKey("source.id"), nullable=False)
    platform_count: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    source_sequence: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONType, default=list, nullable=False
    )
    rule_version: Mapped[str] = mapped_column(String(50), nullable=False)
    signature_hash: Mapped[str] = mapped_column(String(64), nullable=False)


class MarketBar(Base):
    __tablename__ = "market_bar"
    __table_args__ = (
        UniqueConstraint("asset_id", "interval", "ts", "provider", name="uq_market_bar"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset.id"), nullable=False, index=True)
    interval: Mapped[str] = mapped_column(String(10), nullable=False)
    ts: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, index=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    amount: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    adjustment: Mapped[str] = mapped_column(String(20), default="none", nullable=False)
    provider: Mapped[str] = mapped_column(String(80), nullable=False)


class TradingSession(Base):
    __tablename__ = "trading_session"
    __table_args__ = (UniqueConstraint("market", "trade_date", name="uq_trading_session"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market: Mapped[str] = mapped_column(String(30), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    is_open: Mapped[bool] = mapped_column(Boolean, nullable=False)
    open_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    close_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONType, default=dict, nullable=False
    )


class EventReturn(Base):
    __tablename__ = "event_return"
    __table_args__ = (UniqueConstraint("event_id", "asset_id", "horizon", name="uq_event_return"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("signal_event.id"), nullable=False, index=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset.id"), nullable=False)
    entry_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    horizon: Mapped[str] = mapped_column(String(10), nullable=False)
    exit_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    exit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_return: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_return: Mapped[float | None] = mapped_column(Float, nullable=True)
    sector_return: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_abnormal_return: Mapped[float | None] = mapped_column(Float, nullable=True)
    sector_abnormal_return: Mapped[float | None] = mapped_column(Float, nullable=True)
    calculated_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class SourceQualityMetric(Base):
    __tablename__ = "source_quality_metric"
    __table_args__ = (
        UniqueConstraint("source_id", "metric_date", "metric_name", name="uq_source_quality"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("source.id"), nullable=False)
    metric_date: Mapped[date] = mapped_column(Date, nullable=False)
    metric_name: Mapped[str] = mapped_column(String(80), nullable=False)
    metric_value: Mapped[float] = mapped_column(Float, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONType, default=dict, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


class ResearchRun(Base):
    __tablename__ = "research_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    study_type: Mapped[str] = mapped_column(String(50), nullable=False)
    topic_id: Mapped[int | None] = mapped_column(ForeignKey("topic.id"), nullable=True)
    event_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    metric_name: Mapped[str | None] = mapped_column(String(80), nullable=True)
    data_until: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    metric_version: Mapped[str] = mapped_column(String(50), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(100), nullable=False)
    event_rule_version: Mapped[str] = mapped_column(String(50), nullable=False)
    market_provider: Mapped[str] = mapped_column(String(80), nullable=False)
    analysis_model: Mapped[str] = mapped_column(String(100), nullable=False)
    result: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)


# Re-exporting model classes from one module makes both Alembic and small scripts simple.
