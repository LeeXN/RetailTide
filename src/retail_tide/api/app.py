from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Callable
from datetime import date as CalendarDate
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from starlette.middleware.gzip import GZipMiddleware

from ..config import (
    Settings,
    get_settings,
    llm_config_status,
    market_config_status,
    source_config_status,
)
from ..db import init_db, make_engine, session_factory
from ..models import (
    ArchiveLookupState,
    ArchiveSnapshot,
    Asset,
    Content,
    ContentAnalysis,
    ContentEntity,
    DiffusionEvent,
    EventMetricLink,
    EventReturn,
    MetricSignal,
    PlatformMetric,
    RawObservation,
    SignalEvent,
    Source,
    SourceQualityMetric,
    Topic,
    TrendObservation,
    TrendSignal,
)
from ..observability import prometheus_text
from ..pipeline.analysis import analysis_precedence_key
from ..pipeline.trends import trend_snapshot
from ..research import event_study, quantile_study
from ..time import SHANGHAI, as_utc, bucket_delta, now_utc
from .dashboard_pages import dashboard_html
from .overview import CONTENT_FILTERS, topic_contents, topic_overview, topic_series

_CONTENT_FILTER_PATTERN = f"^({'|'.join(CONTENT_FILTERS)})$"
logger = logging.getLogger(__name__)


def _sqlite_revision(engine) -> tuple[tuple[int, int], tuple[int, int]] | None:
    """Return a cross-process cache revision for the SQLite database and WAL."""

    if engine.dialect.name != "sqlite" or not engine.url.database:
        return None
    database = Path(engine.url.database)
    if not database.is_absolute():
        database = Path.cwd() / database

    def state(path: Path) -> tuple[int, int]:
        try:
            stat = path.stat()
        except FileNotFoundError:
            return 0, 0
        return stat.st_mtime_ns, stat.st_size

    return state(database), state(Path(f"{database}-wal"))


class _RevisionCache:
    """Small process cache invalidated by every committed SQLite file revision."""

    def __init__(self, *, max_entries: int = 64):
        self.max_entries = max_entries
        self.values: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()
        self.lock = Lock()

    def get_or_create(
        self,
        *,
        engine: Any,
        key: tuple[Any, ...],
        create: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        with self.lock:
            # Recheck after acquiring the lock so concurrent misses collapse to one calculation.
            for _attempt in range(2):
                revision = _sqlite_revision(engine)
                if revision is None:
                    return create()
                revision_key = (*key, revision)
                cached = self.values.get(revision_key)
                if cached is not None:
                    self.values.move_to_end(revision_key)
                    return cached
                value = create()
                if _sqlite_revision(engine) != revision:
                    continue
                self.values[revision_key] = value
                self.values.move_to_end(revision_key)
                while len(self.values) > self.max_entries:
                    self.values.popitem(last=False)
                return value
            return create()


def _session(engine):
    return session_factory(engine)()


def _event_json(
    session: Session,
    event: SignalEvent,
    *,
    include_raw: bool = False,
    link_rows: list[EventMetricLink] | None = None,
    signals_by_id: dict[int, MetricSignal] | None = None,
    metrics_by_id: dict[int, PlatformMetric] | None = None,
    return_rows: list[EventReturn] | None = None,
) -> dict[str, Any]:
    links = (
        session.scalars(
            select(EventMetricLink).where(EventMetricLink.event_id == event.id)
        ).all()
        if link_rows is None
        else link_rows
    )
    metric_payload = []
    for link in links:
        signal = (
            signals_by_id.get(link.metric_signal_id)
            if signals_by_id is not None
            else session.get(MetricSignal, link.metric_signal_id)
        )
        metric = (
            metrics_by_id.get(signal.platform_metric_id)
            if signal is not None and metrics_by_id is not None
            else session.get(PlatformMetric, signal.platform_metric_id)
            if signal is not None
            else None
        )
        if not signal or not metric:
            continue
        metric_payload.append(
            {
                "signal_id": signal.id,
                "metric_id": metric.id,
                "metric_name": signal.metric_name,
                "raw_value": signal.raw_value,
                "zscore": signal.zscore,
                "robust_z": signal.robust_z,
                "percentile": signal.percentile,
                "bucket_at": as_utc(metric.bucket_at),
                "bucket_size": metric.bucket_size,
                "source_id": metric.source_id,
                "topic_id": metric.topic_id,
                "asset_id": metric.asset_id,
            }
        )
    returns = (
        session.scalars(select(EventReturn).where(EventReturn.event_id == event.id)).all()
        if return_rows is None
        else return_rows
    )
    raw_rows = []
    raw_drilldown_limit = 50
    if include_raw:
        seen = set()
        for item in metric_payload:
            if len(raw_rows) >= raw_drilldown_limit:
                break
            metric = (
                metrics_by_id.get(item["metric_id"])
                if metrics_by_id is not None
                else session.get(PlatformMetric, item["metric_id"])
            )
            if not metric:
                continue
            end = metric.bucket_at + bucket_delta(metric.bucket_size)
            content_query = select(Content).where(
                Content.source_id == metric.source_id,
                Content.published_at >= metric.bucket_at,
                Content.published_at < end,
            )
            if event.topic_id is not None:
                content_query = content_query.join(
                    ContentEntity, ContentEntity.content_id == Content.id
                ).where(
                    ContentEntity.entity_type == "topic",
                    ContentEntity.entity_id == event.topic_id,
                )
            contents = session.scalars(
                content_query.order_by(Content.published_at.desc()).limit(raw_drilldown_limit)
            ).all()
            for content in contents:
                if content.id in seen:
                    continue
                seen.add(content.id)
                raws = session.scalars(
                    select(RawObservation)
                    .where(
                        RawObservation.source_id == content.source_id,
                        RawObservation.source_item_id == content.source_item_id,
                    )
                    .order_by(RawObservation.id.desc())
                ).all()
                analyses = session.scalars(
                    select(ContentAnalysis)
                    .where(ContentAnalysis.content_id == content.id)
                    .order_by(ContentAnalysis.id.desc())
                ).all()
                analysis = max(analyses, key=analysis_precedence_key) if analyses else None
                raw_rows.append(
                    {
                        "content": {
                            "id": content.id,
                            "source_id": content.source_id,
                            "source_item_id": content.source_item_id,
                            "kind": content.kind,
                            "published_at": as_utc(content.published_at),
                            "title": content.title,
                            "body": content.body,
                            "url": content.url,
                            "likes": content.likes,
                            "comments": content.comments,
                            "shares": content.shares,
                            "views": content.views,
                        },
                        "analysis": {
                            "id": analysis.id,
                            "model": analysis.model,
                            "prompt_version": analysis.prompt_version,
                            "schema_version": analysis.schema_version,
                            "actor_type": analysis.actor_type,
                            "investor_level": analysis.investor_level,
                            "direction": analysis.direction,
                            "intent": analysis.intent,
                            "position": analysis.position,
                            "novice_signals": analysis.novice_signals,
                            "emotion_signals": analysis.emotion_signals,
                            "spam": analysis.spam,
                            "promotion": analysis.promotion,
                            "promotion_confidence": analysis.promotion_confidence,
                        }
                        if analysis
                        else None,
                        "raw_observations": [
                            {
                                "id": raw.id,
                                "source_item_id": raw.source_item_id,
                                "observation_kind": raw.observation_kind,
                                "published_at": as_utc(raw.published_at),
                                "observed_at": as_utc(raw.observed_at),
                                "payload": raw.payload,
                                "payload_hash": raw.payload_hash,
                            }
                            for raw in raws
                        ],
                    }
                )
                if len(raw_rows) >= raw_drilldown_limit:
                    break
    return {
        "id": event.id,
        "source_id": event.source_id,
        "topic_id": event.topic_id,
        "asset_id": event.asset_id,
        "event_type": event.event_type,
        "started_at": as_utc(event.started_at),
        "peaked_at": as_utc(event.peaked_at),
        "ended_at": as_utc(event.ended_at),
        "peak_value": event.peak_value,
        "peak_zscore": event.peak_zscore,
        "peak_percentile": event.peak_percentile,
        "rule_version": event.rule_version,
        "status": event.status,
        "metrics": metric_payload,
        "returns": [
            {
                "asset_id": item.asset_id,
                "horizon": item.horizon,
                "entry_at": as_utc(item.entry_at),
                "entry_price": item.entry_price,
                "exit_at": as_utc(item.exit_at),
                "exit_price": item.exit_price,
                "raw_return": item.raw_return,
                "market_return": item.market_return,
                "market_abnormal_return": item.market_abnormal_return,
                "sector_abnormal_return": item.sector_abnormal_return,
            }
            for item in returns
        ],
        "raw_drilldown": raw_rows,
        "raw_drilldown_limit": raw_drilldown_limit,
    }


def create_app(*, engine=None, settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    engine = engine or make_engine(settings)
    init_db(engine)
    app = FastAPI(title="RetailTide API", version="0.1.0")
    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)
    overview_cache = _RevisionCache()
    expected_overview_sources = tuple(
        name
        for name in settings.enabled_sources
        if name in {"guba", "taoguba", "xiaohongshu"}
    )

    def calculate_overview(
        bucket_size: str,
        selected_date: CalendarDate | None,
        history_start_date: CalendarDate | None,
    ) -> dict[str, Any]:
        with _session(engine) as calculation_session:
            return topic_overview(
                calculation_session,
                bucket_size=bucket_size,
                selected_date=selected_date,
                history_start_date=history_start_date,
                expected_sources=expected_overview_sources,
            )

    def warm_closed_overview() -> None:
        selected_date = now_utc().astimezone(SHANGHAI).date() - timedelta(days=1)
        try:
            overview_cache.get_or_create(
                engine=engine,
                key=("1d", selected_date, None, expected_overview_sources),
                create=lambda: calculate_overview("1d", selected_date, None),
            )
        except Exception:
            logger.warning("event=overview_cache_warm_failed", exc_info=True)

    app.router.add_event_handler("startup", warm_closed_overview)

    @app.get("/", response_class=HTMLResponse)
    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard():
        return HTMLResponse(dashboard_html("overview"))

    @app.get("/trends", response_class=HTMLResponse)
    def trends_page():
        return HTMLResponse(dashboard_html("trends"))

    @app.get("/posts", response_class=HTMLResponse)
    def posts_page():
        return HTMLResponse(dashboard_html("posts"))

    @app.get("/research", response_class=HTMLResponse)
    def research_page():
        return HTMLResponse(dashboard_html("research"))

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/metrics/prometheus")
    def prometheus_metrics():
        from fastapi.responses import PlainTextResponse

        return PlainTextResponse(prometheus_text(), media_type="text/plain; version=0.0.4")

    @app.get("/topics")
    def topics():
        with _session(engine) as session:
            return [
                {"id": row.id, "slug": row.slug, "name": row.name, "status": row.status}
                for row in session.scalars(select(Topic).order_by(Topic.slug)).all()
            ]

    @app.get("/topics/overview")
    def topics_overview(
        bucket_size: str = Query("1d", pattern="^(1h|1d)$"),
        date: CalendarDate | None = None,
        from_date: CalendarDate | None = None,
        to_date: CalendarDate | None = None,
    ):
        try:
            if date is not None and to_date is not None and date != to_date:
                raise ValueError("date and to_date must identify the same calendar day")
            selected_date = to_date or date
            def calculate() -> dict[str, Any]:
                return calculate_overview(bucket_size, selected_date, from_date)

            today = now_utc().astimezone(SHANGHAI).date()
            if selected_date is not None and selected_date < today:
                return overview_cache.get_or_create(
                    engine=engine,
                    key=(
                        bucket_size,
                        selected_date,
                        from_date,
                        expected_overview_sources,
                    ),
                    create=calculate,
                )
            return calculate()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/topics/{topic_id}/series")
    def topic_metric_series(
        topic_id: int,
        bucket_size: str = Query("1d", pattern="^(1h|1d)$"),
        limit: int = Query(500, ge=1, le=5000),
    ):
        with _session(engine) as session:
            topic = session.get(Topic, topic_id)
            if topic is None or topic.status != "active":
                raise HTTPException(status_code=404, detail="topic not found")
            return topic_series(
                session,
                topic_id=topic_id,
                bucket_size=bucket_size,
                limit=limit,
            )

    @app.get("/topics/{topic_id}/attention")
    def topic_attention(
        topic_id: int,
        keyword: str | None = None,
        limit: int = Query(200, ge=1, le=5000),
    ):
        """Independent Wikimedia attention series; never merged into post heat."""

        with _session(engine) as session:
            topic = session.get(Topic, topic_id)
            if topic is None or topic.status != "active":
                raise HTTPException(status_code=404, detail="topic not found")
            return trend_snapshot(session, topic_id=topic_id, keyword=keyword, limit=limit)

    @app.get("/trends/attention")
    def attention_trends(
        keyword: str | None = None,
        limit: int = Query(200, ge=1, le=5000),
    ):
        with _session(engine) as session:
            return trend_snapshot(session, keyword=keyword, limit=limit, all_topics=True)

    @app.get("/contents")
    def all_latest_contents(
        bucket_size: str = Query("1d", pattern="^(1h|1d)$"),
        content_filter: str = Query(
            "all", alias="filter", pattern=_CONTENT_FILTER_PATTERN
        ),
        source_filter: str = Query("all", alias="source", pattern="^(all|[a-zA-Z0-9_-]{1,50})$"),
        period: str = Query("latest", pattern="^(latest|24h|7d|30d|all|custom)$"),
        from_at: datetime | None = None,
        to_at: datetime | None = None,
        limit: int = Query(30, ge=1, le=100),
        offset: int = Query(0, ge=0),
    ):
        with _session(engine) as session:
            return topic_contents(
                session,
                topic_id=None,
                bucket_size=bucket_size,
                content_filter=content_filter,
                source_name=source_filter,
                period=period,
                from_at=from_at,
                to_at=to_at,
                limit=limit,
                offset=offset,
            )

    @app.get("/topics/{topic_id}/contents")
    def topic_latest_contents(
        topic_id: int,
        bucket_size: str = Query("1d", pattern="^(1h|1d)$"),
        content_filter: str = Query(
            "all", alias="filter", pattern=_CONTENT_FILTER_PATTERN
        ),
        source_filter: str = Query("all", alias="source", pattern="^(all|[a-zA-Z0-9_-]{1,50})$"),
        period: str = Query("latest", pattern="^(latest|24h|7d|30d|all|custom)$"),
        from_at: datetime | None = None,
        to_at: datetime | None = None,
        limit: int = Query(30, ge=1, le=100),
        offset: int = Query(0, ge=0),
    ):
        with _session(engine) as session:
            topic = session.get(Topic, topic_id)
            if topic is None or topic.status != "active":
                raise HTTPException(status_code=404, detail="topic not found")
            return topic_contents(
                session,
                topic_id=topic_id,
                bucket_size=bucket_size,
                content_filter=content_filter,
                source_name=source_filter,
                period=period,
                from_at=from_at,
                to_at=to_at,
                limit=limit,
                offset=offset,
            )

    @app.get("/assets")
    def assets():
        with _session(engine) as session:
            return [
                {
                    "id": row.id,
                    "market": row.market,
                    "symbol": row.symbol,
                    "name": row.name,
                    "asset_type": row.asset_type,
                    "currency": row.currency,
                }
                for row in session.scalars(select(Asset).order_by(Asset.market, Asset.symbol)).all()
            ]

    @app.get("/metrics")
    def metrics(
        topic_id: int | None = None,
        source_id: int | None = None,
        bucket_size: str = Query("1d", pattern="^(1h|1d)$"),
        limit: int = Query(200, ge=1, le=5000),
    ):
        with _session(engine) as session:
            query = select(PlatformMetric).where(PlatformMetric.bucket_size == bucket_size)
            if topic_id is not None:
                query = query.where(PlatformMetric.topic_id == topic_id)
            if source_id is not None:
                query = query.where(PlatformMetric.source_id == source_id)
            rows = session.scalars(
                query.order_by(PlatformMetric.bucket_at.desc()).limit(limit)
            ).all()
            metric_ids = [row.id for row in rows]
            signal_rows = (
                session.scalars(
                    select(MetricSignal)
                    .where(
                        MetricSignal.platform_metric_id.in_(metric_ids),
                        MetricSignal.metric_name.in_(
                            ("novice_ratio", "fomo_ratio", "post_count")
                        ),
                    )
                    .order_by(MetricSignal.id)
                ).all()
                if metric_ids
                else []
            )
            signals_by_metric: dict[int, dict[str, MetricSignal]] = {}
            for signal in signal_rows:
                signals_by_metric.setdefault(signal.platform_metric_id, {})[
                    signal.metric_name
                ] = signal
            result = []
            for row in rows:
                signal_map = signals_by_metric.get(row.id, {})
                result.append(
                    {
                        "id": row.id,
                        "bucket_at": as_utc(row.bucket_at),
                        "bucket_size": row.bucket_size,
                        "source_id": row.source_id,
                        "topic_id": row.topic_id,
                        "asset_id": row.asset_id,
                        "post_count": row.post_count,
                        "comment_count": row.comment_count,
                        "unique_author_count": row.unique_author_count,
                        "retail_count": row.retail_count,
                        "novice_count": row.novice_count,
                        "fomo_count": row.fomo_count,
                        "panic_count": row.panic_count,
                        "engagement_sum": row.engagement_sum,
                        "novice_percentile": signal_map.get("novice_ratio").percentile
                        if signal_map.get("novice_ratio")
                        else None,
                        "fomo_percentile": signal_map.get("fomo_ratio").percentile
                        if signal_map.get("fomo_ratio")
                        else None,
                        "attention_percentile": signal_map.get("post_count").percentile
                        if signal_map.get("post_count")
                        else None,
                    }
                )
            return result

    @app.get("/diffusion")
    def diffusion(limit: int = Query(200, ge=1, le=5000)):
        with _session(engine) as session:
            rows = session.scalars(
                select(DiffusionEvent).order_by(DiffusionEvent.started_at.desc()).limit(limit)
            ).all()
            result = []
            for row in rows:
                duration = (
                    (row.ended_at - row.started_at).total_seconds()
                    if row.ended_at is not None
                    else None
                )
                result.append(
                    {
                        "id": row.id,
                        "topic_id": row.topic_id,
                        "asset_id": row.asset_id,
                        "started_at": as_utc(row.started_at),
                        "ended_at": as_utc(row.ended_at),
                        "first_source_id": row.first_source_id,
                        "platform_count": row.platform_count,
                        "platform_breadth": row.platform_count,
                        "diffusion_duration_seconds": duration,
                        "diffusion_velocity": row.platform_count / (duration / 86400)
                        if duration and duration > 0
                        else None,
                        "retail_lag": None,
                        "source_sequence": row.source_sequence,
                        "rule_version": row.rule_version,
                    }
                )
            return result

    @app.get("/events")
    def events(
        topic_id: int | None = None,
        event_type: str | None = None,
        limit: int = Query(200, ge=1, le=5000),
    ):
        with _session(engine) as session:
            query = select(SignalEvent)
            if topic_id is not None:
                query = query.where(SignalEvent.topic_id == topic_id)
            if event_type is not None:
                query = query.where(SignalEvent.event_type == event_type)
            rows = session.scalars(query.order_by(SignalEvent.started_at.desc()).limit(limit)).all()
            event_ids = [row.id for row in rows]
            links = (
                session.scalars(
                    select(EventMetricLink)
                    .where(EventMetricLink.event_id.in_(event_ids))
                    .order_by(EventMetricLink.id)
                ).all()
                if event_ids
                else []
            )
            signal_ids = {link.metric_signal_id for link in links}
            signals_by_id = (
                {
                    signal.id: signal
                    for signal in session.scalars(
                        select(MetricSignal).where(MetricSignal.id.in_(signal_ids))
                    ).all()
                }
                if signal_ids
                else {}
            )
            metric_ids = {signal.platform_metric_id for signal in signals_by_id.values()}
            metrics_by_id = (
                {
                    metric.id: metric
                    for metric in session.scalars(
                        select(PlatformMetric).where(PlatformMetric.id.in_(metric_ids))
                    ).all()
                }
                if metric_ids
                else {}
            )
            returns = (
                session.scalars(select(EventReturn).where(EventReturn.event_id.in_(event_ids))).all()
                if event_ids
                else []
            )
            links_by_event: dict[int, list[EventMetricLink]] = {}
            for link in links:
                links_by_event.setdefault(link.event_id, []).append(link)
            returns_by_event: dict[int, list[EventReturn]] = {}
            for item in returns:
                returns_by_event.setdefault(item.event_id, []).append(item)
            return [
                _event_json(
                    session,
                    row,
                    include_raw=False,
                    link_rows=links_by_event.get(row.id, []),
                    signals_by_id=signals_by_id,
                    metrics_by_id=metrics_by_id,
                    return_rows=returns_by_event.get(row.id, []),
                )
                for row in rows
            ]

    @app.get("/events/{event_id}")
    def event_detail(event_id: int):
        with _session(engine) as session:
            event = session.get(SignalEvent, event_id)
            if event is None:
                raise HTTPException(status_code=404, detail="event not found")
            return _event_json(session, event, include_raw=True)

    @app.get("/research/event-study")
    def research_event_study(topic: str = "gold", event: str = "fomo_spike"):
        with _session(engine) as session:
            return event_study(session, topic_slug=topic, event_type=event, settings=settings)

    @app.get("/research/quantile-study")
    def research_quantile_study(
        topic: str = "gold",
        metric: str = "fomo_ratio",
        horizon: str = Query("5d", pattern="^(1d|3d|5d|10d|20d)$"),
    ):
        with _session(engine) as session:
            return quantile_study(
                session,
                topic_slug=topic,
                metric_name=metric,
                horizon=horizon,
                settings=settings,
            )

    @app.get("/sources/status")
    def sources_status():
        with _session(engine) as session:
            raw_stats = {
                source_id: {
                    "raw_observation_count": count,
                    "last_observed_at": as_utc(last_observed_at),
                }
                for source_id, count, last_observed_at in session.execute(
                    select(
                        RawObservation.source_id,
                        func.count(RawObservation.id),
                        func.max(RawObservation.observed_at),
                    ).group_by(RawObservation.source_id)
                ).all()
            }
            content_counts = dict(
                session.execute(
                    select(Content.source_id, func.count(Content.id)).group_by(Content.source_id)
                ).all()
            )
            trend_observation_counts = dict(
                session.execute(
                    select(
                        TrendObservation.source_id,
                        func.count(TrendObservation.id),
                    ).group_by(TrendObservation.source_id)
                ).all()
            )
            trend_signal_counts = dict(
                session.execute(
                    select(
                        TrendObservation.source_id,
                        func.count(TrendSignal.id),
                    )
                    .join(
                        TrendObservation,
                        TrendObservation.id == TrendSignal.trend_observation_id,
                    )
                    .group_by(TrendObservation.source_id)
                ).all()
            )
            archive_status_counts = dict(
                session.execute(
                    select(
                        ArchiveLookupState.status,
                        func.count(ArchiveLookupState.id),
                    ).group_by(ArchiveLookupState.status)
                ).all()
            )
            archive_snapshot_count = session.scalar(select(func.count(ArchiveSnapshot.id))) or 0
            archive_checked_count = (
                session.scalar(
                    select(func.count(ArchiveLookupState.id)).where(
                        ArchiveLookupState.checked_at.is_not(None)
                    )
                )
                or 0
            )
            result = []
            for source in session.scalars(select(Source).order_by(Source.name)).all():
                quality = session.scalars(
                    select(SourceQualityMetric)
                    .where(SourceQualityMetric.source_id == source.id)
                    .order_by(SourceQualityMetric.metric_date.desc())
                    .limit(20)
                ).all()
                evidence = {
                    **raw_stats.get(
                        source.id,
                        {"raw_observation_count": 0, "last_observed_at": None},
                    ),
                    "content_count": content_counts.get(source.id, 0),
                    "trend_observation_count": trend_observation_counts.get(source.id, 0),
                    "trend_signal_count": trend_signal_counts.get(source.id, 0),
                }
                if source.name == "common-crawl":
                    evidence.update(
                        {
                            "archive_snapshot_count": archive_snapshot_count,
                            "archive_checked_count": archive_checked_count,
                            "archive_status_counts": archive_status_counts,
                        }
                    )
                result.append(
                    {
                        "id": source.id,
                        "name": source.name,
                        "source_type": source.source_type,
                        "enabled": source.enabled,
                        "collector_version": source.collector_version,
                        "health_status": source.health_status,
                        "configuration": source_config_status(source.name, settings=settings),
                        "evidence": evidence,
                        "quality": [
                            {
                                "date": item.metric_date,
                                "name": item.metric_name,
                                "value": item.metric_value,
                            }
                            for item in quality
                        ],
                    }
                )
            return result

    @app.get("/config/status")
    def config_status():
        return {
            "mode": settings.data_mode,
            "enabled_sources": list(settings.enabled_sources),
            "source_concurrency": settings.source_concurrency,
            "sources": [
                source_config_status(source_name, settings=settings)
                for source_name in (
                    "guba",
                    "taoguba",
                    "zhihu",
                    "xiaohongshu",
                    "common-crawl",
                    "wikimedia-pageviews",
                )
            ],
            "market": market_config_status(settings),
            "llm": llm_config_status(settings),
        }

    return app


app = create_app()
