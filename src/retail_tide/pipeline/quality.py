from __future__ import annotations

from datetime import date

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import (
    Content,
    ContentAnalysis,
    ContentEntity,
    RawObservation,
    Source,
    SourceQualityMetric,
)
from ..time import now_utc
from .analysis import analysis_precedence_key

QUALITY_NAMES = (
    "source_success_rate",
    "items_collected",
    "duplicate_ratio",
    "parse_failure_ratio",
    "analysis_failure_ratio",
    "unknown_actor_ratio",
    "unknown_asset_ratio",
    "spam_ratio",
    "promotion_ratio",
    "collection_delay",
)


def refresh_source_quality(session: Session, *, metric_date: date | None = None) -> int:
    metric_date = metric_date or now_utc().date()
    created = 0
    for source in session.scalars(select(Source)).all():
        raw_count = (
            session.scalar(
                select(func.count(RawObservation.id)).where(RawObservation.source_id == source.id)
            )
            or 0
        )
        content_count = (
            session.scalar(select(func.count(Content.id)).where(Content.source_id == source.id))
            or 0
        )
        unique_raw_count = (
            session.scalar(
                select(func.count(func.distinct(RawObservation.source_item_id))).where(
                    RawObservation.source_id == source.id
                )
            )
            or 0
        )
        asset_content_count = (
            session.scalar(
                select(func.count(func.distinct(ContentEntity.content_id)))
                .join(Content, ContentEntity.content_id == Content.id)
                .where(Content.source_id == source.id, ContentEntity.entity_type == "asset")
            )
            or 0
        )
        analysis_rows = session.scalars(
            select(ContentAnalysis).join(Content).where(Content.source_id == source.id)
        ).all()
        # A content item can retain rule-based, compatible-LLM and Codex
        # analysis versions for traceability.  Source quality must describe the
        # effective result, not count every historical model run as another
        # analyzed post.
        analyses_by_content: dict[int, ContentAnalysis] = {}
        for analysis in analysis_rows:
            current = analyses_by_content.get(analysis.content_id)
            if current is None or analysis_precedence_key(analysis) > analysis_precedence_key(
                current
            ):
                analyses_by_content[analysis.content_id] = analysis
        analyses = list(analyses_by_content.values())
        analyzed_content_ids = set(analyses_by_content)
        values = {
            "source_success_rate": 1.0 if source.health_status == "healthy" else 0.0,
            "items_collected": float(raw_count),
            "duplicate_ratio": 1 - unique_raw_count / raw_count if raw_count else 0.0,
            # Trend/archive observations intentionally do not normalize into
            # Content, so the content parser ratio is not applicable to them.
            "parse_failure_ratio": 0.0
            if source.source_type != "content" or content_count or raw_count == 0
            else 1.0,
            "analysis_failure_ratio": 0.0
            if content_count == 0
            else max(0.0, 1 - len(analyzed_content_ids) / content_count),
            "unknown_actor_ratio": sum(a.actor_type == "unknown" for a in analyses) / len(analyses)
            if analyses
            else 0.0,
            "unknown_asset_ratio": 1 - asset_content_count / content_count
            if content_count
            else 0.0,
            "spam_ratio": sum(bool(a.spam) for a in analyses) / len(analyses) if analyses else 0.0,
            "promotion_ratio": (
                sum(bool(getattr(a, "promotion", False)) for a in analyses) / len(analyses)
                if analyses
                else 0.0
            ),
            "collection_delay": 0.0,
        }
        for name, value in values.items():
            row = session.scalar(
                select(SourceQualityMetric).where(
                    SourceQualityMetric.source_id == source.id,
                    SourceQualityMetric.metric_date == metric_date,
                    SourceQualityMetric.metric_name == name,
                )
            )
            if row is None:
                session.add(
                    SourceQualityMetric(
                        source_id=source.id,
                        metric_date=metric_date,
                        metric_name=name,
                        metric_value=float(value),
                        metadata_json={},
                        created_at=now_utc(),
                    )
                )
                created += 1
            else:
                row.metric_value = float(value)
    session.commit()
    return created
