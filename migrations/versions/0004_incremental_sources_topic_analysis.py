"""add incremental source state, archive enrichment and topic analysis"""

from alembic import op
from sqlalchemy import Boolean, Column, Float, Integer, String, inspect

from retail_tide.models import (
    AnalysisTask,
    ArchiveLookupState,
    ArchiveSnapshot,
    CollectionCheckpoint,
    ContentAnalysis,
    TrendSignal,
)

revision = "0004_incremental_sources_topic_analysis"
down_revision = "0003_content_analysis_review"
branch_labels = None
depends_on = None


def _add_column_if_missing(table: str, column: Column) -> None:
    bind = op.get_bind()
    if column.name not in {item["name"] for item in inspect(bind).get_columns(table)}:
        op.add_column(table, column)


def upgrade() -> None:
    bind = op.get_bind()
    _add_column_if_missing(
        ContentAnalysis.__tablename__,
        Column("topic_id", Integer, nullable=True),
    )
    _add_column_if_missing(
        ContentAnalysis.__tablename__,
        Column("input_hash", String(64), nullable=False, server_default=""),
    )
    _add_column_if_missing(
        ContentAnalysis.__tablename__,
        Column("promotion", Boolean, nullable=False, server_default="0"),
    )
    _add_column_if_missing(
        ContentAnalysis.__tablename__,
        Column("promotion_confidence", Float, nullable=False, server_default="0"),
    )
    for model in (
        CollectionCheckpoint,
        ArchiveSnapshot,
        ArchiveLookupState,
        AnalysisTask,
        TrendSignal,
    ):
        if not inspect(bind).has_table(model.__tablename__):
            model.__table__.create(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    for model in (
        TrendSignal,
        AnalysisTask,
        ArchiveLookupState,
        ArchiveSnapshot,
        CollectionCheckpoint,
    ):
        if inspect(bind).has_table(model.__tablename__):
            model.__table__.drop(bind=bind)
