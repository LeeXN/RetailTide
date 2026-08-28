"""store inspectable evidence for semantic reviews

Revision ID: 0003_content_analysis_review
Revises: 0002_raw_observation_topic
"""

from alembic import op
from sqlalchemy import inspect

from retail_tide.models import ContentAnalysisReview

revision = "0003_content_analysis_review"
down_revision = "0002_raw_observation_topic"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if not inspect(bind).has_table(ContentAnalysisReview.__tablename__):
        ContentAnalysisReview.__table__.create(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    if inspect(bind).has_table(ContentAnalysisReview.__tablename__):
        ContentAnalysisReview.__table__.drop(bind=bind)
