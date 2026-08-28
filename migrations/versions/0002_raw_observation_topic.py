"""record collection-query topic matches

Revision ID: 0002_raw_observation_topic
Revises: 0001_initial
"""

from alembic import op
from sqlalchemy import inspect

from retail_tide.models import RawObservationTopic

revision = "0002_raw_observation_topic"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 0001 uses metadata.create_all, so a fresh install may already include
    # this table by the time this incremental migration runs.
    bind = op.get_bind()
    if not inspect(bind).has_table(RawObservationTopic.__tablename__):
        RawObservationTopic.__table__.create(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    if inspect(bind).has_table(RawObservationTopic.__tablename__):
        RawObservationTopic.__table__.drop(bind=bind)
