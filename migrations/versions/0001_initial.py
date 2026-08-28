"""initial RetailTide V0 schema

Revision ID: 0001_initial
Revises:
"""

from alembic import op

from retail_tide.models import Base

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The declarative schema is the single source of truth for V0. Keeping this
    # migration compact also makes SQLite acceptance runs and PostgreSQL deploys
    # use exactly the same constraints.
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    Base.metadata.drop_all(bind=op.get_bind())
