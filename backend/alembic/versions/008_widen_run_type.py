"""Widen collection_runs.run_type to 30 chars for longer job type names.

Revision ID: 008
Revises: 007
"""
import sqlalchemy as sa
from alembic import op

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("collection_runs", "run_type", type_=sa.String(30))


def downgrade() -> None:
    op.alter_column("collection_runs", "run_type", type_=sa.String(20))
