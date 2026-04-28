"""Add collection_runs audit table.

Revision ID: 004
Revises: 003
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "collection_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("run_type", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="running"),
        sa.Column("symbols_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("symbols_done", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("new_bars", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("errors_json", JSONB(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.Column("triggered_by", sa.String(100), nullable=False, server_default="api"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_collection_runs_market_started",
        "collection_runs",
        ["market", sa.text("started_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_collection_runs_market_started", table_name="collection_runs")
    op.drop_table("collection_runs")
