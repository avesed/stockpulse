"""Tune autovacuum for high-churn tables.

api_consumers: frequent last_used_at updates.
institutional_holders: bulk ON CONFLICT upserts.

Revision ID: 009
Revises: 008
"""
from alembic import op

revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE api_consumers SET (
            autovacuum_vacuum_scale_factor = 0.01,
            autovacuum_analyze_scale_factor = 0.02,
            autovacuum_vacuum_threshold = 20
        )
    """)
    op.execute("""
        ALTER TABLE institutional_holders SET (
            autovacuum_vacuum_scale_factor = 0.05,
            autovacuum_analyze_scale_factor = 0.02,
            autovacuum_vacuum_threshold = 50
        )
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE api_consumers RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor, autovacuum_vacuum_threshold)")
    op.execute("ALTER TABLE institutional_holders RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor, autovacuum_vacuum_threshold)")
