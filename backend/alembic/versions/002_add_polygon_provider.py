"""Add Polygon.io provider to provider_configs.

Revision ID: 002
Revises: 001
"""
from alembic import op

revision = "002"
down_revision = "001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO provider_configs (provider_name, display_name, is_enabled)
        VALUES ('polygon', 'Polygon.io', false)
        ON CONFLICT (provider_name) DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute(
        "DELETE FROM provider_configs WHERE provider_name = 'polygon'"
    )
