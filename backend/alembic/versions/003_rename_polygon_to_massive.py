"""Rename polygon provider to massive.

Revision ID: 003
Revises: 002
"""
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE provider_configs
        SET provider_name = 'massive',
            display_name = 'Massive'
        WHERE provider_name = 'polygon'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE provider_configs
        SET provider_name = 'polygon',
            display_name = 'Polygon.io'
        WHERE provider_name = 'massive'
        """
    )
