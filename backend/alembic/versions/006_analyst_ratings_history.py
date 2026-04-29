"""Analyst ratings: add date column, change unique to (symbol, date) for history retention.

Revision ID: 006
Revises: 005
"""
import sqlalchemy as sa
from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add date column
    op.add_column("analyst_ratings", sa.Column("date", sa.Date(), nullable=True))

    # Backfill existing rows: set date from updated_at
    op.execute("UPDATE analyst_ratings SET date = updated_at::date WHERE date IS NULL")

    # Make date NOT NULL
    op.alter_column("analyst_ratings", "date", nullable=False)

    # Drop old unique constraint (symbol only)
    op.drop_constraint("uq_analyst_ratings_symbol", "analyst_ratings", type_="unique")

    # Add new unique constraint (symbol + date) for history
    op.create_unique_constraint("uq_analyst_ratings_symbol_date", "analyst_ratings", ["symbol", "date"])

    # Add index for queries
    op.create_index("ix_analyst_ratings_symbol_date", "analyst_ratings", ["symbol", sa.text("date DESC")])


def downgrade() -> None:
    op.drop_index("ix_analyst_ratings_symbol_date", table_name="analyst_ratings")
    op.drop_constraint("uq_analyst_ratings_symbol_date", "analyst_ratings", type_="unique")
    op.create_unique_constraint("uq_analyst_ratings_symbol", "analyst_ratings", ["symbol"])
    op.drop_column("analyst_ratings", "date")
