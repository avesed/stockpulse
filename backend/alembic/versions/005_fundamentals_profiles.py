"""Add stock_fundamentals, stock_profiles, analyst_ratings, northbound_holdings.

Revision ID: 005
Revises: 004
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- stock_fundamentals ---
    op.create_table(
        "stock_fundamentals",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("record_type", sa.String(30), nullable=False, server_default="daily_snapshot"),
        sa.Column("pe_ratio", sa.Float(), nullable=True),
        sa.Column("pb_ratio", sa.Float(), nullable=True),
        sa.Column("roe", sa.Float(), nullable=True),
        sa.Column("roa", sa.Float(), nullable=True),
        sa.Column("profit_margin", sa.Float(), nullable=True),
        sa.Column("gross_margin", sa.Float(), nullable=True),
        sa.Column("operating_margin", sa.Float(), nullable=True),
        sa.Column("revenue", sa.Float(53), nullable=True),
        sa.Column("revenue_growth_yoy", sa.Float(), nullable=True),
        sa.Column("net_income", sa.Float(53), nullable=True),
        sa.Column("eps", sa.Float(), nullable=True),
        sa.Column("eps_growth", sa.Float(), nullable=True),
        sa.Column("debt_to_equity", sa.Float(), nullable=True),
        sa.Column("current_ratio", sa.Float(), nullable=True),
        sa.Column("dividend_yield", sa.Float(), nullable=True),
        sa.Column("dividend_rate", sa.Float(), nullable=True),
        sa.Column("forward_pe", sa.Float(), nullable=True),
        sa.Column("book_value", sa.Float(), nullable=True),
        sa.Column("payout_ratio", sa.Float(), nullable=True),
        sa.Column("data_source", sa.String(30), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "date", "record_type", name="uq_fundamentals_symbol_date_type"),
    )
    op.create_index(
        "ix_fundamentals_symbol_type_date",
        "stock_fundamentals",
        ["symbol", "record_type", sa.text("date DESC")],
    )
    op.create_index("ix_fundamentals_market", "stock_fundamentals", ["market"])

    # --- stock_profiles ---
    op.create_table(
        "stock_profiles",
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("name", sa.String(200), nullable=True),
        sa.Column("name_zh", sa.String(200), nullable=True),
        sa.Column("sector", sa.String(100), nullable=True),
        sa.Column("industry", sa.String(100), nullable=True),
        sa.Column("concepts", JSONB(), server_default="[]", nullable=False),
        sa.Column("main_business", sa.Text(), server_default="", nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("symbol"),
    )
    op.create_index("ix_profiles_market", "stock_profiles", ["market"])

    # --- analyst_ratings ---
    op.create_table(
        "analyst_ratings",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("recommendation", sa.String(30), nullable=True),
        sa.Column("recommendation_mean", sa.Float(), nullable=True),
        sa.Column("target_mean_price", sa.Float(), nullable=True),
        sa.Column("target_high_price", sa.Float(), nullable=True),
        sa.Column("target_low_price", sa.Float(), nullable=True),
        sa.Column("target_median_price", sa.Float(), nullable=True),
        sa.Column("number_of_analysts", sa.Integer(), nullable=True),
        sa.Column("current_price", sa.Float(), nullable=True),
        sa.Column("upside_pct", sa.Float(), nullable=True),
        sa.Column("data_source", sa.String(30), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", name="uq_analyst_ratings_symbol"),
    )
    op.create_index("ix_analyst_ratings_market", "analyst_ratings", ["market"])

    # --- northbound_holdings ---
    op.create_table(
        "northbound_holdings",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("close_price", sa.Float(), nullable=True),
        sa.Column("holding_shares", sa.BigInteger(), nullable=True),
        sa.Column("holding_value", sa.Float(), nullable=True),
        sa.Column("holding_pct", sa.Float(), nullable=True),
        sa.Column("change_shares", sa.Float(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="akshare", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "date", name="uq_northbound_symbol_date"),
    )
    op.create_index(
        "ix_northbound_symbol_date",
        "northbound_holdings",
        ["symbol", sa.text("date DESC")],
    )


def downgrade() -> None:
    op.drop_table("northbound_holdings")
    op.drop_table("analyst_ratings")
    op.drop_table("stock_profiles")
    op.drop_index("ix_fundamentals_symbol_type_date", table_name="stock_fundamentals")
    op.drop_index("ix_fundamentals_market", table_name="stock_fundamentals")
    op.drop_table("stock_fundamentals")
