"""Add institutional_holders and fund_holdings tables.

Revision ID: 007
Revises: 006
"""
import sqlalchemy as sa
from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- institutional_holders (US/HK, from yfinance) ---
    op.create_table(
        "institutional_holders",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("holder", sa.String(200), nullable=False),
        sa.Column("date_reported", sa.Date(), nullable=True),
        sa.Column("pct_held", sa.Float(), nullable=True),
        sa.Column("shares", sa.BigInteger(), nullable=True),
        sa.Column("value", sa.BigInteger(), nullable=True),
        sa.Column("pct_change", sa.Float(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="yfinance", nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "holder", name="uq_inst_holders_symbol_holder"),
    )
    op.create_index("ix_inst_holders_symbol", "institutional_holders", ["symbol"])
    op.create_index("ix_inst_holders_market", "institutional_holders", ["market"])

    # --- fund_holdings (CN, from akshare) ---
    op.create_table(
        "fund_holdings",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("market", sa.String(10), nullable=False, server_default="cn"),
        sa.Column("quarter", sa.String(10), nullable=False),
        sa.Column("institution_count", sa.Integer(), nullable=True),
        sa.Column("institution_count_change", sa.Integer(), nullable=True),
        sa.Column("holding_pct", sa.Float(), nullable=True),
        sa.Column("holding_pct_change", sa.Float(), nullable=True),
        sa.Column("float_pct", sa.Float(), nullable=True),
        sa.Column("float_pct_change", sa.Float(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="akshare", nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "quarter", name="uq_fund_holdings_symbol_quarter"),
    )
    op.create_index("ix_fund_holdings_symbol", "fund_holdings", ["symbol"])


def downgrade() -> None:
    op.drop_table("fund_holdings")
    op.drop_table("institutional_holders")
