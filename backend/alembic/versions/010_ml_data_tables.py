"""Add ML data tables for valuation history, insider, earnings, macro, etc.

Revision ID: 010
Revises: 009
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- 1. valuation_history ---
    op.create_table(
        "valuation_history",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("period_type", sa.String(10), nullable=False),
        sa.Column("pe_ratio", sa.Float(), nullable=True),
        sa.Column("pb_ratio", sa.Float(), nullable=True),
        sa.Column("ps_ratio", sa.Float(), nullable=True),
        sa.Column("ev_to_ebitda", sa.Float(), nullable=True),
        sa.Column("ev_to_revenue", sa.Float(), nullable=True),
        sa.Column("roe", sa.Float(), nullable=True),
        sa.Column("roa", sa.Float(), nullable=True),
        sa.Column("roic", sa.Float(), nullable=True),
        sa.Column("fcf_margin", sa.Float(), nullable=True),
        sa.Column("fcf_per_share", sa.Float(), nullable=True),
        sa.Column("net_margin", sa.Float(), nullable=True),
        sa.Column("operating_margin", sa.Float(), nullable=True),
        sa.Column("gross_margin", sa.Float(), nullable=True),
        sa.Column("debt_to_equity", sa.Float(), nullable=True),
        sa.Column("current_ratio", sa.Float(), nullable=True),
        sa.Column("quick_ratio", sa.Float(), nullable=True),
        sa.Column("payout_ratio", sa.Float(), nullable=True),
        sa.Column("book_value", sa.Float(), nullable=True),
        sa.Column("eps", sa.Float(), nullable=True),
        sa.Column("ev", sa.Float(53), nullable=True),
        sa.Column("market_cap", sa.Float(53), nullable=True),
        sa.Column("data_source", sa.String(30), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "date", "period_type", name="uq_valuation_symbol_date_type"),
    )
    op.create_index("ix_valuation_symbol_type_date", "valuation_history", ["symbol", "period_type", sa.text("date DESC")])
    op.create_index("ix_valuation_market", "valuation_history", ["market"])

    # --- 2. insider_transactions ---
    op.create_table(
        "insider_transactions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("insider_name", sa.String(200), nullable=True),
        sa.Column("title", sa.String(200), nullable=True),
        sa.Column("transaction_type", sa.String(50), nullable=True),
        sa.Column("shares", sa.BigInteger(), nullable=True),
        sa.Column("value", sa.Float(53), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="yfinance", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "date", "insider_name", "transaction_type", name="uq_insider_tx"),
    )
    op.create_index("ix_insider_tx_symbol_date", "insider_transactions", ["symbol", sa.text("date DESC")])

    # --- 3. insider_sentiment ---
    op.create_table(
        "insider_sentiment",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("month", sa.Date(), nullable=False),
        sa.Column("mspr", sa.Float(), nullable=True),
        sa.Column("change", sa.Float(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="finnhub", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "month", name="uq_insider_sentiment_symbol_month"),
    )
    op.create_index("ix_insider_sentiment_symbol", "insider_sentiment", ["symbol", sa.text("month DESC")])

    # --- 4. earnings_surprises ---
    op.create_table(
        "earnings_surprises",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("actual_eps", sa.Float(), nullable=True),
        sa.Column("estimated_eps", sa.Float(), nullable=True),
        sa.Column("surprise_pct", sa.Float(), nullable=True),
        sa.Column("data_source", sa.String(30), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "period", name="uq_earnings_surprise_symbol_period"),
    )
    op.create_index("ix_earnings_surprise_symbol", "earnings_surprises", ["symbol", sa.text("period DESC")])

    # --- 5. recommendation_trends ---
    op.create_table(
        "recommendation_trends",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("strong_buy", sa.Integer(), nullable=True),
        sa.Column("buy", sa.Integer(), nullable=True),
        sa.Column("hold", sa.Integer(), nullable=True),
        sa.Column("sell", sa.Integer(), nullable=True),
        sa.Column("strong_sell", sa.Integer(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="finnhub", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "period", name="uq_rec_trends_symbol_period"),
    )
    op.create_index("ix_rec_trends_symbol", "recommendation_trends", ["symbol", sa.text("period DESC")])

    # --- 6. upgrades_downgrades ---
    op.create_table(
        "upgrades_downgrades",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("firm", sa.String(200), nullable=True),
        sa.Column("to_grade", sa.String(50), nullable=True),
        sa.Column("from_grade", sa.String(50), nullable=True),
        sa.Column("action", sa.String(50), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="yfinance", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "date", "firm", name="uq_upgrades_symbol_date_firm"),
    )
    op.create_index("ix_upgrades_symbol_date", "upgrades_downgrades", ["symbol", sa.text("date DESC")])

    # --- 7. sec_financials ---
    op.create_table(
        "sec_financials",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("form_type", sa.String(10), nullable=False),
        sa.Column("filed_date", sa.Date(), nullable=True),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("quarter", sa.Integer(), nullable=True),
        sa.Column("balance_sheet", JSONB(), nullable=True),
        sa.Column("income_statement", JSONB(), nullable=True),
        sa.Column("cash_flow", JSONB(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="finnhub", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "period", "form_type", name="uq_sec_fin_symbol_period_form"),
    )
    op.create_index("ix_sec_fin_symbol_period", "sec_financials", ["symbol", sa.text("period DESC")])

    # --- 8. earnings_calendar ---
    op.create_table(
        "earnings_calendar",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("earnings_date", sa.Date(), nullable=False),
        sa.Column("eps_estimate", sa.Float(), nullable=True),
        sa.Column("eps_actual", sa.Float(), nullable=True),
        sa.Column("revenue_estimate", sa.Float(53), nullable=True),
        sa.Column("revenue_actual", sa.Float(53), nullable=True),
        sa.Column("quarter", sa.String(10), nullable=True),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="finnhub", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "earnings_date", name="uq_earnings_cal_symbol_date"),
    )
    op.create_index("ix_earnings_cal_date", "earnings_calendar", [sa.text("earnings_date DESC")])
    op.create_index("ix_earnings_cal_symbol", "earnings_calendar", ["symbol"])

    # --- 9. options_sentiment ---
    op.create_table(
        "options_sentiment",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("put_volume", sa.BigInteger(), nullable=True),
        sa.Column("call_volume", sa.BigInteger(), nullable=True),
        sa.Column("put_call_ratio", sa.Float(), nullable=True),
        sa.Column("put_oi", sa.BigInteger(), nullable=True),
        sa.Column("call_oi", sa.BigInteger(), nullable=True),
        sa.Column("put_call_oi_ratio", sa.Float(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="yfinance", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "date", name="uq_options_sentiment_symbol_date"),
    )
    op.create_index("ix_options_symbol_date", "options_sentiment", ["symbol", sa.text("date DESC")])

    # --- 10. macro_daily ---
    op.create_table(
        "macro_daily",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("ticker", sa.String(20), nullable=False),
        sa.Column("indicator_name", sa.String(50), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("open", sa.Float(), nullable=True),
        sa.Column("high", sa.Float(), nullable=True),
        sa.Column("low", sa.Float(), nullable=True),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="yfinance", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker", "date", name="uq_macro_daily_ticker_date"),
    )
    op.create_index("ix_macro_daily_ticker_date", "macro_daily", ["ticker", sa.text("date DESC")])

    # --- 11. economic_indicators ---
    op.create_table(
        "economic_indicators",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("indicator_code", sa.String(50), nullable=False),
        sa.Column("indicator_name", sa.String(100), nullable=True),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="finnhub", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("indicator_code", "date", name="uq_econ_code_date"),
    )
    op.create_index("ix_econ_code_date", "economic_indicators", ["indicator_code", sa.text("date DESC")])

    # --- 12. cn_alternative_data ---
    op.create_table(
        "cn_alternative_data",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=True),
        sa.Column("data_type", sa.String(30), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("data", JSONB(), nullable=False),
        sa.Column("data_source", sa.String(30), server_default="akshare", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "data_type", "date", name="uq_cn_alt_symbol_type_date"),
    )
    op.create_index("ix_cn_alt_type_date", "cn_alternative_data", ["data_type", sa.text("date DESC")])
    op.create_index("ix_cn_alt_symbol", "cn_alternative_data", ["symbol"])

    # --- 13. short_interest ---
    op.create_table(
        "short_interest",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("short_pct_float", sa.Float(), nullable=True),
        sa.Column("short_ratio", sa.Float(), nullable=True),
        sa.Column("shares_short", sa.BigInteger(), nullable=True),
        sa.Column("shares_short_prior", sa.BigInteger(), nullable=True),
        sa.Column("short_pct_shares_out", sa.Float(), nullable=True),
        sa.Column("data_source", sa.String(30), server_default="yfinance", nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "date", name="uq_short_interest_symbol_date"),
    )
    op.create_index("ix_short_interest_symbol_date", "short_interest", ["symbol", sa.text("date DESC")])


def downgrade() -> None:
    op.drop_table("short_interest")
    op.drop_table("cn_alternative_data")
    op.drop_table("economic_indicators")
    op.drop_table("macro_daily")
    op.drop_table("options_sentiment")
    op.drop_table("earnings_calendar")
    op.drop_table("sec_financials")
    op.drop_table("upgrades_downgrades")
    op.drop_table("recommendation_trends")
    op.drop_table("earnings_surprises")
    op.drop_table("insider_sentiment")
    op.drop_table("insider_transactions")
    op.drop_table("valuation_history")
