"""Initial schema — all StockPulse tables.

Revision ID: 001_initial_schema
Create Date: 2026-04-27
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # users — admin users for the management UI
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("email", sa.String(255), nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("display_name", sa.String(100), nullable=True),
        sa.Column("role", sa.String(20), nullable=False, server_default="user"),
        sa.Column("locale", sa.String(10), nullable=False, server_default="zh"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    # api_consumers — machine consumers (e.g. WebStock, data-processor)
    op.create_table(
        "api_consumers",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("api_key", sa.String(128), nullable=False),
        sa.Column("api_key_prefix", sa.String(8), nullable=False),
        sa.Column("description", sa.String(500), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("rate_limit", sa.Integer(), nullable=False, server_default=sa.text("100")),
        sa.Column("allowed_endpoints", postgresql.ARRAY(sa.String(200)), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_api_consumers_name", "api_consumers", ["name"], unique=True)
    op.create_index("ix_api_consumers_api_key", "api_consumers", ["api_key"], unique=True)

    # system_settings — key/value store for JWT secrets, etc.
    op.create_table(
        "system_settings",
        sa.Column("key", sa.String(128), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("key"),
    )

    # provider_configs — data source configuration and health tracking
    op.create_table(
        "provider_configs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("provider_name", sa.String(50), nullable=False),
        sa.Column("display_name", sa.String(100), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("api_key", sa.Text(), nullable=True),
        sa.Column("config_json", postgresql.JSONB(), nullable=True),
        sa.Column("last_health_check", sa.DateTime(timezone=True), nullable=True),
        sa.Column("health_status", sa.String(20), nullable=False, server_default="unknown"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_provider_configs_name", "provider_configs", ["provider_name"], unique=True)

    # stock_daily_bars — OHLCV daily bar data
    op.create_table(
        "stock_daily_bars",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("open", sa.Float(), nullable=True),
        sa.Column("high", sa.Float(), nullable=True),
        sa.Column("low", sa.Float(), nullable=True),
        sa.Column("close", sa.Float(), nullable=True),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("data_source", sa.String(30), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_stock_daily_bars_symbol_date", "stock_daily_bars", ["symbol", "date"], unique=True)
    op.create_index("ix_stock_daily_bars_market", "stock_daily_bars", ["market", "symbol", "date"])

    # stock_symbols — stock list with metadata
    op.create_table(
        "stock_symbols",
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("name", sa.String(200), nullable=True),
        sa.Column("name_zh", sa.String(200), nullable=True),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=True),
        sa.Column("stock_type", sa.String(20), nullable=True),
        sa.Column("pinyin", sa.String(200), nullable=True),
        sa.Column("pinyin_initial", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("symbol"),
    )
    op.create_index("ix_stock_symbols_market", "stock_symbols", ["market"])

    # Seed market data providers
    op.execute(
        """
        INSERT INTO provider_configs (provider_name, display_name, is_enabled) VALUES
        ('yfinance', 'Yahoo Finance', true),
        ('akshare', 'AKShare', true),
        ('finnhub', 'Finnhub', false),
        ('tiingo', 'Tiingo', false),
        ('tushare', 'Tushare', false)
        """
    )


def downgrade() -> None:
    op.drop_table("stock_symbols")
    op.drop_table("stock_daily_bars")
    op.drop_table("provider_configs")
    op.drop_table("system_settings")
    op.drop_table("api_consumers")
    op.drop_table("users")
