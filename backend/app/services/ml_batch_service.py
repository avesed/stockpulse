"""Batch DB read layer for AlphaForge ML training pipeline.

Multi-symbol batch queries and market-level aggregation.
All functions are read-only against existing tables.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Optional

from app.core.database import get_db_pool

logger = logging.getLogger(__name__)

MARKET_GROUPS: dict[str, tuple[str, ...]] = {
    "us": ("us",),
    "hk": ("hk",),
    "cn": ("sh", "sz", "bj"),
}


def _resolve_markets(market: str) -> list[str]:
    return list(MARKET_GROUPS.get(market.lower(), (market.lower(),)))


# ---------------------------------------------------------------------------
# Generic batch helper
# ---------------------------------------------------------------------------

async def _batch_query_grouped(
    table: str,
    columns: list[str],
    symbols: list[str],
    *,
    date_col: str = "date",
    start_date: date | None = None,
    end_date: date | None = None,
    limit_per_symbol: int = 500,
) -> dict[str, list[dict[str, Any]]]:
    """Batch fetch from a single table, grouped by symbol, limited per symbol.

    Uses ROW_NUMBER to cap rows per symbol (most recent first), then returns
    results in chronological order.
    """
    pool = get_db_pool()
    col_list = ", ".join(columns)
    params: list[Any] = [symbols]
    wheres = ["symbol = ANY($1)"]

    if start_date is not None:
        params.append(start_date)
        wheres.append(f"{date_col} >= ${len(params)}")
    if end_date is not None:
        params.append(end_date)
        wheres.append(f"{date_col} <= ${len(params)}")

    params.append(limit_per_symbol)
    where_sql = " AND ".join(wheres)
    limit_idx = len(params)

    sql = (
        f"SELECT symbol, {col_list} FROM ("
        f"  SELECT symbol, {col_list},"
        f"    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY {date_col} DESC) AS _rn"
        f"  FROM {table}"
        f"  WHERE {where_sql}"
        f") t WHERE _rn <= ${limit_idx}"
        f" ORDER BY symbol, {date_col}"
    )

    rows = await pool.fetch(sql, *params)
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        d = dict(r)
        sym = d.pop("symbol")
        d.pop("_rn", None)
        result[sym].append(d)
    return dict(result)


# ---------------------------------------------------------------------------
# 1. batch_financials: sec_financials + valuation_history
# ---------------------------------------------------------------------------

async def batch_financials(
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    limit_per_symbol: int = 500,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    sec = await _batch_query_grouped(
        "sec_financials",
        ["period", "form_type", "filed_date", "year", "quarter",
         "balance_sheet", "income_statement", "cash_flow", "data_source"],
        symbols,
        date_col="period",
        start_date=start_date, end_date=end_date,
        limit_per_symbol=limit_per_symbol,
    )
    val = await _batch_query_grouped(
        "valuation_history",
        ["date", "period_type", "pe_ratio", "pb_ratio", "ps_ratio",
         "ev_to_ebitda", "ev_to_revenue", "roe", "roa", "roic",
         "fcf_margin", "fcf_per_share", "net_margin", "operating_margin",
         "gross_margin", "debt_to_equity", "current_ratio", "quick_ratio",
         "payout_ratio", "book_value", "eps", "ev", "market_cap", "data_source"],
        symbols,
        date_col="date",
        start_date=start_date, end_date=end_date,
        limit_per_symbol=limit_per_symbol,
    )
    all_syms = set(sec) | set(val)
    return {
        sym: {"sec_filings": sec.get(sym, []), "valuation": val.get(sym, [])}
        for sym in all_syms
    }


# ---------------------------------------------------------------------------
# 2. batch_analyst: recommendation_trends + upgrades_downgrades
# ---------------------------------------------------------------------------

async def batch_analyst(
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    limit_per_symbol: int = 500,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    recs = await _batch_query_grouped(
        "recommendation_trends",
        ["period", "strong_buy", "buy", "hold", "sell", "strong_sell", "data_source"],
        symbols,
        date_col="period",
        start_date=start_date, end_date=end_date,
        limit_per_symbol=limit_per_symbol,
    )
    ugdg = await _batch_query_grouped(
        "upgrades_downgrades",
        ["date", "firm", "to_grade", "from_grade", "action", "data_source"],
        symbols,
        date_col="date",
        start_date=start_date, end_date=end_date,
        limit_per_symbol=limit_per_symbol,
    )
    all_syms = set(recs) | set(ugdg)
    return {
        sym: {
            "recommendations": recs.get(sym, []),
            "upgrades_downgrades": ugdg.get(sym, []),
        }
        for sym in all_syms
    }


# ---------------------------------------------------------------------------
# 3. batch_options: options_sentiment
# ---------------------------------------------------------------------------

async def batch_options(
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    limit_per_symbol: int = 500,
) -> dict[str, list[dict[str, Any]]]:
    return await _batch_query_grouped(
        "options_sentiment",
        ["date", "put_volume", "call_volume", "put_call_ratio",
         "put_oi", "call_oi", "put_call_oi_ratio", "data_source"],
        symbols,
        date_col="date",
        start_date=start_date, end_date=end_date,
        limit_per_symbol=limit_per_symbol,
    )


# ---------------------------------------------------------------------------
# 4. batch_short_interest
# ---------------------------------------------------------------------------

async def batch_short_interest(
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    limit_per_symbol: int = 500,
) -> dict[str, list[dict[str, Any]]]:
    return await _batch_query_grouped(
        "short_interest",
        ["date", "short_pct_float", "short_ratio", "shares_short",
         "shares_short_prior", "short_pct_shares_out", "data_source"],
        symbols,
        date_col="date",
        start_date=start_date, end_date=end_date,
        limit_per_symbol=limit_per_symbol,
    )


# ---------------------------------------------------------------------------
# 5. batch_insider: insider_transactions + insider_sentiment
# ---------------------------------------------------------------------------

async def batch_insider(
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    limit_per_symbol: int = 500,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    txns = await _batch_query_grouped(
        "insider_transactions",
        ["date", "insider_name", "title", "transaction_type",
         "shares", "value", "data_source"],
        symbols,
        date_col="date",
        start_date=start_date, end_date=end_date,
        limit_per_symbol=limit_per_symbol,
    )
    sent = await _batch_query_grouped(
        "insider_sentiment",
        ["month", "mspr", "change", "data_source"],
        symbols,
        date_col="month",
        start_date=start_date, end_date=end_date,
        limit_per_symbol=limit_per_symbol,
    )
    all_syms = set(txns) | set(sent)
    return {
        sym: {"transactions": txns.get(sym, []), "sentiment": sent.get(sym, [])}
        for sym in all_syms
    }


# ---------------------------------------------------------------------------
# 6. batch_earnings: earnings_surprises + earnings_calendar
# ---------------------------------------------------------------------------

async def batch_earnings(
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    limit_per_symbol: int = 500,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    surprises = await _batch_query_grouped(
        "earnings_surprises",
        ["period", "actual_eps", "estimated_eps", "surprise_pct", "data_source"],
        symbols,
        date_col="period",
        start_date=start_date, end_date=end_date,
        limit_per_symbol=limit_per_symbol,
    )
    cal = await _batch_query_grouped(
        "earnings_calendar",
        ["earnings_date", "eps_estimate", "eps_actual",
         "revenue_estimate", "revenue_actual", "quarter", "year", "data_source"],
        symbols,
        date_col="earnings_date",
        start_date=start_date, end_date=end_date,
        limit_per_symbol=limit_per_symbol,
    )
    all_syms = set(surprises) | set(cal)
    return {
        sym: {"surprises": surprises.get(sym, []), "calendar": cal.get(sym, [])}
        for sym in all_syms
    }


# ---------------------------------------------------------------------------
# 7. batch_sectors: stock_profiles (no time series)
# ---------------------------------------------------------------------------

async def batch_sectors(
    symbols: list[str],
) -> dict[str, dict[str, Any]]:
    pool = get_db_pool()
    rows = await pool.fetch(
        "SELECT symbol, market, name, sector, industry, concepts "
        "FROM stock_profiles WHERE symbol = ANY($1)",
        symbols,
    )
    return {
        r["symbol"]: {
            "market": r["market"],
            "name": r["name"],
            "sector": r["sector"],
            "industry": r["industry"],
            "concepts": r["concepts"],
        }
        for r in rows
    }


# ---------------------------------------------------------------------------
# Market aggregation endpoints
# ---------------------------------------------------------------------------

async def market_breadth(
    market: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    pool = get_db_pool()
    markets = _resolve_markets(market)
    lookback = start_date - timedelta(days=10)

    rows = await pool.fetch(
        """
        WITH daily_returns AS (
            SELECT symbol, date, close,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
            FROM stock_daily_bars
            WHERE market = ANY($1) AND date >= $2 AND date <= $3
        )
        SELECT date,
            COUNT(*) AS total_issues,
            COUNT(*) FILTER (WHERE close > prev_close) AS advancers,
            COUNT(*) FILTER (WHERE close < prev_close) AS decliners,
            COUNT(*) FILTER (WHERE close = prev_close) AS unchanged,
            ROUND(
                COUNT(*) FILTER (WHERE close > prev_close)::numeric /
                NULLIF(COUNT(*) FILTER (WHERE close < prev_close), 0), 4
            ) AS ad_ratio
        FROM daily_returns
        WHERE prev_close IS NOT NULL AND date >= $4
        GROUP BY date
        ORDER BY date
        """,
        markets, lookback, end_date, start_date,
    )
    return [dict(r) for r in rows]


async def market_volume(
    market: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    pool = get_db_pool()
    markets = _resolve_markets(market)
    lookback = start_date - timedelta(days=40)

    rows = await pool.fetch(
        """
        WITH daily_vol AS (
            SELECT date,
                SUM(volume) AS total_volume,
                SUM(close * volume) AS total_turnover,
                SUM(CASE WHEN close >= open THEN volume ELSE 0 END) AS up_volume,
                SUM(CASE WHEN close < open THEN volume ELSE 0 END) AS down_volume,
                COUNT(*) AS num_stocks
            FROM stock_daily_bars
            WHERE market = ANY($1) AND date >= $2 AND date <= $3
            GROUP BY date
        ),
        vol_ma AS (
            SELECT *,
                ROUND(AVG(total_volume) OVER (
                    ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                )) AS volume_ma20
            FROM daily_vol
        )
        SELECT date, total_volume, total_turnover, up_volume, down_volume,
               num_stocks, volume_ma20,
               ROUND(total_volume::numeric / NULLIF(volume_ma20, 0), 4) AS volume_ratio
        FROM vol_ma
        WHERE date >= $4
        ORDER BY date
        """,
        markets, lookback, end_date, start_date,
    )
    return [dict(r) for r in rows]


async def market_sector_returns(
    market: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    pool = get_db_pool()
    markets = _resolve_markets(market)
    lookback = start_date - timedelta(days=10)

    rows = await pool.fetch(
        """
        WITH returns AS (
            SELECT b.date, p.sector,
                (b.close - LAG(b.close) OVER (PARTITION BY b.symbol ORDER BY b.date)) /
                NULLIF(LAG(b.close) OVER (PARTITION BY b.symbol ORDER BY b.date), 0)
                    AS daily_return
            FROM stock_daily_bars b
            JOIN stock_profiles p ON b.symbol = p.symbol
            WHERE b.market = ANY($1) AND b.date >= $2 AND b.date <= $3
                AND p.sector IS NOT NULL AND p.sector != ''
        )
        SELECT date, sector,
            ROUND(AVG(daily_return)::numeric, 6) AS avg_return,
            ROUND(SUM(daily_return)::numeric, 6) AS total_return,
            COUNT(*) AS num_stocks
        FROM returns
        WHERE daily_return IS NOT NULL AND date >= $4
        GROUP BY date, sector
        ORDER BY date, sector
        """,
        markets, lookback, end_date, start_date,
    )
    return [dict(r) for r in rows]
