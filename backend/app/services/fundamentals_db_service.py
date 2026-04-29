"""DB-first fundamental data reads for the financials endpoint.

Queries the stock_fundamentals table (populated by data-processor's
fundamental_service) to serve financial metrics without calling live
external APIs. Falls back to None (triggering live API fallback in the
caller) when data is stale (>48h) or unavailable.

Field naming: The DB uses snake_case column names that differ slightly
from the FinancialsData API model. This module handles the mapping:
  - pb_ratio -> price_to_book
  - revenue_growth_yoy -> revenue_growth
  - All other columns map by direct name match.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from app.core.database import get_db_pool

logger = logging.getLogger(__name__)

# Maximum age (in days) for DB data to be considered fresh.
# 2 days covers weekends (Friday collection valid through Sunday).
_FRESHNESS_DAYS = 2

_QUERY_SQL = """
    SELECT symbol, market, date, pe_ratio, pb_ratio, roe, roa,
           profit_margin, gross_margin, revenue, revenue_growth_yoy,
           net_income, eps, debt_to_equity, current_ratio,
           dividend_yield,
           forward_pe, dividend_rate, book_value,
           operating_margin, payout_ratio, eps_growth
    FROM stock_fundamentals
    WHERE symbol = $1
      AND record_type = 'daily_snapshot'
    ORDER BY date DESC
    LIMIT 1
"""

# DB column -> FinancialsData field mapping (only where names differ)
_FIELD_RENAMES = {
    "pb_ratio": "price_to_book",
    "revenue_growth_yoy": "revenue_growth",
}


async def get_financials_from_db(symbol: str) -> Optional[dict[str, Any]]:
    """Query the latest fundamental snapshot for a symbol.

    Returns a dict compatible with FinancialsData constructor if data is
    fresh (<=2 days old), or None to trigger live API fallback.

    The caller should catch exceptions; this function only returns None
    on expected conditions (no data, stale data, pool unavailable).
    """
    try:
        pool = get_db_pool()
    except RuntimeError:
        logger.debug("DB pool not available for fundamentals query")
        return None

    try:
        async with pool.acquire(timeout=5) as conn:
            row = await conn.fetchrow(_QUERY_SQL, symbol)
    except Exception as e:
        logger.warning("Failed to query stock_fundamentals for %s: %s", symbol, e)
        return None

    if row is None:
        logger.debug("No fundamental data in DB for %s", symbol)
        return None

    # Freshness check
    record_date = row["date"]
    if record_date is None:
        logger.warning("Fundamental record for %s has no date, treating as stale", symbol)
        return None
    if isinstance(record_date, date):
        age = (date.today() - record_date).days
        if age > _FRESHNESS_DAYS:
            logger.debug(
                "Fundamental data for %s is stale (%d days old, limit %d)",
                symbol, age, _FRESHNESS_DAYS,
            )
            return None

    # Build result dict with FinancialsData field names
    # Note: ps_ratio and market_cap are stored in DB but not exposed via
    # FinancialsData (served by QuoteData/InfoData instead), so we skip them.
    result: dict[str, Any] = {"symbol": symbol, "source": "db"}

    for key in (
        "pe_ratio", "pb_ratio", "roe", "roa",
        "profit_margin", "gross_margin", "revenue", "revenue_growth_yoy",
        "net_income", "eps", "debt_to_equity", "current_ratio",
        "dividend_yield",
        "forward_pe", "dividend_rate", "book_value",
        "operating_margin", "payout_ratio", "eps_growth",
    ):
        value = row[key]
        # Convert Decimal to float for JSON serialization
        if value is not None:
            value = float(value)
        # Apply field rename if needed
        out_key = _FIELD_RENAMES.get(key, key)
        result[out_key] = value

    if row["market"]:
        result["market"] = row["market"]

    return result


# ---------------------------------------------------------------------------
# Analyst ratings from DB
# ---------------------------------------------------------------------------

_ANALYST_QUERY = """
    SELECT symbol, market, date, recommendation, recommendation_mean,
           target_mean_price, target_high_price, target_low_price,
           target_median_price, number_of_analysts,
           current_price, upside_pct, data_source, updated_at
    FROM analyst_ratings
    WHERE symbol = $1
    ORDER BY date DESC
    LIMIT 1
"""


async def get_analyst_ratings_from_db(symbol: str) -> Optional[dict[str, Any]]:
    """Query the latest analyst rating for a symbol.

    Returns a dict compatible with AnalystRatingsData, or None.
    """
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None

    try:
        async with pool.acquire(timeout=5) as conn:
            row = await conn.fetchrow(_ANALYST_QUERY, symbol)
    except Exception as e:
        logger.warning("Failed to query analyst_ratings for %s: %s", symbol, e)
        return None

    if row is None:
        return None

    # Freshness check — 7 days for analyst ratings
    if row["updated_at"]:
        from datetime import datetime, timezone
        age = (datetime.now(timezone.utc) - row["updated_at"]).days
        if age > 7:
            return None

    return {
        "symbol": row["symbol"],
        "market": row["market"],
        "recommendation": row["recommendation"],
        "recommendation_mean": float(row["recommendation_mean"]) if row["recommendation_mean"] else None,
        "target_mean_price": float(row["target_mean_price"]) if row["target_mean_price"] else None,
        "target_high_price": float(row["target_high_price"]) if row["target_high_price"] else None,
        "target_low_price": float(row["target_low_price"]) if row["target_low_price"] else None,
        "target_median_price": float(row["target_median_price"]) if row["target_median_price"] else None,
        "number_of_analysts": row["number_of_analysts"],
        "current_price": float(row["current_price"]) if row["current_price"] else None,
        "upside_pct": float(row["upside_pct"]) if row["upside_pct"] else None,
        "source": "db",
    }


# ---------------------------------------------------------------------------
# Northbound holdings from DB
# ---------------------------------------------------------------------------

_NORTHBOUND_QUERY = """
    SELECT symbol, date, close_price, holding_shares,
           holding_value, holding_pct, change_shares
    FROM northbound_holdings
    WHERE symbol = $1
    ORDER BY date DESC
    LIMIT $2
"""


async def get_northbound_holdings_from_db(
    symbol: str, days: int = 30,
) -> Optional[list[dict[str, Any]]]:
    """Query recent northbound holding data for a CN symbol."""
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None

    try:
        async with pool.acquire(timeout=5) as conn:
            rows = await conn.fetch(_NORTHBOUND_QUERY, symbol, days)
    except Exception as e:
        logger.warning("Failed to query northbound_holdings for %s: %s", symbol, e)
        return None

    if not rows:
        return None

    return [
        {
            "date": str(r["date"]),
            "close_price": float(r["close_price"]) if r["close_price"] else None,
            "holding_shares": r["holding_shares"],
            "holding_value": float(r["holding_value"]) if r["holding_value"] else None,
            "holding_pct": float(r["holding_pct"]) if r["holding_pct"] else None,
            "change_shares": float(r["change_shares"]) if r["change_shares"] else None,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Institutional holders from DB
# ---------------------------------------------------------------------------

async def get_institutional_holders_from_db(symbol: str) -> Optional[dict[str, Any]]:
    """Query institutional holders for a symbol."""
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None

    try:
        async with pool.acquire(timeout=5) as conn:
            rows = await conn.fetch(
                "SELECT holder, date_reported, pct_held, shares, value, pct_change "
                "FROM institutional_holders WHERE symbol = $1 "
                "ORDER BY pct_held DESC NULLS LAST LIMIT 50",
                symbol,
            )
    except Exception as e:
        logger.warning("Failed to query institutional_holders for %s: %s", symbol, e)
        return None

    if not rows:
        return None

    holders = []
    total_pct = 0.0
    for r in rows:
        pct = float(r["pct_held"]) if r["pct_held"] else None
        if pct:
            total_pct += pct
        holders.append({
            "holder": r["holder"],
            "date_reported": str(r["date_reported"]) if r["date_reported"] else None,
            "pct_held": pct,
            "shares": r["shares"],
            "value": r["value"],
            "pct_change": float(r["pct_change"]) if r["pct_change"] else None,
        })

    return {
        "symbol": symbol,
        "holders": holders,
        "total_institutional_pct": round(total_pct, 4),
        "source": "db",
    }


# ---------------------------------------------------------------------------
# Fund holdings from DB
# ---------------------------------------------------------------------------

async def get_fund_holdings_from_db(symbol: str) -> Optional[dict[str, Any]]:
    """Query latest fund holdings for a CN symbol."""
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None

    try:
        async with pool.acquire(timeout=5) as conn:
            row = await conn.fetchrow(
                "SELECT quarter, institution_count, institution_count_change, "
                "holding_pct, holding_pct_change, float_pct, float_pct_change "
                "FROM fund_holdings WHERE symbol = $1 "
                "ORDER BY quarter DESC LIMIT 1",
                symbol,
            )
    except Exception as e:
        logger.warning("Failed to query fund_holdings for %s: %s", symbol, e)
        return None

    if row is None:
        return None

    return {
        "symbol": symbol,
        "quarter": row["quarter"],
        "institution_count": row["institution_count"],
        "institution_count_change": row["institution_count_change"],
        "holding_pct": float(row["holding_pct"]) if row["holding_pct"] else None,
        "holding_pct_change": float(row["holding_pct_change"]) if row["holding_pct_change"] else None,
        "float_pct": float(row["float_pct"]) if row["float_pct"] else None,
        "float_pct_change": float(row["float_pct_change"]) if row["float_pct_change"] else None,
        "source": "db",
    }


# ---------------------------------------------------------------------------
# Peers (industry-based matching from stock_profiles)
# ---------------------------------------------------------------------------

async def get_peers_from_db(symbol: str, limit: int = 20) -> Optional[dict[str, Any]]:
    """Find peer stocks by matching industry in stock_profiles."""
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None

    try:
        async with pool.acquire(timeout=5) as conn:
            # Get the target stock's profile
            target = await conn.fetchrow(
                "SELECT symbol, market, name, name_zh, sector, industry "
                "FROM stock_profiles WHERE symbol = $1",
                symbol,
            )
            if target is None or not target["industry"]:
                return None

            # Find peers in the same industry (excluding self)
            rows = await conn.fetch(
                "SELECT symbol, name, name_zh, market, sector, industry "
                "FROM stock_profiles "
                "WHERE industry = $1 AND symbol != $2 "
                "ORDER BY symbol LIMIT $3",
                target["industry"], symbol, limit,
            )

            peers = [
                {
                    "symbol": r["symbol"],
                    "name": r["name_zh"] or r["name"],
                    "market": r["market"],
                    "sector": r["sector"],
                    "industry": r["industry"],
                }
                for r in rows
            ]

            return {
                "symbol": symbol,
                "industry": target["industry"],
                "sector": target["sector"],
                "peers": peers,
                "source": "db",
            }
    except Exception as e:
        logger.warning("Failed to query peers for %s: %s", symbol, e)
        return None
