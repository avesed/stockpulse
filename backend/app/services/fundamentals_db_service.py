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
