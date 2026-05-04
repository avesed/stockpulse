"""DB read layer for ML data tables.

Provides read-only access to the 13 ML data tables created in migration 010.
Each function returns a dict envelope or None when data is unavailable.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from app.core.database import get_db_pool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. valuation_history
# ---------------------------------------------------------------------------

async def get_valuation_history(
    symbol: str, *, period_type: str = "quarterly", limit: int = 40,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT date, period_type, pe_ratio, pb_ratio, ps_ratio, "
            "ev_to_ebitda, ev_to_revenue, roe, roa, roic, "
            "fcf_margin, fcf_per_share, net_margin, operating_margin, "
            "gross_margin, debt_to_equity, current_ratio, quick_ratio, "
            "payout_ratio, book_value, eps, ev, market_cap, data_source "
            "FROM valuation_history "
            "WHERE symbol = $1 AND period_type = $2 "
            "ORDER BY date DESC LIMIT $3",
            symbol, period_type, limit,
        )
    except Exception as e:
        logger.warning("Failed to query valuation_history for %s: %s", symbol, e)
        return None
    if not rows:
        return None
    return {"symbol": symbol, "period_type": period_type, "data": [dict(r) for r in rows], "source": "db"}


# ---------------------------------------------------------------------------
# 2. insider_transactions
# ---------------------------------------------------------------------------

async def get_insider_transactions_from_db(
    symbol: str, *, limit: int = 100,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT date, insider_name, title, transaction_type, "
            "shares, value, data_source "
            "FROM insider_transactions "
            "WHERE symbol = $1 ORDER BY date DESC LIMIT $2",
            symbol, limit,
        )
    except Exception as e:
        logger.warning("Failed to query insider_transactions for %s: %s", symbol, e)
        return None
    if not rows:
        return None
    return {"symbol": symbol, "data": [dict(r) for r in rows], "source": "db"}


# ---------------------------------------------------------------------------
# 3. insider_sentiment
# ---------------------------------------------------------------------------

async def get_insider_sentiment_from_db(
    symbol: str, *, limit: int = 24,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT month, mspr, change, data_source "
            "FROM insider_sentiment "
            "WHERE symbol = $1 ORDER BY month DESC LIMIT $2",
            symbol, limit,
        )
    except Exception as e:
        logger.warning("Failed to query insider_sentiment for %s: %s", symbol, e)
        return None
    if not rows:
        return None
    return {"symbol": symbol, "data": [dict(r) for r in rows], "source": "db"}


# ---------------------------------------------------------------------------
# 4. earnings_surprises
# ---------------------------------------------------------------------------

async def get_earnings_surprises_from_db(
    symbol: str, *, limit: int = 20,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT period, actual_eps, estimated_eps, surprise_pct, data_source "
            "FROM earnings_surprises "
            "WHERE symbol = $1 ORDER BY period DESC LIMIT $2",
            symbol, limit,
        )
    except Exception as e:
        logger.warning("Failed to query earnings_surprises for %s: %s", symbol, e)
        return None
    if not rows:
        return None
    return {"symbol": symbol, "data": [dict(r) for r in rows], "source": "db"}


# ---------------------------------------------------------------------------
# 5. recommendation_trends
# ---------------------------------------------------------------------------

async def get_recommendation_trends_from_db(
    symbol: str, *, limit: int = 12,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT period, strong_buy, buy, hold, sell, strong_sell, data_source "
            "FROM recommendation_trends "
            "WHERE symbol = $1 ORDER BY period DESC LIMIT $2",
            symbol, limit,
        )
    except Exception as e:
        logger.warning("Failed to query recommendation_trends for %s: %s", symbol, e)
        return None
    if not rows:
        return None
    return {"symbol": symbol, "data": [dict(r) for r in rows], "source": "db"}


# ---------------------------------------------------------------------------
# 6. upgrades_downgrades
# ---------------------------------------------------------------------------

async def get_upgrades_downgrades_from_db(
    symbol: str, *, limit: int = 100,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT date, firm, to_grade, from_grade, action, data_source "
            "FROM upgrades_downgrades "
            "WHERE symbol = $1 ORDER BY date DESC LIMIT $2",
            symbol, limit,
        )
    except Exception as e:
        logger.warning("Failed to query upgrades_downgrades for %s: %s", symbol, e)
        return None
    if not rows:
        return None
    return {"symbol": symbol, "data": [dict(r) for r in rows], "source": "db"}


# ---------------------------------------------------------------------------
# 7. sec_financials
# ---------------------------------------------------------------------------

async def get_sec_financials_from_db(
    symbol: str, *, limit: int = 20,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT period, form_type, filed_date, year, quarter, "
            "balance_sheet, income_statement, cash_flow, data_source "
            "FROM sec_financials "
            "WHERE symbol = $1 ORDER BY period DESC LIMIT $2",
            symbol, limit,
        )
    except Exception as e:
        logger.warning("Failed to query sec_financials for %s: %s", symbol, e)
        return None
    if not rows:
        return None
    return {"symbol": symbol, "data": [dict(r) for r in rows], "source": "db"}


# ---------------------------------------------------------------------------
# 8. options_sentiment
# ---------------------------------------------------------------------------

async def get_options_sentiment_from_db(
    symbol: str, *, limit: int = 30,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT date, put_volume, call_volume, put_call_ratio, "
            "put_oi, call_oi, put_call_oi_ratio, data_source "
            "FROM options_sentiment "
            "WHERE symbol = $1 ORDER BY date DESC LIMIT $2",
            symbol, limit,
        )
    except Exception as e:
        logger.warning("Failed to query options_sentiment for %s: %s", symbol, e)
        return None
    if not rows:
        return None
    return {"symbol": symbol, "data": [dict(r) for r in rows], "source": "db"}


# ---------------------------------------------------------------------------
# 9. short_interest
# ---------------------------------------------------------------------------

async def get_short_interest_from_db(
    symbol: str, *, limit: int = 30,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT date, short_pct_float, short_ratio, shares_short, "
            "shares_short_prior, short_pct_shares_out, data_source "
            "FROM short_interest "
            "WHERE symbol = $1 ORDER BY date DESC LIMIT $2",
            symbol, limit,
        )
    except Exception as e:
        logger.warning("Failed to query short_interest for %s: %s", symbol, e)
        return None
    if not rows:
        return None
    return {"symbol": symbol, "data": [dict(r) for r in rows], "source": "db"}


# ---------------------------------------------------------------------------
# 10. earnings_calendar (date range, not per-symbol)
# ---------------------------------------------------------------------------

async def get_earnings_calendar_from_db(
    from_date: date, to_date: date,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT symbol, earnings_date, eps_estimate, eps_actual, "
            "revenue_estimate, revenue_actual, quarter, year, data_source "
            "FROM earnings_calendar "
            "WHERE earnings_date >= $1 AND earnings_date <= $2 "
            "ORDER BY earnings_date, symbol",
            from_date, to_date,
        )
    except Exception as e:
        logger.warning("Failed to query earnings_calendar %s~%s: %s", from_date, to_date, e)
        return None
    if not rows:
        return None
    return {
        "from_date": str(from_date),
        "to_date": str(to_date),
        "count": len(rows),
        "data": [dict(r) for r in rows],
        "source": "db",
    }


# ---------------------------------------------------------------------------
# 11. economic_indicators
# ---------------------------------------------------------------------------

async def get_economic_indicator_from_db(
    code: str, *, limit: int = 100,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT indicator_code, indicator_name, date, value, data_source "
            "FROM economic_indicators "
            "WHERE indicator_code = $1 ORDER BY date DESC LIMIT $2",
            code, limit,
        )
    except Exception as e:
        logger.warning("Failed to query economic_indicators for %s: %s", code, e)
        return None
    if not rows:
        return None
    return {
        "indicator_code": code,
        "indicator_name": rows[0]["indicator_name"],
        "data": [dict(r) for r in rows],
        "source": "db",
    }


# ---------------------------------------------------------------------------
# 12. macro_daily
# ---------------------------------------------------------------------------

async def get_macro_daily_from_db(
    ticker: str, *, limit: int = 365,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT ticker, indicator_name, date, open, high, low, close, "
            "volume, data_source "
            "FROM macro_daily "
            "WHERE ticker = $1 ORDER BY date DESC LIMIT $2",
            ticker, limit,
        )
    except Exception as e:
        logger.warning("Failed to query macro_daily for %s: %s", ticker, e)
        return None
    if not rows:
        return None
    return {
        "ticker": ticker,
        "indicator_name": rows[0]["indicator_name"],
        "data": [dict(r) for r in rows],
        "source": "db",
    }


# ---------------------------------------------------------------------------
# 13. cn_alternative_data
# ---------------------------------------------------------------------------

async def get_cn_alternative_from_db(
    symbol: str, data_type: str, *, limit: int = 30,
) -> Optional[dict[str, Any]]:
    try:
        pool = get_db_pool()
    except RuntimeError:
        return None
    try:
        rows = await pool.fetch(
            "SELECT date, data_type, data, data_source "
            "FROM cn_alternative_data "
            "WHERE symbol = $1 AND data_type = $2 "
            "ORDER BY date DESC LIMIT $3",
            symbol, data_type, limit,
        )
    except Exception as e:
        logger.warning("Failed to query cn_alternative_data for %s/%s: %s", symbol, data_type, e)
        return None
    if not rows:
        return None
    return {
        "symbol": symbol,
        "data_type": data_type,
        "data": [dict(r) for r in rows],
        "source": "db",
    }
