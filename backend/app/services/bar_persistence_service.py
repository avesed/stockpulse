"""Raw asyncpg persistence layer for stock_daily_bars.

This module provides direct database access for daily bar CRUD operations,
bypassing SQLAlchemy for maximum performance on bulk inserts.  It is the
data-service counterpart of ``backend/app/services/daily_bar_service.py``
but uses ``asyncpg.Pool`` instead of ``AsyncSession``.

Key design decisions:
- ``executemany()`` for bulk upserts (faster than building VALUES strings)
- Each execute call contains exactly ONE SQL statement (asyncpg constraint)
- Decimal → float / int conversion for columnar output (qlib-service contract)
- Chunked inserts (500 rows) to bound memory and avoid oversized queries
"""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Optional

import asyncpg

logger = logging.getLogger(__name__)

INSERT_CHUNK_SIZE = 500


def _parse_bar_date(date_str: str) -> Optional[date]:
    """Parse a date from bar dict (e.g. '2025-01-15T00:00:00' or '2025-01-15')."""
    try:
        return date.fromisoformat(date_str[:10])
    except (ValueError, TypeError):
        return None


async def upsert_bars(
    pool: asyncpg.Pool,
    symbol: str,
    market: str,
    data_source: str,
    bars: list[dict],
) -> int:
    """Insert bars using ON CONFLICT DO NOTHING, processed in chunks.

    Args:
        pool: asyncpg connection pool.
        symbol: Stock symbol (e.g. 'AAPL', '600000.SS').
        market: Market code ('us', 'hk', 'cn', 'metal').
        data_source: Provider name ('yfinance', 'akshare', etc.).
        bars: List of bar dicts with keys: date, open, high, low, close, volume.

    Returns:
        Total number of rows actually inserted (excludes conflicts).
    """
    if not bars:
        return 0

    total_inserted = 0
    skipped_dates = 0

    sql = (
        "INSERT INTO stock_daily_bars "
        "(symbol, market, date, open, high, low, close, volume, data_source) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
        "ON CONFLICT (symbol, date) DO NOTHING"
    )

    for chunk_start in range(0, len(bars), INSERT_CHUNK_SIZE):
        chunk = bars[chunk_start : chunk_start + INSERT_CHUNK_SIZE]

        # Build parameter tuples for executemany
        rows: list[tuple] = []
        for bar in chunk:
            bar_date = _parse_bar_date(bar.get("date", ""))
            if bar_date is None:
                skipped_dates += 1
                continue

            rows.append((
                symbol,
                market,
                bar_date,
                bar.get("open", 0),
                bar.get("high", 0),
                bar.get("low", 0),
                bar.get("close", 0),
                int(bar.get("volume") or 0),
                data_source,
            ))

        if not rows:
            continue

        # executemany does not return rowcount in asyncpg, so we use
        # len(rows) as an upper bound (actual inserts may be fewer due
        # to ON CONFLICT DO NOTHING).  Wrap in an explicit transaction
        # so the batch is atomic.
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, rows)
            total_inserted += len(rows)

    if skipped_dates > 0:
        logger.warning(
            "Skipped %d bars with unparseable dates for %s",
            skipped_dates, symbol,
        )

    if total_inserted > 0:
        logger.info("Inserted %d bars for %s", total_inserted, symbol)

    return total_inserted


async def get_latest_dates(
    pool: asyncpg.Pool,
    market: str,
) -> dict[str, date]:
    """Get the latest data date per symbol for a given market.

    Returns:
        Dict mapping symbol -> latest date in DB for that market.
    """
    rows = await pool.fetch(
        "SELECT symbol, MAX(date) AS max_date "
        "FROM stock_daily_bars "
        "WHERE market = $1 "
        "GROUP BY symbol",
        market,
    )
    return {row["symbol"]: row["max_date"] for row in rows}


async def get_latest_dates_for_symbols(
    pool: asyncpg.Pool,
    symbols: list[str],
) -> dict[str, date]:
    """Get the latest data date per symbol (no market filter needed).

    Symbols encode their market (e.g. AAPL, 0700.HK, 600000.SS), so the
    unique constraint (symbol, date) already provides correct grouping.

    Returns:
        Dict mapping symbol -> latest date in DB.
    """
    rows = await pool.fetch(
        "SELECT symbol, MAX(date) AS max_date "
        "FROM stock_daily_bars "
        "WHERE symbol = ANY($1) "
        "GROUP BY symbol",
        symbols,
    )
    return {row["symbol"]: row["max_date"] for row in rows}


async def get_bars_batch(
    pool: asyncpg.Pool,
    symbols: list[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> dict[str, dict[str, list]]:
    """Query bars from DB in columnar format for qlib-service consumption.

    Returns dict keyed by symbol, each containing lists of dates, open, high,
    low, close, volume.  Decimal values are converted to float, volume to int.

    Example::

        {
            "AAPL": {
                "dates": ["2025-01-15", "2025-01-16", ...],
                "open": [150.0, 151.5, ...],
                "high": [152.0, 153.0, ...],
                "low": [149.0, 150.0, ...],
                "close": [151.0, 152.5, ...],
                "volume": [50000000, 48000000, ...],
            }
        }
    """
    if not symbols:
        return {}

    logger.info(
        "Querying bars: symbols=%d, start=%s, end=%s",
        len(symbols), start_date, end_date,
    )

    # Build query dynamically based on optional date filters
    query = (
        "SELECT symbol, date, open, high, low, close, volume "
        "FROM stock_daily_bars "
        "WHERE symbol = ANY($1)"
    )
    params: list[Any] = [symbols]

    if start_date is not None:
        params.append(start_date)
        query += f" AND date >= ${len(params)}"
    if end_date is not None:
        params.append(end_date)
        query += f" AND date <= ${len(params)}"

    query += " ORDER BY symbol, date"

    rows = await pool.fetch(query, *params)

    grouped: dict[str, dict[str, list]] = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in grouped:
            grouped[sym] = {
                "dates": [],
                "open": [],
                "high": [],
                "low": [],
                "close": [],
                "volume": [],
            }
        col = grouped[sym]
        col["dates"].append(str(row["date"]))
        col["open"].append(
            float(row["open"]) if isinstance(row["open"], Decimal) else row["open"]
        )
        col["high"].append(
            float(row["high"]) if isinstance(row["high"], Decimal) else row["high"]
        )
        col["low"].append(
            float(row["low"]) if isinstance(row["low"], Decimal) else row["low"]
        )
        col["close"].append(
            float(row["close"]) if isinstance(row["close"], Decimal) else row["close"]
        )
        col["volume"].append(int(row["volume"]))

    logger.info(
        "Returned bars for %d/%d symbols, %d total rows",
        len(grouped), len(symbols), len(rows),
    )

    return grouped


async def delete_market_bars(
    pool: asyncpg.Pool,
    market: str,
) -> int:
    """Delete all daily bars for a given market.

    Returns:
        Number of rows deleted.
    """
    logger.warning("Deleting all daily bars for market=%s", market)
    result = await pool.execute(
        "DELETE FROM stock_daily_bars WHERE market = $1",
        market,
    )
    # asyncpg execute returns a status string like "DELETE 1234"
    deleted = int(result.split()[-1]) if result else 0
    logger.info("Deleted %d daily bars for market=%s", deleted, market)
    return deleted


async def get_daily_bars_row(
    pool: asyncpg.Pool,
    symbol: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> list[dict]:
    """Query daily bars from DB in row format for the cache service.

    Returns bars as a list of dicts compatible with the OHLCV bar format used
    throughout the caching layer:

        [{"date": "2025-01-15", "open": 150.0, "high": 152.0, ...}, ...]

    Decimal values are converted to float, volume to int, dates to ISO strings.

    Args:
        pool: asyncpg connection pool.
        symbol: Stock symbol (e.g. 'AAPL', '600000.SS').
        start_date: Optional start date filter (inclusive).
        end_date: Optional end date filter (inclusive).

    Returns:
        List of bar dicts sorted by date ascending.
    """
    query = (
        "SELECT date, open, high, low, close, volume "
        "FROM stock_daily_bars "
        "WHERE symbol = $1"
    )
    params: list[Any] = [symbol]

    if start_date is not None:
        params.append(start_date)
        query += f" AND date >= ${len(params)}"
    if end_date is not None:
        params.append(end_date)
        query += f" AND date <= ${len(params)}"

    query += " ORDER BY date"

    rows = await pool.fetch(query, *params)

    result: list[dict] = []
    for row in rows:
        result.append({
            "date": str(row["date"]),
            "open": round(
                float(row["open"]) if isinstance(row["open"], Decimal)
                else row["open"],
                4,
            ),
            "high": round(
                float(row["high"]) if isinstance(row["high"], Decimal)
                else row["high"],
                4,
            ),
            "low": round(
                float(row["low"]) if isinstance(row["low"], Decimal)
                else row["low"],
                4,
            ),
            "close": round(
                float(row["close"]) if isinstance(row["close"], Decimal)
                else row["close"],
                4,
            ),
            "volume": int(row["volume"]) if row["volume"] is not None else 0,
        })

    if result:
        logger.debug(
            "get_daily_bars_row: %s returned %d rows (%s to %s)",
            symbol, len(result), result[0]["date"], result[-1]["date"],
        )

    return result


async def get_market_stats(
    pool: asyncpg.Pool,
    market: str,
) -> dict[str, Any]:
    """Get summary statistics for a market's daily bars.

    Returns:
        Dict with count, symbol_count, first_date, last_date.
    """
    row = await pool.fetchrow(
        "SELECT "
        "  COUNT(*) AS count, "
        "  COUNT(DISTINCT symbol) AS symbol_count, "
        "  MIN(date) AS first_date, "
        "  MAX(date) AS last_date "
        "FROM stock_daily_bars "
        "WHERE market = $1",
        market,
    )

    if row is None:
        return {
            "count": 0,
            "symbol_count": 0,
            "first_date": None,
            "last_date": None,
        }

    return {
        "count": row["count"],
        "symbol_count": row["symbol_count"],
        "first_date": str(row["first_date"]) if row["first_date"] else None,
        "last_date": str(row["last_date"]) if row["last_date"] else None,
    }
