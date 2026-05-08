"""Stock list persistence service.

Builds the full stock list via stock_list_service.build_stock_list() and
saves directly to the ``stock_symbols`` PostgreSQL table.  The backend
reads from the same table to build its in-memory search indexes.

Version tracking: after a successful write, sets Redis key
``stock_list:version`` so the backend can detect changes via polling.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.core.redis import get_redis

logger = logging.getLogger(__name__)

# Chunk size for asyncpg executemany
_INSERT_CHUNK_SIZE = 500

# Redis keys
_PROGRESS_KEY = "ds:stock_list:progress"
_PROGRESS_TTL = 3600  # 1 hour
_VERSION_KEY = "stock_list:version"
_VERSION_TTL = 172800  # 48 hours


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def build_and_save_stock_list() -> Dict[str, Any]:
    """Build the full stock list and persist to the ``stock_symbols`` table.

    Steps:
        1. Call existing build_stock_list() to fetch from all markets.
        2. Deduplicate by symbol.
        3. Write to PostgreSQL via TRUNCATE + INSERT in a transaction.
        4. Set Redis version marker.
        5. Invalidate symbol_resolver caches.

    Returns:
        Dict with status, total_stocks, and by_market breakdown.
    """
    t0 = time.monotonic()

    await _set_progress("building", "Fetching stock data from all markets...")

    # 1. Fetch from all markets
    from app.services.stock_list_service import build_stock_list

    all_stocks = await build_stock_list()

    if not all_stocks:
        await _set_progress("failed", "No stock data returned from fetchers")
        return {"status": "error", "reason": "no_data"}

    await _set_progress(
        "saving",
        f"Saving {len(all_stocks)} stocks to database...",
    )

    # 2. Deduplicate by symbol (safety guard)
    seen: set[str] = set()
    unique: List[Dict[str, Any]] = []
    for stock in all_stocks:
        sym = stock.get("symbol", "")
        if sym and sym not in seen:
            seen.add(sym)
            unique.append(stock)

    # 3. Safety check: refuse to overwrite if new data is drastically smaller
    from app.core.database import get_db_pool
    pool = get_db_pool()
    existing_count = await pool.fetchval("SELECT COUNT(*) FROM stock_symbols")
    if existing_count > 1000 and len(unique) < existing_count * 0.5:
        msg = (
            f"Refusing to overwrite stock_symbols: existing={existing_count}, "
            f"new={len(unique)} (< 50% of existing). Data source may be incomplete."
        )
        logger.error(msg)
        await _set_progress("failed", msg)
        return {"status": "error", "reason": "safety_check_failed", "detail": msg}

    # 4. Write to PostgreSQL
    await _save_to_db(unique)

    # 4. Set Redis version
    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    await _set_version(version)

    # 5. Invalidate symbol_resolver caches
    from app.services import symbol_resolver

    for market in ("us", "hk", "cn"):
        await symbol_resolver.invalidate_cache(market)

    elapsed = time.monotonic() - t0

    # Build by-market breakdown
    by_market: Dict[str, int] = {}
    for s in unique:
        m = s.get("market", "unknown")
        by_market[m] = by_market.get(m, 0) + 1

    logger.info(
        "Stock list built and saved to DB: %d stocks in %.1fs -- %s",
        len(unique),
        elapsed,
        by_market,
    )

    await _set_progress(
        "completed",
        f"Saved {len(unique)} stocks in {elapsed:.1f}s",
        extra={"total_stocks": len(unique), "by_market": by_market},
    )

    return {
        "status": "success",
        "total_stocks": len(unique),
        "by_market": by_market,
    }


async def get_progress() -> Optional[Dict[str, Any]]:
    """Read stock list build progress from Redis (for admin API)."""
    try:
        r = await get_redis()
        data = await r.get(_PROGRESS_KEY)
        if data is not None:
            return json.loads(data)
    except Exception as exc:
        logger.warning("Failed to read stock list progress: %s", exc)
    return None


async def is_table_empty() -> bool:
    """Check if the stock_symbols table has no rows."""
    from app.core.database import get_db_pool

    pool = get_db_pool()
    row = await pool.fetchval("SELECT EXISTS(SELECT 1 FROM stock_symbols)")
    return not row


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _save_to_db(stocks: List[Dict[str, Any]]) -> None:
    """Write stocks to ``stock_symbols`` using upsert (ON CONFLICT DO UPDATE).

    Never truncates — only inserts new symbols and updates existing ones.
    This prevents data loss when a data source returns partial results.
    """
    from app.core.database import get_db_pool

    pool = get_db_pool()

    sql = (
        "INSERT INTO stock_symbols "
        "(symbol, name, name_zh, exchange, market, pinyin, pinyin_initial, updated_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, NOW()) "
        "ON CONFLICT (symbol) DO UPDATE SET "
        "name = COALESCE(NULLIF(EXCLUDED.name, ''), stock_symbols.name), "
        "name_zh = COALESCE(NULLIF(EXCLUDED.name_zh, ''), stock_symbols.name_zh), "
        "exchange = COALESCE(NULLIF(EXCLUDED.exchange, ''), stock_symbols.exchange), "
        "market = EXCLUDED.market, "
        "pinyin = COALESCE(NULLIF(EXCLUDED.pinyin, ''), stock_symbols.pinyin), "
        "pinyin_initial = COALESCE(NULLIF(EXCLUDED.pinyin_initial, ''), stock_symbols.pinyin_initial), "
        "updated_at = NOW()"
    )

    try:
        async with pool.acquire(timeout=5) as conn:
            for i in range(0, len(stocks), _INSERT_CHUNK_SIZE):
                chunk = stocks[i : i + _INSERT_CHUNK_SIZE]
                rows = [
                    (
                        s.get("symbol", ""),
                        s.get("name", ""),
                        s.get("name_zh", ""),
                        s.get("exchange", ""),
                        s.get("market", ""),
                        s.get("pinyin", ""),
                        s.get("pinyin_initial", ""),
                    )
                    for s in chunk
                ]
                await conn.executemany(sql, rows)

        logger.info("Upserted %d stocks to stock_symbols table", len(stocks))

    except Exception as e:
        logger.exception("Failed to save stock list to DB: %s", e)
        raise RuntimeError(f"Failed to save stock list: {e}") from e


async def _set_version(version: str) -> None:
    """Set the stock list version in Redis for backend polling.

    Writes to both DB 5 (data-service) and DB 0 (backend app cache) so
    that the backend's StockListService.check_for_updates() can detect
    changes when polling from its own Redis DB.
    """
    try:
        r = await get_redis()
        await r.setex(_VERSION_KEY, _VERSION_TTL, version)

        # Also write to DB 0 (backend app cache) for cross-service visibility
        import redis.asyncio as aioredis
        from app.config import get_settings
        settings = get_settings()
        base_url = settings.REDIS_URL.rsplit("/", 1)[0]  # strip /5
        r0 = aioredis.from_url(
            f"{base_url}/0", decode_responses=True,
            socket_connect_timeout=5, socket_timeout=5,
        )
        try:
            await r0.setex(_VERSION_KEY, _VERSION_TTL, version)
        finally:
            await r0.close()

        logger.info("Set stock_list:version = %s (DB 0 + 5)", version)
    except Exception as e:
        logger.warning("Failed to set stock_list:version: %s", e)


async def _set_progress(
    status: str,
    message: str,
    *,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Write progress to Redis for admin UI polling."""
    try:
        r = await get_redis()
        progress: Dict[str, Any] = {
            "status": status,
            "message": message,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            progress.update(extra)
        await r.setex(_PROGRESS_KEY, _PROGRESS_TTL, json.dumps(progress))
    except Exception:
        pass  # Non-critical
