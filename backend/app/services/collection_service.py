"""Daily bar collection pipeline for data-service.

Replaces the backend's Celery-based collection with a direct async pipeline:
1. Redis SET NX lock (prevent concurrent collection for same market)
2. Resolve symbols via symbol_resolver
3. Query latest dates from DB (bar_persistence_service)
4. Fetch bars from external APIs (daily_bar_service.DailyBarFetcher)
5. Upsert to DB (bar_persistence_service)
6. Update Redis progress for admin UI
7. Release lock

Redis key patterns (compatible with existing backend tasks):
- Lock: ``sp:daily_bars:{market}:lock`` (SET NX, TTL 28800s)
- Progress: ``sp:daily_bars:{market}:progress`` (JSON, TTL 3600s)
- Counter: ``sp:counters:daily_bars:{market}`` (no TTL)
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from app.config import get_settings
from app.core.redis import get_redis
from app.core.database import get_db_pool
from app.services import bar_persistence_service
from app.services.daily_bar_service import DailyBarFetcher
from app.services import symbol_resolver

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants (match backend/worker/tasks/daily_bar_tasks.py)
# ---------------------------------------------------------------------------

_LOCK_KEY_TEMPLATE = "sp:daily_bars:{market}:lock"
_LOCK_TTL = 3600  # 1 hour — auto-releases if process crashes (collection takes ~15min)

_PROGRESS_KEY_TEMPLATE = "sp:daily_bars:{market}:progress"
_PROGRESS_TTL = 3600  # 1 hour

_COUNTER_KEY_TEMPLATE = "sp:counters:daily_bars:{market}"

# Batch sizes (match backend DailyBarService constants)
_CN_BATCH_SIZE = 36
_YF_BATCH_SIZE = 50
_YF_MAX_CONCURRENT_BATCHES = 5
_YF_WINDOW_SIZE = 20

# Lua script for atomic CAS lock release
_RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""

# Shared fetcher instance
_fetcher = DailyBarFetcher()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def collect_market(market: str) -> dict[str, Any]:
    """Collect daily bars for a market and upsert into PostgreSQL.

    This is the main entry point, equivalent to the backend's
    ``collect_market_daily_bars`` Celery task.

    Args:
        market: Market code ('us', 'hk', 'cn', 'metal').

    Returns:
        Summary dict with keys: symbol_count, new_bars, errors.
    """
    market = market.lower()
    owner = await _acquire_lock(market)
    if owner is None:
        logger.warning(
            "Collection for market=%s already running, skipping", market,
        )
        return {"symbol_count": 0, "new_bars": 0, "errors": ["Already running"]}

    try:
        # 1. Resolve symbols
        symbols = await symbol_resolver.get_symbols(market)
        if not symbols:
            logger.warning("No symbols found for market=%s", market)
            return {"symbol_count": 0, "new_bars": 0, "errors": ["No symbols"]}

        logger.info(
            "Collection started: market=%s, symbols=%d", market, len(symbols),
        )
        await _update_progress(market, 0, len(symbols), 0)

        # 2. Query latest dates from DB
        pool = get_db_pool()
        latest_dates = await bar_persistence_service.get_latest_dates(pool, market)

        # 3. Fetch + upsert
        if market == "cn":
            result = await _collect_cn(market, symbols, latest_dates)
        else:
            result = await _collect_yf(market, symbols, latest_dates)

        logger.info(
            "Collection complete: market=%s, symbols=%d, new_bars=%d, errors=%d",
            market,
            result.get("symbol_count", 0),
            result.get("new_bars", 0),
            len(result.get("errors", [])),
        )

        return result

    except Exception as exc:
        logger.exception("Collection failed for market=%s: %s", market, exc)
        return {
            "symbol_count": 0,
            "new_bars": 0,
            "errors": [f"Collection failed: {exc}"],
        }
    finally:
        await _rebuild_counter(market)
        await _clear_progress(market)
        await _release_lock(market, owner)


async def rebuild_market(market: str) -> dict[str, Any]:
    """Delete all bars for a market, then re-collect from scratch.

    Equivalent to the backend's ``rebuild_market_daily_bars`` Celery task.

    Args:
        market: Market code ('us', 'hk', 'cn', 'metal').

    Returns:
        Summary dict with keys: symbol_count, new_bars, deleted, errors.
    """
    market = market.lower()
    owner = await _acquire_lock(market)
    if owner is None:
        logger.warning(
            "Collection for market=%s already running, cannot rebuild", market,
        )
        return {"symbol_count": 0, "new_bars": 0, "deleted": 0, "errors": ["Already running"]}

    try:
        pool = get_db_pool()

        # Phase 1: Delete existing bars
        deleted = await bar_persistence_service.delete_market_bars(pool, market)
        logger.info("Rebuild phase 1: deleted %d bars for market=%s", deleted, market)

        # Phase 2: Re-collect from scratch
        symbols = await symbol_resolver.get_symbols(market)
        if not symbols:
            logger.warning("No symbols found for market=%s after delete", market)
            return {
                "symbol_count": 0,
                "new_bars": 0,
                "deleted": deleted,
                "errors": ["No symbols"],
            }

        logger.info(
            "Rebuild phase 2: collecting %d symbols for market=%s",
            len(symbols), market,
        )
        await _update_progress(market, 0, len(symbols), 0)

        # Latest dates will all be empty since we deleted everything
        latest_dates: dict[str, date] = {}

        if market == "cn":
            result = await _collect_cn(market, symbols, latest_dates)
        else:
            result = await _collect_yf(market, symbols, latest_dates)

        result["deleted"] = deleted

        logger.info(
            "Rebuild complete: market=%s, deleted=%d, symbols=%d, new_bars=%d, errors=%d",
            market, deleted,
            result.get("symbol_count", 0),
            result.get("new_bars", 0),
            len(result.get("errors", [])),
        )

        return result

    except Exception as exc:
        logger.exception("Rebuild failed for market=%s: %s", market, exc)
        return {
            "symbol_count": 0,
            "new_bars": 0,
            "deleted": 0,
            "errors": [f"Rebuild failed: {exc}"],
        }
    finally:
        await _rebuild_counter(market)
        await _clear_progress(market)
        await _release_lock(market, owner)


# ---------------------------------------------------------------------------
# CN collection path (akshare, sequential batches)
# ---------------------------------------------------------------------------


async def _collect_cn(
    market: str,
    symbols: list[str],
    latest_dates: dict[str, date],
) -> dict[str, Any]:
    """Collect CN daily bars in sequential batches of _CN_BATCH_SIZE.

    CN market uses akshare which fetches one symbol at a time internally.
    We process in small batches to bound memory and provide progress updates.
    """
    pool = get_db_pool()
    today = date.today()
    total_inserted = 0
    errors: list[str] = []
    symbols_done = 0
    symbols_with_data = 0

    for batch_start in range(0, len(symbols), _CN_BATCH_SIZE):
        batch = symbols[batch_start: batch_start + _CN_BATCH_SIZE]

        # Build fetch request, skipping up-to-date symbols
        batch_request: list[dict] = []
        skipped = 0
        for sym in batch:
            last_date = latest_dates.get(sym)
            if last_date is not None and last_date + timedelta(days=1) >= today:
                skipped += 1
                continue
            start_date = (
                (last_date + timedelta(days=1)).isoformat()
                if last_date is not None
                else None
            )
            batch_request.append({"symbol": sym, "start_date": start_date})

        if batch_request:
            try:
                results, fetch_errors = await _fetcher.fetch_batch(
                    batch_request, market,
                )
            except Exception as exc:
                logger.error(
                    "CN fetch_batch failed (offset=%d, size=%d): %s",
                    batch_start, len(batch_request), exc,
                )
                errors.append(
                    f"batch {batch_start // _CN_BATCH_SIZE}: fetch error - {exc}"
                )
                results, fetch_errors = {}, {}

            # Upsert results to DB
            for symbol, data in results.items():
                try:
                    bars = data.get("bars", []) if isinstance(data, dict) else []
                    source = (
                        data.get("source", "akshare")
                        if isinstance(data, dict) else "akshare"
                    )
                    count = await bar_persistence_service.upsert_bars(
                        pool, symbol, market, source, bars,
                    )
                    total_inserted += count
                except Exception as exc:
                    errors.append(f"{symbol}: upsert - {exc}")

            symbols_with_data += len(results)

            # Record fetch errors
            for sym, msg in fetch_errors.items():
                errors.append(f"{sym}: {msg}")

        symbols_done += len(batch)
        await _update_progress(market, symbols_done, len(symbols), total_inserted)

        logger.info(
            "CN batch: %d/%d done (%d with data, %d skipped, %d errors)",
            symbols_done, len(symbols), symbols_with_data, skipped, len(errors),
        )

    return {
        "symbol_count": len(symbols),
        "new_bars": total_inserted,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# US / HK / Metal collection path (yfinance, windowed parallel)
# ---------------------------------------------------------------------------


async def _collect_yf(
    market: str,
    symbols: list[str],
    latest_dates: dict[str, date],
) -> dict[str, Any]:
    """Collect daily bars via yfinance with windowed parallel fetching.

    Groups symbols by start_date (yfinance requires a single start per batch),
    then processes in windows of _YF_WINDOW_SIZE batches with a semaphore
    for concurrency control.
    """
    pool = get_db_pool()
    today = date.today()
    total_inserted = 0
    errors: list[str] = []

    # Group symbols by start_date
    date_groups: dict[Optional[str], list[str]] = defaultdict(list)
    up_to_date_count = 0

    for sym in symbols:
        last_date = latest_dates.get(sym)
        if last_date is None:
            date_groups[None].append(sym)
        else:
            start = last_date + timedelta(days=1)
            if start >= today:
                up_to_date_count += 1
                continue
            date_groups[start.isoformat()].append(sym)

    if up_to_date_count:
        logger.info("Skipped %d already-up-to-date symbols", up_to_date_count)

    # Build flat batch list: each entry is (start_str, [symbols])
    batches: list[tuple[Optional[str], list[str]]] = []
    for start_key, group in date_groups.items():
        for i in range(0, len(group), _YF_BATCH_SIZE):
            batches.append((start_key, group[i: i + _YF_BATCH_SIZE]))

    total_to_fetch = len(symbols) - up_to_date_count
    logger.info(
        "yfinance batch plan: %d batches, %d date groups, %d symbols to fetch",
        len(batches), len(date_groups), total_to_fetch,
    )

    semaphore = asyncio.Semaphore(_YF_MAX_CONCURRENT_BATCHES)
    symbols_done = up_to_date_count
    symbols_with_data = 0

    async def fetch_one(
        start_str: Optional[str],
        batch_symbols: list[str],
    ) -> Optional[tuple[dict[str, dict], dict[str, str]]]:
        """Fetch a single batch with semaphore-bounded concurrency."""
        async with semaphore:
            try:
                batch_request = [
                    {"symbol": sym, "start_date": start_str}
                    for sym in batch_symbols
                ]
                return await _fetcher.fetch_batch(batch_request, market)
            except Exception as exc:
                logger.error(
                    "Batch fetch failed (start=%s, size=%d): %s",
                    start_str, len(batch_symbols), exc,
                )
                for sym in batch_symbols:
                    errors.append(f"{sym}: batch error - {exc}")
                return None

    # Process in windows of _YF_WINDOW_SIZE batches
    for win_start in range(0, len(batches), _YF_WINDOW_SIZE):
        window = batches[win_start: win_start + _YF_WINDOW_SIZE]

        # Parallel HTTP fetch within this window
        responses = await asyncio.gather(
            *[fetch_one(s, b) for s, b in window]
        )

        # Sequential DB upsert for this window's results
        for (start_str, batch_symbols), resp in zip(window, responses):
            if resp is not None:
                results, fetch_errors = resp

                for symbol, data in results.items():
                    try:
                        bars = (
                            data.get("bars", [])
                            if isinstance(data, dict) else []
                        )
                        source = (
                            data.get("source", "yfinance")
                            if isinstance(data, dict) else "yfinance"
                        )
                        count = await bar_persistence_service.upsert_bars(
                            pool, symbol, market, source, bars,
                        )
                        total_inserted += count
                    except Exception as exc:
                        errors.append(f"{symbol}: upsert error - {exc}")
                        logger.error("Upsert failed for %s: %s", symbol, exc)

                symbols_with_data += len(results)

                for sym, msg in fetch_errors.items():
                    errors.append(f"{sym}: {msg}")
            else:
                errors.append(
                    f"batch: fetch failed (start={start_str}, size={len(batch_symbols)})"
                )

            symbols_done += len(batch_symbols)
            await _update_progress(
                market, symbols_done, len(symbols), total_inserted,
            )

        # Log after each window
        logger.info(
            "Window %d-%d/%d done, %d/%d symbols, %d inserted, %d errors",
            win_start + 1, min(win_start + _YF_WINDOW_SIZE, len(batches)),
            len(batches), symbols_done, len(symbols),
            total_inserted, len(errors),
        )

        # Explicitly discard references so GC can reclaim memory
        del responses

    return {
        "symbol_count": len(symbols),
        "new_bars": total_inserted,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Redis lock helpers
# ---------------------------------------------------------------------------


async def _acquire_lock(market: str) -> Optional[str]:
    """Try to acquire the per-market collection lock via Redis SET NX.

    Returns:
        Owner token string if acquired, None if already held.
    """
    try:
        r = await get_redis()
        owner = str(uuid.uuid4())
        acquired = await r.set(
            _LOCK_KEY_TEMPLATE.format(market=market),
            owner,
            nx=True,
            ex=_LOCK_TTL,
        )
        if acquired:
            # Clear queued flag
            await r.delete(f"sp:daily_bars:{market}:queued")
            return owner
        return None
    except Exception as exc:
        # Redis unavailable — fail closed to prevent concurrent collection
        logger.error("Redis lock acquisition failed for %s, skipping collection: %s", market, exc)
        return None


async def _release_lock(market: str, owner: str) -> None:
    """Release the per-market lock only if we still own it (CAS via Lua)."""
    try:
        r = await get_redis()
        await r.eval(
            _RELEASE_LOCK_LUA,
            1,
            _LOCK_KEY_TEMPLATE.format(market=market),
            owner,
        )
    except Exception as exc:
        logger.warning("Failed to release lock for %s: %s", market, exc)


async def force_unlock(market: str) -> bool:
    """Force-release the per-market lock (admin operation).

    Returns:
        True if a lock was deleted, False otherwise.
    """
    try:
        r = await get_redis()
        deleted = await r.delete(_LOCK_KEY_TEMPLATE.format(market=market))
        return deleted > 0
    except Exception as exc:
        logger.warning("Failed to force-unlock %s: %s", market, exc)
        return False


# ---------------------------------------------------------------------------
# Progress helpers
# ---------------------------------------------------------------------------


async def _update_progress(
    market: str,
    symbols_done: int,
    symbols_total: int,
    new_bars: int,
) -> None:
    """Write collection progress to Redis for admin UI consumption."""
    try:
        r = await get_redis()
        pct = (
            int(symbols_done * 100 / symbols_total) if symbols_total > 0 else 0
        )
        progress = {
            "symbolsDone": symbols_done,
            "symbolsTotal": symbols_total,
            "newBars": new_bars,
            "percent": pct,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }
        key = _PROGRESS_KEY_TEMPLATE.format(market=market)
        await r.setex(key, _PROGRESS_TTL, json.dumps(progress))
        # Publish for WebSocket fanout
        from app.ws.redis_fanout import publish_collection_progress
        from app.ws.protocol import make_collection_progress
        await publish_collection_progress(
            make_collection_progress(market, symbols_done, symbols_total, new_bars, pct)
        )
    except Exception:
        pass  # Non-critical


async def _clear_progress(market: str) -> None:
    """Clear the progress key from Redis."""
    try:
        r = await get_redis()
        await r.delete(_PROGRESS_KEY_TEMPLATE.format(market=market))
    except Exception:
        pass


async def get_progress(market: str) -> Optional[dict[str, Any]]:
    """Read collection progress from Redis (for admin API).

    Returns:
        Progress dict or None if no collection is in progress.
    """
    try:
        r = await get_redis()
        data = await r.get(_PROGRESS_KEY_TEMPLATE.format(market=market))
        if data is not None:
            return json.loads(data)
    except Exception as exc:
        logger.warning("Failed to read progress for %s: %s", market, exc)
    return None


# ---------------------------------------------------------------------------
# Counter rebuild
# ---------------------------------------------------------------------------


async def _rebuild_counter(market: str) -> None:
    """Query per-market stats from DB and write to Redis counter.

    The admin stats endpoint reads this counter instead of running
    expensive queries on every request.
    """
    try:
        pool = get_db_pool()
        stats = await bar_persistence_service.get_market_stats(pool, market)

        counter = {
            "count": stats["count"],
            "symbolCount": stats["symbol_count"],
            "firstDate": stats["first_date"],
            "lastDate": stats["last_date"],
        }

        r = await get_redis()
        await r.set(
            _COUNTER_KEY_TEMPLATE.format(market=market),
            json.dumps(counter),
        )
        logger.debug(
            "Rebuilt counter for market=%s: %d bars, %d symbols",
            market, stats["count"], stats["symbol_count"],
        )
    except Exception as exc:
        logger.warning("Failed to rebuild counter for %s: %s", market, exc)
