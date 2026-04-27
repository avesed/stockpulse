"""APScheduler-based background scheduler for StockPulse.

Uses Redis-backed leader election so that only ONE uvicorn worker runs
scheduled jobs when multiple workers are deployed.  The leader checks
a Redis key with SET NX + TTL, and renews it on a heartbeat interval.

This is a bare-bones skeleton: leader election, heartbeat, start/stop.
Jobs will be registered in Phase 5.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.core.redis import get_redis

logger = logging.getLogger(__name__)

# Leader election settings
_LEADER_KEY = "sp:scheduler:leader"
_LEADER_TTL = 60       # seconds — key expires if leader crashes
_HEARTBEAT_INTERVAL = 30  # seconds — renew before TTL expires

# Unique ID for this worker instance
_INSTANCE_ID = str(uuid.uuid4())

# Module-level state
_scheduler: Optional[AsyncIOScheduler] = None
_is_leader = False


async def start_scheduler() -> None:
    """Attempt to acquire leadership and start the scheduler if successful.

    Called during application startup (FastAPI lifespan).  If another
    worker is already the leader, this is a no-op and the worker runs
    without scheduled jobs.
    """
    global _scheduler, _is_leader

    acquired = await _try_acquire_leadership()
    if not acquired:
        logger.info(
            "Scheduler: another worker is leader, skipping (instance=%s)",
            _INSTANCE_ID[:8],
        )
        return

    _is_leader = True
    logger.info(
        "Scheduler: acquired leadership (instance=%s)", _INSTANCE_ID[:8],
    )

    _scheduler = AsyncIOScheduler(timezone="UTC")

    # Leadership heartbeat
    _scheduler.add_job(
        _heartbeat,
        IntervalTrigger(seconds=_HEARTBEAT_INTERVAL),
        id="leader_heartbeat",
        name="Leader heartbeat",
        replace_existing=True,
    )

    # Shared job kwargs
    _job_kwargs = dict(
        misfire_grace_time=60,
        coalesce=True,
        max_instances=1,
        replace_existing=True,
    )

    # Daily bar collection (UTC schedule, after each market closes)
    _scheduler.add_job(
        _run_collection, CronTrigger(hour=8, minute=0),
        args=["cn"], id="collect_cn", name="Collect CN bars",
        **_job_kwargs,
    )
    _scheduler.add_job(
        _run_collection, CronTrigger(hour=9, minute=0),
        args=["hk"], id="collect_hk", name="Collect HK bars",
        **_job_kwargs,
    )
    _scheduler.add_job(
        _run_collection, CronTrigger(hour=22, minute=0),
        args=["us"], id="collect_us", name="Collect US bars",
        **_job_kwargs,
    )
    _scheduler.add_job(
        _run_collection, CronTrigger(hour=22, minute=30),
        args=["metal"], id="collect_metal", name="Collect Metal bars",
        **_job_kwargs,
    )

    # Stock list update (daily 05:30 UTC)
    _scheduler.add_job(
        _run_stock_list_update, CronTrigger(hour=5, minute=30),
        id="update_stock_list", name="Update stock list",
        **_job_kwargs,
    )

    # Stock profile collection (weekly Sunday 06:00 UTC)
    _scheduler.add_job(
        _run_profile_collection_all,
        CronTrigger(day_of_week="sun", hour=6, minute=0),
        id="build_stock_kb", name="Build stock profiles",
        **_job_kwargs,
    )

    # Concept board sync (Mon-Sat 06:00 UTC)
    _scheduler.add_job(
        _run_concept_sync,
        CronTrigger(day_of_week="mon-sat", hour=6, minute=0),
        id="sync_concept_boards", name="Sync concept boards",
        **_job_kwargs,
    )

    _scheduler.start()
    logger.info("Scheduler started with %d jobs", len(_scheduler.get_jobs()))


async def stop_scheduler() -> None:
    """Stop the scheduler and release leadership.

    Called during application shutdown (FastAPI lifespan).
    """
    global _scheduler, _is_leader

    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("Scheduler shut down")

    if _is_leader:
        await _release_leadership()
        _is_leader = False
        logger.info("Leadership released (instance=%s)", _INSTANCE_ID[:8])


def is_leader() -> bool:
    """Return whether this instance currently holds leadership."""
    return _is_leader


def get_scheduler() -> Optional[AsyncIOScheduler]:
    """Return the scheduler instance (None if not leader or not started)."""
    return _scheduler


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

_RENEW_LEADER_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], ARGV[2])
end
return 0
"""


async def _heartbeat() -> None:
    """Atomically check ownership and renew the leader key TTL.

    Uses a Lua CAS script to prevent the race where another instance
    steals the key between our GET and EXPIRE calls.  If the key is
    no longer ours, stop the scheduler gracefully.
    """
    global _is_leader

    try:
        r = await get_redis()
        renewed = await r.eval(
            _RENEW_LEADER_LUA, 1, _LEADER_KEY, _INSTANCE_ID, _LEADER_TTL,
        )
        if renewed:
            logger.debug("Scheduler: heartbeat OK (instance=%s)", _INSTANCE_ID[:8])
            return
    except Exception as exc:
        logger.warning("Scheduler: heartbeat failed: %s", exc)
        # Fail-open on transient error: assume still leader
        return

    # Lua returned 0 -- key is gone or owned by another instance
    logger.warning(
        "Scheduler: leadership lost during heartbeat, stopping (instance=%s)",
        _INSTANCE_ID[:8],
    )
    _is_leader = False
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Leadership helpers
# ---------------------------------------------------------------------------


async def _try_acquire_leadership() -> bool:
    """Try to acquire the leader key via SET NX.

    Returns True if this instance is now the leader.
    """
    try:
        r = await get_redis()
        acquired = await r.set(
            _LEADER_KEY, _INSTANCE_ID, nx=True, ex=_LEADER_TTL,
        )
        return bool(acquired)
    except Exception as exc:
        logger.warning("Failed to acquire leadership: %s", exc)
        return False


async def _verify_leadership() -> bool:
    """Check if this instance is still the leader.

    Returns True on Redis errors (fail-open) to prevent premature
    scheduler shutdown on transient network blips.
    """
    try:
        r = await get_redis()
        current = await r.get(_LEADER_KEY)
        return current == _INSTANCE_ID
    except Exception as exc:
        logger.warning("Failed to verify leadership: %s", exc)
        return True  # Assume still leader on transient error


async def _release_leadership() -> None:
    """Release the leader key (only if we own it)."""
    try:
        r = await get_redis()
        lua = (
            'if redis.call("get", KEYS[1]) == ARGV[1] then '
            'return redis.call("del", KEYS[1]) end return 0'
        )
        await r.eval(lua, 1, _LEADER_KEY, _INSTANCE_ID)
    except Exception as exc:
        logger.warning("Failed to release leadership: %s", exc)


# ---------------------------------------------------------------------------
# Job wrappers
# ---------------------------------------------------------------------------


async def _run_collection(market: str) -> None:
    """Scheduled job: run daily bar collection for a market."""
    if not _is_leader:
        return

    logger.info("Scheduler: starting collection for market=%s", market)
    try:
        from app.services import collection_service

        result = await collection_service.collect_market(market)
        logger.info(
            "Scheduler: collection for %s complete — symbols=%d, new_bars=%d, errors=%d",
            market,
            result.get("symbol_count", 0),
            result.get("new_bars", 0),
            len(result.get("errors", [])),
        )
    except Exception as exc:
        logger.exception("Scheduler: collection for %s failed: %s", market, exc)


async def _run_stock_list_update() -> None:
    """Scheduled job: build full stock list and save to stock_symbols table."""
    if not _is_leader:
        return

    logger.info("Scheduler: starting stock list update")
    try:
        from app.services import stock_list_persistence

        result = await stock_list_persistence.build_and_save_stock_list()
        logger.info(
            "Scheduler: stock list update complete — total=%d, by_market=%s",
            result.get("total_stocks", 0),
            result.get("by_market", {}),
        )
    except Exception as exc:
        logger.exception("Scheduler: stock list update failed: %s", exc)


async def _run_profile_collection_all() -> None:
    """Scheduled job: collect profiles for all markets (CN, US, HK)."""
    if not _is_leader:
        return

    logger.info("Scheduler: starting stock profile collection for all markets")
    try:
        from app.services import profile_collection_service

        for market in ("cn", "us", "hk"):
            logger.info("Scheduler: collecting profiles for market=%s", market)
            result = await profile_collection_service.collect_market_profiles(market)
            collected = result.get("collected", 0)
            elapsed = result.get("elapsed_seconds", 0)
            error = result.get("error")
            if error:
                logger.warning(
                    "Scheduler: profile collection for %s had error: %s (collected=%d, %.0fs)",
                    market, error, collected, elapsed,
                )
            else:
                logger.info(
                    "Scheduler: profile collection for %s complete — collected=%d, %.0fs",
                    market, collected, elapsed,
                )

        logger.info("Scheduler: stock profile collection for all markets complete")
    except Exception as exc:
        logger.exception("Scheduler: stock profile collection failed: %s", exc)


async def _run_concept_sync() -> None:
    """Scheduled job: daily concept board mapping sync."""
    if not _is_leader:
        return

    logger.info("Scheduler: starting concept board sync")
    try:
        from app.services import profile_collection_service

        result = await profile_collection_service.collect_cn_concept_mapping()
        stock_count = result.get("stock_count", 0)
        elapsed = result.get("elapsed_seconds", 0)
        logger.info(
            "Scheduler: concept board sync complete — %d stocks in %.0fs",
            stock_count, elapsed,
        )
    except Exception as exc:
        logger.exception("Scheduler: concept board sync failed: %s", exc)
