"""APScheduler-based background scheduler for StockPulse.

Uses Redis-backed leader election so that only ONE uvicorn worker runs
scheduled jobs when multiple workers are deployed.

A persistent supervisor task continuously attempts to acquire leadership
and rebuilds the scheduler whenever the previous leader dies (Redis
blip, restart, etc.).  This means a single transient heartbeat miss no
longer kills scheduling permanently — the next supervisor tick picks
the role back up.
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
_HEARTBEAT_INTERVAL = 20  # seconds — renew well before TTL expires
_SUPERVISOR_INTERVAL = 30  # seconds — re-elect cadence when not leader

# Unique ID for this worker instance
_INSTANCE_ID = str(uuid.uuid4())

# Module-level state
_scheduler: Optional[AsyncIOScheduler] = None
_is_leader = False
_supervisor_task: Optional[asyncio.Task] = None
_supervisor_stop = asyncio.Event()


async def start_scheduler() -> None:
    """Start the leader-election supervisor.

    The supervisor runs forever, repeatedly trying to acquire leadership.
    When it succeeds it builds a fresh AsyncIOScheduler with the cron
    jobs.  If leadership is lost (heartbeat fails or someone else takes
    the key), the scheduler is torn down and the supervisor goes back to
    polling — no process restart required.
    """
    global _supervisor_task, _supervisor_stop

    _supervisor_stop = asyncio.Event()
    _supervisor_task = asyncio.create_task(
        _supervisor_loop(), name="scheduler-supervisor",
    )
    logger.info(
        "Scheduler supervisor started (instance=%s)", _INSTANCE_ID[:8],
    )


async def stop_scheduler() -> None:
    """Stop the supervisor, scheduler, and release leadership."""
    global _supervisor_task

    _supervisor_stop.set()
    if _supervisor_task is not None:
        try:
            await asyncio.wait_for(_supervisor_task, timeout=5)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            _supervisor_task.cancel()
        _supervisor_task = None

    await _teardown_scheduler()
    if _is_leader:
        await _release_leadership()


def is_leader() -> bool:
    """Return whether this instance currently holds leadership."""
    return _is_leader


def get_scheduler() -> Optional[AsyncIOScheduler]:
    """Return the scheduler instance (None if not leader or not started)."""
    return _scheduler


# ---------------------------------------------------------------------------
# Supervisor loop
# ---------------------------------------------------------------------------


async def _supervisor_loop() -> None:
    """Continuously elect a leader and (re)build the scheduler.

    Sleeps for _SUPERVISOR_INTERVAL between attempts when not leader.
    When leader, sleeps until either the scheduler dies or stop is
    requested, then loops back to re-elect.
    """
    global _is_leader

    while not _supervisor_stop.is_set():
        try:
            if not _is_leader:
                acquired = await _try_acquire_leadership()
                if acquired:
                    _is_leader = True
                    logger.info(
                        "Scheduler: acquired leadership (instance=%s)",
                        _INSTANCE_ID[:8],
                    )
                    try:
                        _build_scheduler()
                    except Exception:
                        logger.exception(
                            "Scheduler: failed to build, releasing leadership",
                        )
                        await _release_leadership()
                        _is_leader = False
                else:
                    logger.debug(
                        "Scheduler: another worker is leader (instance=%s)",
                        _INSTANCE_ID[:8],
                    )

            # Wait for either stop signal or interval expiry
            try:
                await asyncio.wait_for(
                    _supervisor_stop.wait(), timeout=_SUPERVISOR_INTERVAL,
                )
            except asyncio.TimeoutError:
                pass

            # If we were leader but the scheduler died, treat as lost
            if _is_leader and (_scheduler is None or not _scheduler.running):
                logger.warning(
                    "Scheduler: instance died while leader, will re-elect "
                    "(instance=%s)", _INSTANCE_ID[:8],
                )
                _is_leader = False
                await _release_leadership()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Scheduler supervisor loop error")
            await asyncio.sleep(_SUPERVISOR_INTERVAL)


def _build_scheduler() -> None:
    """Construct a fresh AsyncIOScheduler with all cron jobs and start it."""
    global _scheduler

    _scheduler = AsyncIOScheduler(timezone="UTC")

    # Leadership heartbeat
    _scheduler.add_job(
        _heartbeat,
        IntervalTrigger(seconds=_HEARTBEAT_INTERVAL),
        id="leader_heartbeat",
        name="Leader heartbeat",
        replace_existing=True,
    )

    _job_kwargs = dict(
        misfire_grace_time=300,  # tolerate up to 5min late firing
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

    _scheduler.add_job(
        _run_stock_list_update, CronTrigger(hour=5, minute=30),
        id="update_stock_list", name="Update stock list",
        **_job_kwargs,
    )
    _scheduler.add_job(
        _run_profile_collection_all,
        CronTrigger(day_of_week="sun", hour=6, minute=0),
        id="build_stock_kb", name="Build stock profiles",
        **_job_kwargs,
    )
    _scheduler.add_job(
        _run_concept_sync,
        CronTrigger(day_of_week="mon-sat", hour=6, minute=0),
        id="sync_concept_boards", name="Sync concept boards",
        **_job_kwargs,
    )

    _scheduler.add_job(
        _run_financials, CronTrigger(day_of_week="sun", hour=7, minute=0),
        args=["cn"], id="collect_financials_cn", name="Collect CN financials",
        **_job_kwargs,
    )
    _scheduler.add_job(
        _run_financials, CronTrigger(day_of_week="sun", hour=7, minute=30),
        args=["hk"], id="collect_financials_hk", name="Collect HK financials",
        **_job_kwargs,
    )
    _scheduler.add_job(
        _run_financials, CronTrigger(day_of_week="sun", hour=8, minute=0),
        args=["us"], id="collect_financials_us", name="Collect US financials",
        **_job_kwargs,
    )

    _scheduler.add_job(
        _run_analyst_ratings, CronTrigger(hour=23, minute=30),
        args=["us"], id="collect_analyst_us", name="Collect US analyst ratings",
        **_job_kwargs,
    )
    _scheduler.add_job(
        _run_analyst_ratings, CronTrigger(hour=10, minute=30),
        args=["hk"], id="collect_analyst_hk", name="Collect HK analyst ratings",
        **_job_kwargs,
    )

    _scheduler.add_job(
        _run_northbound, CronTrigger(day_of_week="mon-fri", hour=9, minute=30),
        id="collect_northbound", name="Collect CN northbound",
        **_job_kwargs,
    )

    _scheduler.add_job(
        _run_institutional_holders, CronTrigger(day=1, hour=8, minute=0),
        args=["us"], id="collect_inst_holders_us", name="Collect US institutional holders",
        **_job_kwargs,
    )
    _scheduler.add_job(
        _run_institutional_holders, CronTrigger(day=1, hour=8, minute=30),
        args=["hk"], id="collect_inst_holders_hk", name="Collect HK institutional holders",
        **_job_kwargs,
    )
    _scheduler.add_job(
        _run_fund_holdings, CronTrigger(day=1, hour=10, minute=0),
        id="collect_fund_holdings", name="Collect CN fund holdings",
        **_job_kwargs,
    )

    _scheduler.start()
    logger.info("Scheduler started with %d jobs", len(_scheduler.get_jobs()))


async def _teardown_scheduler() -> None:
    """Shut down the AsyncIOScheduler instance if running."""
    global _scheduler

    if _scheduler is not None:
        try:
            _scheduler.shutdown(wait=False)
        except Exception:
            pass
        _scheduler = None
        logger.info("Scheduler shut down")


async def request_relinquish() -> None:
    """Force this instance to drop leadership and trigger a re-election.

    Used by the admin /restart endpoint to recover when a worker is
    stuck or wedged.  Safe to call from any worker — the supervisor
    loop will re-acquire on the next tick.
    """
    global _is_leader

    if _is_leader:
        logger.warning(
            "Scheduler: relinquish requested, tearing down (instance=%s)",
            _INSTANCE_ID[:8],
        )
        await _teardown_scheduler()
        await _release_leadership()
        _is_leader = False


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

    On failure: tear down the scheduler and reset _is_leader.  The
    supervisor will pick things back up on its next tick — this
    instance no longer commits suicide on a single transient blip.
    """
    global _is_leader

    try:
        r = await get_redis()
        renewed = await r.eval(
            _RENEW_LEADER_LUA, 1, _LEADER_KEY, _INSTANCE_ID, _LEADER_TTL,
        )
        if renewed:
            return
    except Exception as exc:
        logger.warning(
            "Scheduler: heartbeat error (instance=%s): %s — staying leader",
            _INSTANCE_ID[:8], exc,
        )
        # Fail-open: keep running and try again next tick
        return

    # Lua returned 0 — key is gone or owned by another instance
    logger.warning(
        "Scheduler: leadership lost during heartbeat (instance=%s) — "
        "tearing down, supervisor will retry election",
        _INSTANCE_ID[:8],
    )
    _is_leader = False
    await _teardown_scheduler()


# ---------------------------------------------------------------------------
# Leadership helpers
# ---------------------------------------------------------------------------


async def _try_acquire_leadership() -> bool:
    """Try to acquire the leader key via SET NX."""
    try:
        r = await get_redis()
        acquired = await r.set(
            _LEADER_KEY, _INSTANCE_ID, nx=True, ex=_LEADER_TTL,
        )
        return bool(acquired)
    except Exception as exc:
        logger.warning("Failed to acquire leadership: %s", exc)
        return False


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
    if not _is_leader:
        return
    logger.info("Scheduler: starting collection for market=%s", market)
    try:
        from app.services import collection_service
        result = await collection_service.collect_market(market, triggered_by="scheduler")
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


async def _run_financials(market: str) -> None:
    if not _is_leader:
        return
    logger.info("Scheduler: starting financials collection for market=%s", market)
    try:
        from app.services import fundamentals_collection_service
        result = await fundamentals_collection_service.collect_financials(market, triggered_by="scheduler")
        logger.info(
            "Scheduler: financials for %s complete — symbols=%d, upserted=%d, errors=%d",
            market, result.get("symbol_count", 0), result.get("upserted", 0), len(result.get("errors", [])),
        )
    except Exception as exc:
        logger.exception("Scheduler: financials for %s failed: %s", market, exc)


async def _run_analyst_ratings(market: str) -> None:
    if not _is_leader:
        return
    logger.info("Scheduler: starting analyst ratings for market=%s", market)
    try:
        from app.services import fundamentals_collection_service
        result = await fundamentals_collection_service.collect_analyst_ratings(market, triggered_by="scheduler")
        logger.info(
            "Scheduler: analyst ratings for %s complete — symbols=%d, upserted=%d, errors=%d",
            market, result.get("symbol_count", 0), result.get("upserted", 0), len(result.get("errors", [])),
        )
    except Exception as exc:
        logger.exception("Scheduler: analyst ratings for %s failed: %s", market, exc)


async def _run_northbound() -> None:
    if not _is_leader:
        return
    logger.info("Scheduler: starting northbound collection")
    try:
        from app.services import fundamentals_collection_service
        result = await fundamentals_collection_service.collect_northbound("cn", triggered_by="scheduler")
        logger.info(
            "Scheduler: northbound complete — symbols=%d, upserted=%d, errors=%d",
            result.get("symbol_count", 0), result.get("upserted", 0), len(result.get("errors", [])),
        )
    except Exception as exc:
        logger.exception("Scheduler: northbound failed: %s", exc)


async def _run_institutional_holders(market: str) -> None:
    if not _is_leader:
        return
    logger.info("Scheduler: starting institutional holders for market=%s", market)
    try:
        from app.services import fundamentals_collection_service
        result = await fundamentals_collection_service.collect_institutional_holders(market, triggered_by="scheduler")
        logger.info(
            "Scheduler: institutional holders for %s complete — symbols=%d, upserted=%d",
            market, result.get("symbol_count", 0), result.get("upserted", 0),
        )
    except Exception as exc:
        logger.exception("Scheduler: institutional holders for %s failed: %s", market, exc)


async def _run_fund_holdings() -> None:
    if not _is_leader:
        return
    logger.info("Scheduler: starting fund holdings collection")
    try:
        from app.services import fundamentals_collection_service
        result = await fundamentals_collection_service.collect_fund_holdings("cn", triggered_by="scheduler")
        logger.info(
            "Scheduler: fund holdings complete — symbols=%d, upserted=%d",
            result.get("symbol_count", 0), result.get("upserted", 0),
        )
    except Exception as exc:
        logger.exception("Scheduler: fund holdings failed: %s", exc)


async def _run_concept_sync() -> None:
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
