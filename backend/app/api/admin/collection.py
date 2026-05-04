"""Admin endpoints for daily bar, stock list, and stock profile collection.

Daily bars:
  POST /api/v1/admin/collection/daily-bars/{market}/collect  -- trigger collection
  POST /api/v1/admin/collection/daily-bars/{market}/rebuild  -- delete + re-collect
  GET  /api/v1/admin/collection/daily-bars/{market}/progress -- get collection progress
  POST /api/v1/admin/collection/daily-bars/{market}/unlock   -- force-release lock

Collection runs (audit):
  GET  /api/v1/admin/collection/runs          -- paginated list with optional market filter
  GET  /api/v1/admin/collection/runs/{run_id} -- single run detail with errors

Stock list:
  POST /api/v1/admin/collection/stock-list/build     -- trigger stock list build
  GET  /api/v1/admin/collection/stock-list/progress  -- get stock list build progress

Stock profiles:
  POST /api/v1/admin/collection/stock-profiles/{market}/collect  -- trigger profile collection
  GET  /api/v1/admin/collection/stock-profiles/{market}/progress -- get profile progress
  POST /api/v1/admin/collection/stock-profiles/{market}/unlock   -- force-release profile lock
  GET  /api/v1/admin/collection/stock-profiles/{market}/download  -- download profiles JSON
  GET  /api/v1/admin/collection/stock-profiles/cn/concept-mapping/download -- download concept mapping

All endpoints are protected by require_admin (admin-only auth).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.auth import require_admin

logger = logging.getLogger(__name__)

_VALID_MARKETS = {"us", "hk", "cn", "metal"}
_VALID_PROFILE_MARKETS = {"cn", "us", "hk"}

router = APIRouter(
    prefix="/api/v1/admin/collection",
    tags=["collection"],
    dependencies=[Depends(require_admin)],
)

# Track background tasks so we can avoid launching duplicates
_running_tasks: dict[str, asyncio.Task] = {}

# Separate tracking for profile collection tasks
_running_profile_tasks: dict[str, asyncio.Task] = {}


def _validate_market(market: str) -> str:
    """Normalize and validate market code."""
    market = market.lower()
    if market not in _VALID_MARKETS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown market: {market}. Supported: {', '.join(sorted(_VALID_MARKETS))}",
        )
    return market


def _validate_profile_market(market: str) -> str:
    """Normalize and validate market code for profile collection."""
    market = market.lower()
    if market not in _VALID_PROFILE_MARKETS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown profile market: {market}. Supported: {', '.join(sorted(_VALID_PROFILE_MARKETS))}",
        )
    return market


def _cleanup_task(market: str, task: asyncio.Task) -> None:
    """Callback to remove a completed task from the tracking dict."""
    _running_tasks.pop(market, None)
    if task.cancelled():
        logger.info("Collection task for %s was cancelled", market)
    elif task.exception():
        logger.error(
            "Collection task for %s raised: %s",
            market, task.exception(),
        )


def _cleanup_profile_task(market: str, task: asyncio.Task) -> None:
    """Callback to remove a completed profile task from the tracking dict."""
    _running_profile_tasks.pop(market, None)
    if task.cancelled():
        logger.info("Profile collection task for %s was cancelled", market)
    elif task.exception():
        logger.error(
            "Profile collection task for %s raised: %s",
            market, task.exception(),
        )


@router.post("/daily-bars/{market}/collect")
async def trigger_collect(market: str) -> dict[str, Any]:
    """Trigger daily bar collection for a market as a background task.

    Returns immediately with status. The collection runs asynchronously.
    """
    market = _validate_market(market)

    # Check if a task is already running for this market
    existing = _running_tasks.get(market)
    if existing is not None and not existing.done():
        return {
            "status": "already_running",
            "market": market,
            "message": f"Collection for {market} is already in progress",
        }

    from app.services import collection_service

    task = asyncio.create_task(
        collection_service.collect_market(market),
        name=f"collect_{market}",
    )
    task.add_done_callback(lambda t: _cleanup_task(market, t))
    _running_tasks[market] = task

    logger.info("Collection task launched for market=%s", market)

    return {
        "status": "started",
        "market": market,
        "message": f"Collection for {market} started in background",
    }


@router.post("/daily-bars/{market}/rebuild")
async def trigger_rebuild(market: str) -> dict[str, Any]:
    """Trigger daily bar rebuild (delete + re-collect) as a background task.

    Returns immediately with status.
    """
    market = _validate_market(market)

    existing = _running_tasks.get(market)
    if existing is not None and not existing.done():
        return {
            "status": "already_running",
            "market": market,
            "message": f"A collection/rebuild for {market} is already in progress",
        }

    from app.services import collection_service

    task = asyncio.create_task(
        collection_service.rebuild_market(market),
        name=f"rebuild_{market}",
    )
    task.add_done_callback(lambda t: _cleanup_task(market, t))
    _running_tasks[market] = task

    logger.info("Rebuild task launched for market=%s", market)

    return {
        "status": "started",
        "market": market,
        "message": f"Rebuild for {market} started in background",
    }


@router.get("/daily-bars/{market}/progress")
async def get_progress(market: str) -> dict[str, Any]:
    """Get collection progress for a market.

    Returns the progress dict from Redis, or null if no collection is active.
    Transforms backend keys to frontend-friendly names.
    """
    market = _validate_market(market)

    from app.services import collection_service

    raw = await collection_service.get_progress(market)

    # Transform to frontend shape
    progress = None
    last_run = None
    if raw is not None:
        last_run = raw.pop("lastRun", None)
        # Only build progress if there are actual progress fields
        if "symbolsDone" in raw:
            progress = {
                "current": raw.get("symbolsDone", 0),
                "total": raw.get("symbolsTotal", 0),
                "message": f"{raw.get('newBars', 0)} new bars",
                "elapsedSeconds": raw.get("elapsedSeconds"),
                "errorsCount": raw.get("errorsCount", 0),
                "estimatedRemaining": raw.get("estimatedRemaining"),
                "startedAt": raw.get("startedAt"),
                "lastRun": last_run,
            }

    return {
        "market": market,
        "progress": progress,
        "lastRun": last_run,
        "taskRunning": (
            market in _running_tasks
            and not _running_tasks[market].done()
        ),
    }


# ---------------------------------------------------------------------------
# Fundamentals collection (3 independent jobs)
# ---------------------------------------------------------------------------

_VALID_FUND_JOBS = {"financials", "analyst_ratings", "northbound", "institutional_holders", "fund_holdings"}
_VALID_FUND_MARKETS = {"us", "hk", "cn"}
_running_fund_tasks: dict[str, asyncio.Task] = {}


def _cleanup_fund_task(key: str, task: asyncio.Task) -> None:
    _running_fund_tasks.pop(key, None)
    if task.exception():
        logger.error("Fund task %s raised: %s", key, task.exception())


@router.post("/fundamentals/{job_type}/{market}/collect")
async def trigger_fund_collect(job_type: str, market: str) -> dict[str, Any]:
    """Trigger a fundamentals sub-job (financials / analyst_ratings / northbound)."""
    job_type = job_type.lower()
    market = market.lower()
    if job_type not in _VALID_FUND_JOBS:
        raise HTTPException(status_code=400, detail=f"Unknown job: {job_type}. Use: {', '.join(sorted(_VALID_FUND_JOBS))}")
    if market not in _VALID_FUND_MARKETS:
        raise HTTPException(status_code=400, detail=f"Unsupported market: {market}")

    task_key = f"{job_type}_{market}"
    existing = _running_fund_tasks.get(task_key)
    if existing is not None and not existing.done():
        return {"status": "already_running", "jobType": job_type, "market": market}

    from app.services import fundamentals_collection_service

    job_fn = {
        "financials": fundamentals_collection_service.collect_financials,
        "analyst_ratings": fundamentals_collection_service.collect_analyst_ratings,
        "northbound": fundamentals_collection_service.collect_northbound,
        "institutional_holders": fundamentals_collection_service.collect_institutional_holders,
        "fund_holdings": fundamentals_collection_service.collect_fund_holdings,
    }[job_type]

    task = asyncio.create_task(
        job_fn(market, triggered_by="api"),
        name=f"{job_type}_{market}",
    )
    task.add_done_callback(lambda t: _cleanup_fund_task(task_key, t))
    _running_fund_tasks[task_key] = task

    return {"status": "started", "jobType": job_type, "market": market}


@router.get("/fundamentals/{job_type}/{market}/progress")
async def get_fund_progress(job_type: str, market: str) -> dict[str, Any]:
    """Get fundamentals sub-job progress (same shape as daily-bars progress)."""
    from app.services import fundamentals_collection_service

    raw = await fundamentals_collection_service.get_progress(job_type.lower(), market.lower())
    task_key = f"{job_type.lower()}_{market.lower()}"
    task_running = task_key in _running_fund_tasks and not _running_fund_tasks[task_key].done()

    progress = None
    if raw and "symbolsDone" in raw:
        progress = {
            "current": raw.get("symbolsDone", 0),
            "total": raw.get("symbolsTotal", 0),
            "message": f"{raw.get('upserted', 0)} upserted",
            "elapsedSeconds": raw.get("elapsedSeconds"),
            "errorsCount": raw.get("errorsCount", 0),
            "estimatedRemaining": raw.get("estimatedRemaining"),
            "startedAt": raw.get("startedAt"),
        }

    return {
        "jobType": job_type,
        "market": market,
        "progress": progress,
        "taskRunning": task_running,
    }


@router.post("/fundamentals/{job_type}/{market}/unlock")
async def force_fund_unlock(job_type: str, market: str) -> dict[str, Any]:
    """Force-release fundamentals sub-job lock."""
    from app.services import fundamentals_collection_service

    released = await fundamentals_collection_service.force_unlock(job_type.lower(), market.lower())
    return {"jobType": job_type, "market": market, "released": released}


# ---------------------------------------------------------------------------
# Collection run history (audit)
# ---------------------------------------------------------------------------


@router.get("/runs")
async def list_runs(
    market: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """Paginated list of collection runs."""
    from app.services import collection_run_service

    runs, total = await collection_run_service.get_runs(
        market=market.lower() if market else None,
        limit=limit,
        offset=offset,
    )
    return {"runs": runs, "total": total}


@router.get("/runs/{run_id}")
async def get_run_detail(run_id: int) -> dict[str, Any]:
    """Single collection run detail including errors."""
    from app.services import collection_run_service

    run = await collection_run_service.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


# ---------------------------------------------------------------------------
# Stock list build / progress
# ---------------------------------------------------------------------------

# Track the stock list background task
_stock_list_task: Optional[asyncio.Task] = None


def _cleanup_stock_list_task(task: asyncio.Task) -> None:
    """Callback to clear the module-level stock list task ref."""
    global _stock_list_task
    _stock_list_task = None
    if task.cancelled():
        logger.info("Stock list build task was cancelled")
    elif task.exception():
        logger.error("Stock list build task raised: %s", task.exception())


@router.post("/stock-list/build")
async def trigger_stock_list_build() -> dict[str, Any]:
    """Trigger stock list build as a background task.

    Returns immediately with status.  The build runs asynchronously.
    """
    global _stock_list_task

    if _stock_list_task is not None and not _stock_list_task.done():
        return {
            "status": "already_running",
            "message": "Stock list build is already in progress",
        }

    from app.services import stock_list_persistence

    _stock_list_task = asyncio.create_task(
        stock_list_persistence.build_and_save_stock_list(),
        name="build_stock_list",
    )
    _stock_list_task.add_done_callback(_cleanup_stock_list_task)

    logger.info("Stock list build task launched")

    return {
        "status": "started",
        "message": "Stock list build started in background",
    }


@router.get("/stock-list/progress")
async def get_stock_list_progress() -> dict[str, Any]:
    """Get stock list build progress/status.

    Returns the progress dict from Redis, plus whether a build task is
    currently running in this worker.
    """
    from app.services import stock_list_persistence

    progress = await stock_list_persistence.get_progress()

    return {
        "progress": progress,
        "task_running": (
            _stock_list_task is not None
            and not _stock_list_task.done()
        ),
    }


# ---------------------------------------------------------------------------
# Daily bar unlock
# ---------------------------------------------------------------------------


@router.post("/daily-bars/{market}/unlock")
async def force_unlock(market: str) -> dict[str, Any]:
    """Force-release the collection lock for a market.

    Use this to recover from stuck tasks. Does NOT cancel running tasks.
    """
    market = _validate_market(market)

    from app.services import collection_service

    released = await collection_service.force_unlock(market)

    logger.info(
        "Force-unlock for market=%s: %s",
        market, "released" if released else "no lock found",
    )

    return {
        "market": market,
        "released": released,
        "message": (
            f"Lock for {market} released"
            if released
            else f"No lock found for {market}"
        ),
    }


# ===========================================================================
# Stock profile collection endpoints
# ===========================================================================


@router.post("/stock-profiles/{market}/collect")
async def trigger_profile_collect(market: str) -> dict[str, Any]:
    """Trigger stock profile collection for a market as a background task.

    Collects profiles using data providers (akshare for CN, yfinance for
    US/HK) and saves results to disk as JSON for later download.

    Returns immediately with status. The collection runs asynchronously.
    """
    market = _validate_profile_market(market)

    # Check if a task is already running for this market
    existing = _running_profile_tasks.get(market)
    if existing is not None and not existing.done():
        return {
            "status": "already_running",
            "market": market,
            "message": f"Profile collection for {market} is already in progress",
        }

    from app.services import profile_collection_service

    task = asyncio.create_task(
        profile_collection_service.collect_market_profiles(market),
        name=f"profile_collect_{market}",
    )
    task.add_done_callback(lambda t: _cleanup_profile_task(market, t))
    _running_profile_tasks[market] = task

    logger.info("Profile collection task launched for market=%s", market)

    return {
        "status": "started",
        "market": market,
        "message": f"Profile collection for {market} started in background",
    }


@router.get("/stock-profiles/{market}/progress")
async def get_profile_progress(market: str) -> dict[str, Any]:
    """Get profile collection progress for a market.

    Returns the progress dict from Redis, or null if no collection is active.
    """
    market = _validate_profile_market(market)

    from app.services import profile_collection_service

    progress = await profile_collection_service.get_collection_progress(market)
    metadata = await profile_collection_service.get_collection_metadata(market)

    return {
        "market": market,
        "progress": progress,
        "metadata": metadata,
        "task_running": (
            market in _running_profile_tasks
            and not _running_profile_tasks[market].done()
        ),
    }


@router.post("/stock-profiles/{market}/unlock")
async def force_profile_unlock(market: str) -> dict[str, Any]:
    """Force-release the profile collection lock for a market.

    Use this to recover from stuck tasks. Does NOT cancel running tasks.
    """
    market = _validate_profile_market(market)

    from app.services import profile_collection_service

    released = await profile_collection_service.force_unlock(market)

    logger.info(
        "Force-unlock profile lock for market=%s: %s",
        market, "released" if released else "no lock found",
    )

    return {
        "market": market,
        "released": released,
        "message": (
            f"Profile lock for {market} released"
            if released
            else f"No profile lock found for {market}"
        ),
    }


@router.get("/stock-profiles/{market}/download")
async def download_profiles(market: str) -> dict[str, Any]:
    """Download pre-collected profiles for a market as JSON.

    Returns the profiles list, metadata, and the concept mapping
    (for CN market) if available.

    Raises 404 if no profiles have been collected yet.
    """
    market = _validate_profile_market(market)

    from app.services import profile_collection_service

    profiles = await profile_collection_service.get_market_profiles(market)
    if profiles is None:
        raise HTTPException(
            status_code=404,
            detail=f"No pre-collected profiles available for {market}. "
                   f"Trigger collection first.",
        )

    metadata = await profile_collection_service.get_collection_metadata(market)

    result: dict[str, Any] = {
        "market": market,
        "count": len(profiles),
        "profiles": profiles,
        "metadata": metadata,
    }

    # Include concept mapping for CN market
    if market == "cn":
        mapping = await profile_collection_service.get_concept_mapping()
        if mapping:
            result["concept_mapping"] = mapping

    return result


@router.get("/stock-profiles/cn/concept-mapping/download")
async def download_concept_mapping() -> dict[str, Any]:
    """Download pre-collected CN concept mapping.

    Returns the mapping dict with ``concepts`` and ``names`` keys.

    Raises 404 if no concept mapping has been collected yet.
    """
    from app.services import profile_collection_service

    mapping = await profile_collection_service.get_concept_mapping()
    if mapping is None:
        raise HTTPException(
            status_code=404,
            detail="No pre-collected concept mapping available. "
                   "Trigger CN profile collection or concept sync first.",
        )

    return {
        "concepts": mapping.get("concepts", {}),
        "names": mapping.get("names", {}),
        "count": len(mapping.get("concepts", {})),
    }


# -----------------------------------------------------------------------
# ML Data Collection
# -----------------------------------------------------------------------
_VALID_ML_JOBS = {
    "valuation_history", "insider_sentiment", "insider_transactions",
    "earnings_surprises", "recommendation_trends", "upgrades_downgrades",
    "sec_financials", "earnings_calendar", "options_sentiment",
    "short_interest", "economic_indicators", "macro_daily",
    "cn_alternative",
}
_running_ml_tasks: dict[str, asyncio.Task] = {}


def _cleanup_ml_task(key: str, task: asyncio.Task) -> None:
    _running_ml_tasks.pop(key, None)


@router.post("/ml/{job_type}/{market}/collect")
async def trigger_ml_collect(job_type: str, market: str) -> dict[str, Any]:
    """Trigger an ML data collection job."""
    job_type = job_type.lower()
    market = market.lower()
    if job_type not in _VALID_ML_JOBS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown ML job: {job_type}. Use: {', '.join(sorted(_VALID_ML_JOBS))}",
        )

    task_key = f"ml_{job_type}_{market}"
    existing = _running_ml_tasks.get(task_key)
    if existing is not None and not existing.done():
        return {"status": "already_running", "jobType": job_type, "market": market}

    from app.services import ml_collection_service

    fn = getattr(ml_collection_service, f"collect_{job_type}", None)
    if fn is None:
        raise HTTPException(status_code=400, detail=f"No collect function for {job_type}")

    task = asyncio.create_task(fn(market, triggered_by="api"), name=task_key)
    task.add_done_callback(lambda t: _cleanup_ml_task(task_key, t))
    _running_ml_tasks[task_key] = task

    return {"status": "started", "jobType": job_type, "market": market}


@router.get("/ml/{job_type}/{market}/progress")
async def get_ml_progress(job_type: str, market: str) -> dict[str, Any]:
    """Get ML collection job progress."""
    from app.services import ml_collection_service

    raw = await ml_collection_service.get_progress(job_type.lower(), market.lower())
    task_key = f"ml_{job_type.lower()}_{market.lower()}"
    task_running = task_key in _running_ml_tasks and not _running_ml_tasks[task_key].done()

    progress = None
    if raw and "symbolsDone" in raw:
        progress = {
            "current": raw.get("symbolsDone", 0),
            "total": raw.get("symbolsTotal", 0),
            "message": f"{raw.get('upserted', 0)} upserted",
            "elapsedSeconds": raw.get("elapsedSeconds"),
            "errorsCount": raw.get("errorsCount", 0),
        }

    return {"jobType": job_type, "market": market, "progress": progress, "taskRunning": task_running}


@router.post("/ml/{job_type}/{market}/unlock")
async def force_ml_unlock(job_type: str, market: str) -> dict[str, Any]:
    """Force-release ML collection job lock."""
    from app.services import ml_collection_service

    released = await ml_collection_service.force_unlock(job_type.lower(), market.lower())
    return {"released": released, "jobType": job_type, "market": market}
