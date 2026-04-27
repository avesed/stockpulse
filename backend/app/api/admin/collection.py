"""Admin endpoints for daily bar, stock list, and stock profile collection.

Daily bars:
  POST /api/v1/admin/collection/daily-bars/{market}/collect  -- trigger collection
  POST /api/v1/admin/collection/daily-bars/{market}/rebuild  -- delete + re-collect
  GET  /api/v1/admin/collection/daily-bars/{market}/progress -- get collection progress
  POST /api/v1/admin/collection/daily-bars/{market}/unlock   -- force-release lock

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

from fastapi import APIRouter, Depends, HTTPException

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
    """
    market = _validate_market(market)

    from app.services import collection_service

    progress = await collection_service.get_progress(market)

    return {
        "market": market,
        "progress": progress,
        "task_running": (
            market in _running_tasks
            and not _running_tasks[market].done()
        ),
    }


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
