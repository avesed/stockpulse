"""Health check endpoint for StockPulse."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Response

from app.core.redis import get_redis
from app.core.database import get_db_pool

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check(response: Response):
    """Health check with Redis, database, executor, and provider key status.

    Returns HTTP 503 when degraded so Docker health checks
    (``curl -f``) correctly report the container as unhealthy.
    """
    checks: dict = {"status": "healthy", "service": "stockpulse"}

    # Check Redis
    try:
        r = await get_redis()
        await r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        logger.warning("Redis health check failed: %s", e)
        checks["redis"] = f"error: {e}"
        checks["status"] = "degraded"

    # Check Database pool
    try:
        pool = get_db_pool()
        async with pool.acquire(timeout=5) as conn:
            await conn.fetchval("SELECT 1")
        checks["database"] = "ok"
    except RuntimeError:
        checks["database"] = "not_initialized"
    except Exception as e:
        logger.warning("Database health check failed: %s", e)
        checks["database"] = f"error: {e}"
        checks["status"] = "degraded"

    # Check ThreadPoolExecutor health
    try:
        from app.core.executor import check_executor_health
        executor_ok = await check_executor_health()
        checks["executor"] = "ok" if executor_ok else "stuck"
        if not executor_ok:
            checks["status"] = "degraded"
    except Exception as e:
        logger.warning("Executor health check error: %s", e)
        checks["executor"] = f"error: {e}"
        checks["status"] = "degraded"

    # Report which API keys are configured (never expose actual keys)
    try:
        from app.core.api_keys import get_api_key
        def _provider_status(name: str) -> str:
            return "ok" if get_api_key(name) else "unconfigured"
        checks["providers"] = {
            "yfinance": "ok",
            "akshare": "ok",
            "finnhub": _provider_status("finnhub"),
            "tiingo": _provider_status("tiingo"),
            "tushare": _provider_status("tushare"),
        }
    except Exception:
        checks["providers"] = "not_initialized"

    if checks["status"] == "degraded":
        response.status_code = 503

    return checks
