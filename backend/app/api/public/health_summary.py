"""Public health summary endpoint — X-API-Key protected aggregated health for external dashboards."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from app.core.auth import verify_api_key
from app.core.database import get_db_pool
from app.core.executor import ExecutorPool, check_executor_health
from app.core.redis import cache_get, cache_set, get_redis
from app.schemas.base import ApiResponse
from app.schemas.health import (
    HealthSummary,
    MarketCollectionStatus,
    ProviderHealth,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/data/health",
    tags=["Public - Health"],
    dependencies=[Depends(verify_api_key)],
)

# Aggregation over stock_daily_bars (~34M rows) is expensive; cache 60s.
_AGG_CACHE_KEY = "sp:health:agg"
_AGG_CACHE_TTL = 60


@router.get("/summary", response_model=ApiResponse[HealthSummary])
async def get_health_summary() -> ApiResponse[HealthSummary]:
    """Aggregated health summary for external consumer dashboards.

    Returns service status, provider health, and per-market collection status.
    Safe to expose to consumers — no sensitive data (no consumer list, no request stats).
    """
    started = datetime.now(timezone.utc)

    # 1. Service-level health checks
    redis_ok = "ok"
    try:
        r = await get_redis()
        await r.ping()
    except Exception as e:
        redis_ok = "down"
        logger.warning("Redis health check failed: %s", e)

    db_ok = "ok"
    pool = None
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
    except Exception as e:
        db_ok = "down"
        logger.warning("Database health check failed: %s", e)

    # Executor health: probe the frontend pool (used by the API itself)
    executor_ok = "ok"
    try:
        healthy = await check_executor_health(ExecutorPool.FRONTEND)
        executor_ok = "ok" if healthy else "degraded"
    except Exception as e:
        logger.warning("Executor health probe failed: %s", e)
        executor_ok = "degraded"

    # 2 + 3. Provider health and per-market aggregation — cached together (60s)
    # because the bar count over ~34M rows is the dominant cost.
    providers: list[ProviderHealth] = []
    markets: list[MarketCollectionStatus] = []

    cached = await cache_get(_AGG_CACHE_KEY) if redis_ok == "ok" else None
    if cached is not None:
        try:
            providers = [ProviderHealth(**p) for p in cached.get("providers", [])]
            markets = [MarketCollectionStatus(**m) for m in cached.get("markets", [])]
        except Exception as e:
            logger.warning("Cached health aggregation invalid, recomputing: %s", e)
            cached = None

    if cached is None and db_ok == "ok" and pool is not None:
        try:
            async with pool.acquire() as conn:
                provider_rows = await conn.fetch(
                    """
                    SELECT provider_name, is_enabled,
                           health_status, last_health_check, error_message
                    FROM provider_configs
                    ORDER BY provider_name
                    """
                )
                bar_rows = await conn.fetch(
                    """
                    SELECT market,
                           MAX(date) AS last_date,
                           COUNT(*) AS total_bars
                    FROM stock_daily_bars
                    GROUP BY market
                    """
                )
                symbol_rows = await conn.fetch(
                    """
                    SELECT market, COUNT(*) AS total_symbols
                    FROM stock_symbols
                    GROUP BY market
                    """
                )

            for row in provider_rows:
                providers.append(
                    ProviderHealth(
                        name=row["provider_name"],
                        enabled=row["is_enabled"],
                        health_status=row["health_status"] or "unknown",
                        last_check=row["last_health_check"],
                        error_message=row["error_message"],
                    )
                )

            symbols_by_market = {r["market"]: r["total_symbols"] for r in symbol_rows}
            seen: set[str] = set()
            for row in bar_rows:
                market = row["market"]
                seen.add(market)
                last_at = None
                if row["last_date"]:
                    last_at = datetime.combine(
                        row["last_date"],
                        datetime.min.time(),
                        tzinfo=timezone.utc,
                    )
                markets.append(
                    MarketCollectionStatus(
                        market=market,
                        last_collection_at=last_at,
                        total_bars=row["total_bars"] or 0,
                        total_symbols=symbols_by_market.get(market, 0),
                    )
                )
            for market, sym_count in symbols_by_market.items():
                if market not in seen:
                    markets.append(
                        MarketCollectionStatus(
                            market=market,
                            last_collection_at=None,
                            total_bars=0,
                            total_symbols=sym_count,
                        )
                    )
            markets.sort(key=lambda m: m.market)

            await cache_set(
                _AGG_CACHE_KEY,
                {
                    "providers": [p.model_dump(mode="json") for p in providers],
                    "markets": [m.model_dump(mode="json") for m in markets],
                },
                ttl=_AGG_CACHE_TTL,
            )
        except Exception as e:
            logger.warning("Failed to fetch health aggregation: %s", e)

    # 4. Overall status: degraded if any critical service is down
    overall_status = "healthy"
    if redis_ok != "ok" or db_ok != "ok" or executor_ok != "ok":
        overall_status = "degraded"

    summary = HealthSummary(
        status=overall_status,
        redis=redis_ok,
        database=db_ok,
        executor=executor_ok,
        providers=providers,
        markets=markets,
    )

    elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
    return ApiResponse[HealthSummary](
        success=True,
        data=summary,
        elapsed_ms=elapsed_ms,
    )
