"""Admin stats and dashboard endpoints."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query

from app.core.auth import require_admin
from app.core.database import get_db_pool
from app.core.redis import get_redis
from app.models.user import User
from app.schemas.base import CamelModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin/stats", tags=["admin"])


class DashboardStats(CamelModel):
    total_bars: int = 0
    total_symbols: int = 0
    active_consumers: int = 0
    total_providers: int = 0
    enabled_providers: int = 0
    requests_today: int = 0
    errors_today: int = 0


class ConsumerRequestStats(CamelModel):
    consumer_id: str
    consumer_name: str | None = None
    date: str
    requests: int = 0
    errors: int = 0


@router.get("/dashboard", response_model=DashboardStats)
async def get_dashboard_stats(admin: User = Depends(require_admin)):
    """Get overview stats for the dashboard."""
    pool = get_db_pool()

    # Query DB for aggregates
    async with pool.acquire(timeout=5) as conn:
        bar_count = await conn.fetchval("SELECT count(*) FROM stock_daily_bars") or 0
        symbol_count = await conn.fetchval("SELECT count(*) FROM stock_symbols") or 0
        consumer_count = await conn.fetchval(
            "SELECT count(*) FROM api_consumers WHERE is_active = true"
        ) or 0
        provider_total = await conn.fetchval("SELECT count(*) FROM provider_configs") or 0
        provider_enabled = await conn.fetchval(
            "SELECT count(*) FROM provider_configs WHERE is_enabled = true"
        ) or 0

    # Query Redis for today's request counts
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        r = await get_redis()
        requests_today = int(await r.get(f"sp:stats:total:{today}") or 0)

        # Sum all error keys for today
        error_keys = []
        cursor = "0"
        while cursor != 0:
            cursor, keys = await r.scan(cursor=cursor, match=f"sp:stats:errors:*:{today}", count=100)
            error_keys.extend(keys)
            if cursor == 0:
                break

        errors_today = 0
        if error_keys:
            values = await r.mget(*error_keys)
            errors_today = sum(int(v or 0) for v in values)
    except Exception:
        requests_today = 0
        errors_today = 0

    return DashboardStats(
        total_bars=bar_count,
        total_symbols=symbol_count,
        active_consumers=consumer_count,
        total_providers=provider_total,
        enabled_providers=provider_enabled,
        requests_today=requests_today,
        errors_today=errors_today,
    )


@router.get("/requests", response_model=list[ConsumerRequestStats])
async def get_request_stats(
    admin: User = Depends(require_admin),
    days: int = Query(7, ge=1, le=30),
):
    """Get per-consumer request stats for the last N days."""
    r = await get_redis()
    results = []

    for i in range(days):
        date = (datetime.now(timezone.utc) - timedelta(days=i)).strftime("%Y-%m-%d")

        # Scan for all consumer request keys for this date
        cursor = "0"
        while True:
            cursor, keys = await r.scan(
                cursor=cursor, match=f"sp:stats:requests:*:{date}", count=100,
            )
            for key in keys:
                # key format: sp:stats:requests:{consumer_id}:{date}
                parts = key.split(":")
                if len(parts) >= 5:
                    consumer_id = parts[3]
                    count = int(await r.get(key) or 0)

                    # Get error count
                    err_key = f"sp:stats:errors:{consumer_id}:{date}"
                    err_count = int(await r.get(err_key) or 0)

                    results.append(ConsumerRequestStats(
                        consumer_id=consumer_id,
                        date=date,
                        requests=count,
                        errors=err_count,
                    ))

            if cursor == 0 or cursor == "0":
                break

    return results


@router.get("/providers")
async def get_provider_stats(admin: User = Depends(require_admin)):
    """Get provider health overview."""
    pool = get_db_pool()
    async with pool.acquire(timeout=5) as conn:
        rows = await conn.fetch(
            "SELECT provider_name, display_name, is_enabled, health_status, "
            "last_health_check, error_message "
            "FROM provider_configs ORDER BY provider_name"
        )

    return [
        {
            "providerName": row["provider_name"],
            "displayName": row["display_name"],
            "isEnabled": row["is_enabled"],
            "healthStatus": row["health_status"],
            "lastHealthCheck": str(row["last_health_check"]) if row["last_health_check"] else None,
            "errorMessage": row["error_message"],
        }
        for row in rows
    ]
