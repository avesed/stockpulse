"""Admin data provider configuration endpoints."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import require_admin
from app.core.orm import get_db
from app.core.redis import get_redis
from app.models.provider_config import ProviderConfig
from app.models.user import User
from app.schemas.base import CamelModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin/providers", tags=["admin"])


# --- Schemas ---

class ProviderResponse(CamelModel):
    id: int
    provider_name: str
    display_name: str
    is_enabled: bool
    has_api_key: bool = False
    config_json: dict | None = None
    last_health_check: datetime | None = None
    health_status: str
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime


class ProviderUpdateRequest(CamelModel):
    display_name: str | None = None
    is_enabled: bool | None = None
    api_key: str | None = None
    config_json: dict | None = None


class ProviderTestResult(CamelModel):
    success: bool
    message: str
    elapsed_ms: int | None = None


# --- Endpoints ---

@router.get("", response_model=list[ProviderResponse])
async def list_providers(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ProviderConfig).order_by(ProviderConfig.provider_name)
    )
    providers = result.scalars().all()
    return [
        ProviderResponse(
            id=p.id,
            provider_name=p.provider_name,
            display_name=p.display_name,
            is_enabled=p.is_enabled,
            has_api_key=bool(p.api_key),
            config_json=p.config_json,
            last_health_check=p.last_health_check,
            health_status=p.health_status,
            error_message=p.error_message,
            created_at=p.created_at,
            updated_at=p.updated_at,
        )
        for p in providers
    ]


@router.patch("/{provider_id}", response_model=ProviderResponse)
async def update_provider(
    provider_id: int,
    body: ProviderUpdateRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ProviderConfig).where(ProviderConfig.id == provider_id)
    )
    provider = result.scalar_one_or_none()
    if provider is None:
        raise HTTPException(status_code=404, detail="Provider not found")

    logger.info(
        "Provider update request for %s: display_name=%r, is_enabled=%r, api_key=%s, config_json=%r",
        provider.provider_name,
        body.display_name,
        body.is_enabled,
        f"[{len(body.api_key)} chars]" if body.api_key else repr(body.api_key),
        body.config_json,
    )

    if body.display_name is not None:
        provider.display_name = body.display_name
    if body.is_enabled is not None:
        provider.is_enabled = body.is_enabled
    if body.api_key is not None:
        provider.api_key = body.api_key if body.api_key else None
    if body.config_json is not None:
        provider.config_json = body.config_json
    provider.updated_at = datetime.now(timezone.utc)

    # Commit first so the subscriber reads fresh data
    await db.commit()

    # Notify api_keys module to reload + reset router singleton
    try:
        r = await get_redis()
        await r.publish("sp:reload_provider_keys", "updated")
    except Exception as e:
        logger.warning("Failed to publish key reload: %s", e)

    # Reset StockRouter so it picks up newly enabled/disabled providers
    from app.services.stock_router import reset_router
    reset_router()

    logger.info("Provider %s updated by %s", provider.provider_name, admin.email)

    return ProviderResponse(
        id=provider.id,
        provider_name=provider.provider_name,
        display_name=provider.display_name,
        is_enabled=provider.is_enabled,
        has_api_key=bool(provider.api_key),
        config_json=provider.config_json,
        last_health_check=provider.last_health_check,
        health_status=provider.health_status,
        error_message=provider.error_message,
        created_at=provider.created_at,
        updated_at=provider.updated_at,
    )


@router.post("/{provider_id}/test", response_model=ProviderTestResult)
async def test_provider(
    provider_id: int,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Test provider connectivity with a real API call."""
    result = await db.execute(
        select(ProviderConfig).where(ProviderConfig.id == provider_id)
    )
    provider = result.scalar_one_or_none()
    if provider is None:
        raise HTTPException(status_code=404, detail="Provider not found")

    import time
    start = time.monotonic()

    try:
        # First check if API key is configured (for providers that need one)
        from app.core.api_keys import get_api_key
        key = provider.api_key or get_api_key(provider.provider_name)
        no_key_providers = {"yfinance", "akshare"}

        if provider.provider_name not in no_key_providers and not key:
            elapsed = int((time.monotonic() - start) * 1000)
            message = f"No API key configured for {provider.display_name}"
            provider.last_health_check = datetime.now(timezone.utc)
            provider.health_status = "error"
            provider.error_message = message
            return ProviderTestResult(success=False, message=message, elapsed_ms=elapsed)

        # Real connectivity test: fetch a quote for a known symbol
        from app.services.stock_router import get_stock_router

        # Test symbols ordered by reliability (US/HK first, A-shares last)
        _TEST_SYMBOLS = [
            ("us", "AAPL"),
            ("hk", "0700.HK"),
            ("sh", "600519.SS"),
            ("sz", "000858.SZ"),
            ("metal", "GC=F"),
        ]

        sr = await get_stock_router()
        p = sr.get_provider_by_name(provider.provider_name)

        if p is None:
            # No REST provider (e.g. Finnhub is WS-only) — fall back to key check
            elapsed = int((time.monotonic() - start) * 1000)
            if key:
                message = f"API key configured for {provider.display_name} (WS-only, no REST test available)"
                success = True
            else:
                message = f"No API key configured for {provider.display_name}"
                success = False
            provider.last_health_check = datetime.now(timezone.utc)
            provider.health_status = "healthy" if success else "error"
            provider.error_message = None if success else message
            return ProviderTestResult(success=success, message=message, elapsed_ms=elapsed)

        # Try the best test symbol for this provider's supported markets
        test_symbol = None
        test_market = None
        for market, symbol in _TEST_SYMBOLS:
            if market in p.supported_markets:
                test_symbol = symbol
                test_market = market
                break

        if not test_symbol:
            elapsed = int((time.monotonic() - start) * 1000)
            message = f"{provider.display_name} has no testable market"
            provider.last_health_check = datetime.now(timezone.utc)
            provider.health_status = "error"
            provider.error_message = message
            return ProviderTestResult(success=False, message=message, elapsed_ms=elapsed)

        logger.info(
            "Testing %s connectivity: get_quote(%s, %s)",
            provider.provider_name, test_symbol, test_market,
        )
        data = await p.get_quote(test_symbol, test_market)
        elapsed = int((time.monotonic() - start) * 1000)

        if data and data.get("price"):
            price = data["price"]
            message = (
                f"{provider.display_name} OK — "
                f"{test_symbol} = ${price:.2f} ({elapsed}ms)"
            )
            success = True
        else:
            message = (
                f"{provider.display_name} returned no data for "
                f"{test_symbol} ({elapsed}ms)"
            )
            success = False

        provider.last_health_check = datetime.now(timezone.utc)
        provider.health_status = "healthy" if success else "error"
        provider.error_message = None if success else message

        return ProviderTestResult(success=success, message=message, elapsed_ms=elapsed)

    except Exception as e:
        elapsed = int((time.monotonic() - start) * 1000)
        logger.warning("Provider test failed for %s: %s", provider.provider_name, e)
        provider.last_health_check = datetime.now(timezone.utc)
        provider.health_status = "error"
        provider.error_message = str(e)
        return ProviderTestResult(success=False, message=str(e), elapsed_ms=elapsed)
