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

    if body.display_name is not None:
        provider.display_name = body.display_name
    if body.is_enabled is not None:
        provider.is_enabled = body.is_enabled
    if body.api_key is not None:
        provider.api_key = body.api_key if body.api_key else None
    if body.config_json is not None:
        provider.config_json = body.config_json
    provider.updated_at = datetime.now(timezone.utc)

    # Notify api_keys module to reload
    try:
        r = await get_redis()
        await r.publish("sp:reload_provider_keys", "updated")
    except Exception as e:
        logger.warning("Failed to publish key reload: %s", e)

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
    """Test provider connectivity (basic health check)."""
    result = await db.execute(
        select(ProviderConfig).where(ProviderConfig.id == provider_id)
    )
    provider = result.scalar_one_or_none()
    if provider is None:
        raise HTTPException(status_code=404, detail="Provider not found")

    # Basic connectivity test — will be enhanced in Phase 4 with actual provider calls
    import time
    start = time.monotonic()

    try:
        from app.core.api_keys import get_api_key
        key = get_api_key(provider.provider_name)

        # Providers that don't need API keys
        no_key_providers = {"yfinance", "akshare"}

        if provider.provider_name in no_key_providers:
            message = f"{provider.display_name} does not require an API key"
            success = True
        elif key:
            message = f"API key configured for {provider.display_name}"
            success = True
        else:
            message = f"No API key configured for {provider.display_name}"
            success = False

        elapsed = int((time.monotonic() - start) * 1000)

        # Update health status
        provider.last_health_check = datetime.now(timezone.utc)
        provider.health_status = "healthy" if success else "error"
        provider.error_message = None if success else message

        return ProviderTestResult(success=success, message=message, elapsed_ms=elapsed)

    except Exception as e:
        elapsed = int((time.monotonic() - start) * 1000)
        provider.last_health_check = datetime.now(timezone.utc)
        provider.health_status = "error"
        provider.error_message = str(e)
        return ProviderTestResult(success=False, message=str(e), elapsed_ms=elapsed)
