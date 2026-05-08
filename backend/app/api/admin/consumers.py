"""Admin API consumer management endpoints."""

from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import require_admin
from app.core.orm import get_db
from app.models.api_consumer import ApiConsumer
from app.models.user import User
from app.schemas.base import CamelModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin/consumers", tags=["admin"])


# --- Schemas ---

class ConsumerCreateRequest(CamelModel):
    name: str
    description: str | None = None
    rate_limit: int = 100
    allowed_endpoints: list[str] | None = None


class ConsumerResponse(CamelModel):
    id: UUID
    name: str
    api_key_prefix: str
    description: str | None = None
    is_active: bool
    rate_limit: int
    allowed_endpoints: list[str] | None = None
    last_used_at: datetime | None = None
    created_at: datetime


class ConsumerCreateResponse(ConsumerResponse):
    """Returned only on creation — includes the raw API key."""
    raw_api_key: str


class ConsumerUsageResponse(CamelModel):
    consumer_id: UUID
    name: str
    is_active: bool
    last_used_at: datetime | None = None
    created_at: datetime | None = None


# --- Endpoints ---

@router.get("", response_model=list[ConsumerResponse])
async def list_consumers(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ApiConsumer).order_by(ApiConsumer.created_at.desc())
    )
    return result.scalars().all()


@router.post("", response_model=ConsumerCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_consumer(
    body: ConsumerCreateRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Create a new API consumer. Returns the raw API key ONCE."""
    existing = await db.execute(
        select(ApiConsumer).where(ApiConsumer.name == body.name)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Consumer with name '{body.name}' already exists",
        )

    raw_key = secrets.token_urlsafe(48)
    key_prefix = raw_key[:8]
    key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    consumer = ApiConsumer(
        name=body.name,
        api_key=key_hash,
        api_key_prefix=key_prefix,
        description=body.description,
        rate_limit=body.rate_limit,
        allowed_endpoints=body.allowed_endpoints,
    )
    db.add(consumer)
    await db.flush()

    logger.info("API consumer created: %s (prefix=%s)", body.name, key_prefix)

    return ConsumerCreateResponse(
        id=consumer.id,
        name=consumer.name,
        api_key_prefix=key_prefix,
        description=consumer.description,
        is_active=consumer.is_active,
        rate_limit=consumer.rate_limit,
        allowed_endpoints=consumer.allowed_endpoints,
        last_used_at=consumer.last_used_at,
        created_at=consumer.created_at,
        raw_api_key=raw_key,
    )


@router.delete("/{consumer_id}", status_code=status.HTTP_204_NO_CONTENT)
async def deactivate_consumer(
    consumer_id: UUID,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Deactivate an API consumer (soft delete)."""
    result = await db.execute(
        select(ApiConsumer).where(ApiConsumer.id == consumer_id)
    )
    consumer = result.scalar_one_or_none()
    if consumer is None:
        raise HTTPException(status_code=404, detail="Consumer not found")

    consumer.is_active = False
    consumer.updated_at = datetime.now(timezone.utc)


@router.delete("/{consumer_id}/permanent", status_code=status.HTTP_204_NO_CONTENT)
async def delete_consumer(
    consumer_id: UUID,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Permanently delete an API consumer."""
    result = await db.execute(
        select(ApiConsumer).where(ApiConsumer.id == consumer_id)
    )
    consumer = result.scalar_one_or_none()
    if consumer is None:
        raise HTTPException(status_code=404, detail="Consumer not found")

    logger.info("API consumer permanently deleted: %s (prefix=%s)", consumer.name, consumer.api_key_prefix)
    await db.delete(consumer)


@router.get("/{consumer_id}/usage", response_model=ConsumerUsageResponse)
async def consumer_usage(
    consumer_id: UUID,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ApiConsumer).where(ApiConsumer.id == consumer_id)
    )
    consumer = result.scalar_one_or_none()
    if consumer is None:
        raise HTTPException(status_code=404, detail="Consumer not found")

    return ConsumerUsageResponse(
        consumer_id=consumer.id,
        name=consumer.name,
        is_active=consumer.is_active,
        last_used_at=consumer.last_used_at,
        created_at=consumer.created_at,
    )
