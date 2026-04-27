"""Admin system settings endpoints (key-value store)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import require_admin
from app.core.orm import get_db
from app.models.system_setting import SystemSetting
from app.models.user import User
from app.schemas.base import CamelModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin/settings", tags=["admin"])


class SettingResponse(CamelModel):
    key: str
    value: str
    updated_at: datetime | None = None


class SettingUpdateRequest(CamelModel):
    value: str


# Protected keys that cannot be modified via admin API
_PROTECTED_KEYS = {"jwt_secret_key"}


@router.get("", response_model=list[SettingResponse])
async def list_settings(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(SystemSetting).order_by(SystemSetting.key)
    )
    settings = result.scalars().all()
    # Filter out protected keys from display
    return [s for s in settings if s.key not in _PROTECTED_KEYS]


@router.put("/{key}", response_model=SettingResponse)
async def upsert_setting(
    key: str,
    body: SettingUpdateRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    if key in _PROTECTED_KEYS:
        raise HTTPException(status_code=403, detail="This setting cannot be modified")

    await db.execute(
        text(
            "INSERT INTO system_settings (key, value, updated_at) "
            "VALUES (:key, :value, :now) "
            "ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = :now"
        ),
        {"key": key, "value": body.value, "now": datetime.now(timezone.utc)},
    )
    await db.commit()

    result = await db.execute(
        select(SystemSetting).where(SystemSetting.key == key)
    )
    setting = result.scalar_one()

    logger.info("Setting '%s' updated by %s", key, admin.email)
    return setting


@router.delete("/{key}", status_code=204)
async def delete_setting(
    key: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    if key in _PROTECTED_KEYS:
        raise HTTPException(status_code=403, detail="This setting cannot be deleted")

    result = await db.execute(
        select(SystemSetting).where(SystemSetting.key == key)
    )
    setting = result.scalar_one_or_none()
    if setting is None:
        raise HTTPException(status_code=404, detail="Setting not found")

    await db.delete(setting)
    logger.info("Setting '%s' deleted by %s", key, admin.email)
