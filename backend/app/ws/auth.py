"""WebSocket authentication helpers.

Browser WebSocket API does not support custom headers, so auth credentials
are passed via query parameters:
- Admin: ``?token=<JWT_ACCESS_TOKEN>``
- Consumer: ``?api_key=<RAW_API_KEY>``

Auth is validated *before* calling ``websocket.accept()``.
On failure the connection is closed with code 4001.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Optional

from fastapi import WebSocket
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import ALGORITHM
from app.core.secrets import get_jwt_secret
from app.core.orm import get_session_factory
from app.models.user import User
from app.models.api_consumer import ApiConsumer

logger = logging.getLogger(__name__)


async def authenticate_admin_ws(websocket: WebSocket) -> Optional[User]:
    """Validate JWT from query param, return User or None."""
    token = websocket.query_params.get("token")
    if not token:
        return None

    try:
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[ALGORITHM])
        user_id = int(payload.get("sub", 0))
        token_type = payload.get("type", "")
        if not user_id or token_type != "access":
            return None
    except JWTError:
        return None

    try:
        async with get_session_factory()() as db:
            result = await db.execute(select(User).where(User.id == user_id))
            user = result.scalar_one_or_none()
            if user is None or not user.is_active or user.role != "admin":
                return None
            return user
    except Exception:
        logger.exception("DB error during WS admin auth")
        return None


async def authenticate_consumer_ws(websocket: WebSocket) -> Optional[ApiConsumer]:
    """Validate API key from query param, return ApiConsumer or None."""
    raw_key = websocket.query_params.get("api_key")
    if not raw_key:
        return None

    key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    try:
        async with get_session_factory()() as db:
            result = await db.execute(
                select(ApiConsumer).where(
                    ApiConsumer.api_key == key_hash,
                    ApiConsumer.is_active.is_(True),
                )
            )
            consumer = result.scalar_one_or_none()
            return consumer
    except Exception:
        logger.exception("DB error during WS consumer auth")
        return None
