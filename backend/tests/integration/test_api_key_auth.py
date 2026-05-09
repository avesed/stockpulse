"""Integration tests for X-API-Key auth (verify_api_key) and get_current_user edge cases."""
from __future__ import annotations

import hashlib
import secrets

import pytest
import pytest_asyncio

from tests.integration.conftest import _get_test_sf


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _create_consumer(name, rate_limit=100, is_active=True, allowed_endpoints=None):
    """Create an ApiConsumer and return (raw_key, consumer)."""
    from app.models.api_consumer import ApiConsumer

    raw_key = secrets.token_urlsafe(48)
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    consumer = ApiConsumer(
        name=name,
        api_key=key_hash,
        api_key_prefix=raw_key[:8],
        rate_limit=rate_limit,
        is_active=is_active,
        allowed_endpoints=allowed_endpoints,
    )
    sf = _get_test_sf()
    async with sf() as session:
        session.add(consumer)
        await session.commit()
        await session.refresh(consumer)
    return raw_key, consumer


# ---------------------------------------------------------------------------
# verify_api_key via data endpoints
# ---------------------------------------------------------------------------

class TestVerifyApiKey:
    async def test_valid_key_returns_data(self, async_client):
        raw_key, _ = await _create_consumer("valid-consumer")
        resp = await async_client.get(
            "/api/v1/data/search?q=test",
            headers={"X-API-Key": raw_key},
        )
        assert resp.status_code == 200

    async def test_missing_key_returns_401(self, async_client):
        resp = await async_client.get("/api/v1/data/search?q=test")
        assert resp.status_code == 401
        assert "X-API-Key" in resp.json().get("detail", "")

    async def test_invalid_key_returns_401(self, async_client):
        resp = await async_client.get(
            "/api/v1/data/search?q=test",
            headers={"X-API-Key": "totally-invalid-key"},
        )
        assert resp.status_code == 401

    async def test_inactive_consumer_returns_401(self, async_client):
        raw_key, _ = await _create_consumer("inactive-consumer", is_active=False)
        resp = await async_client.get(
            "/api/v1/data/search?q=test",
            headers={"X-API-Key": raw_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# get_current_user edge cases (JWT Bearer auth)
# ---------------------------------------------------------------------------

class TestGetCurrentUser:
    async def test_expired_access_token(self, async_client):
        from jose import jwt
        from app.core.secrets import get_jwt_secret
        from app.core.auth import ALGORITHM
        from datetime import datetime, timezone, timedelta

        expired_payload = {
            "sub": "999",
            "role": "admin",
            "type": "access",
            "exp": datetime.now(timezone.utc) - timedelta(hours=1),
        }
        expired_token = jwt.encode(expired_payload, get_jwt_secret(), algorithm=ALGORITHM)
        resp = await async_client.get(
            "/api/v1/auth/me",
            headers={"Authorization": f"Bearer {expired_token}"},
        )
        assert resp.status_code == 401

    async def test_refresh_token_as_access_rejected(self, async_client, admin_user):
        from app.core.auth import create_refresh_token
        refresh = create_refresh_token(admin_user.id)
        resp = await async_client.get(
            "/api/v1/auth/me",
            headers={"Authorization": f"Bearer {refresh}"},
        )
        assert resp.status_code == 401

    async def test_deleted_user_returns_401(self, async_client):
        from jose import jwt
        from app.core.secrets import get_jwt_secret
        from app.core.auth import ALGORITHM
        from datetime import datetime, timezone, timedelta

        token_payload = {
            "sub": "99999",
            "role": "admin",
            "type": "access",
            "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        }
        token = jwt.encode(token_payload, get_jwt_secret(), algorithm=ALGORITHM)
        resp = await async_client.get(
            "/api/v1/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 401

    async def test_inactive_user_with_valid_token(self, async_client, inactive_user):
        from app.core.auth import create_access_token
        token = create_access_token(inactive_user.id, inactive_user.role)
        resp = await async_client.get(
            "/api/v1/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 401
