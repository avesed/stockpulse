"""Root conftest — fixtures shared across all test types."""
from __future__ import annotations

import pytest
import fakeredis.aioredis

TEST_JWT_SECRET = "test-secret-key-that-is-at-least-32-characters-long"


@pytest.fixture(autouse=True)
def mock_jwt_secret(monkeypatch):
    """Inject a fixed JWT secret so tests never need bootstrap_jwt_secret()."""
    import app.core.secrets as _mod
    monkeypatch.setattr(_mod, "_jwt_secret", TEST_JWT_SECRET)


@pytest.fixture()
def fake_redis(monkeypatch):
    """Replace get_redis() with an in-memory fakeredis instance."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)

    async def _get():
        return client

    import app.core.redis as _mod
    monkeypatch.setattr(_mod, "get_redis", _get)
    monkeypatch.setattr(_mod, "_redis_client", client)
    return client


def create_test_token(user_id: int = 1, role: str = "admin") -> str:
    """Create a valid JWT access token for testing."""
    from app.core.auth import create_access_token
    return create_access_token(user_id, role)
