"""Integration test fixtures — real PostgreSQL, bypassed app lifespan."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys

import asyncpg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

TEST_DB_NAME = "stockpulse_test"
BASE_DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://stockpulse:stockpulse@localhost:5433/stockpulse",
)
TEST_DB_URL = BASE_DB_URL.rsplit("/", 1)[0] + f"/{TEST_DB_NAME}"
TEST_DB_URL_ASYNC = TEST_DB_URL.replace("postgresql://", "postgresql+asyncpg://")


# ---------------------------------------------------------------------------
# Session-scoped: create test database, run migrations
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def _test_db_setup():
    """Create the test database and run Alembic migrations (once per session)."""

    async def _create():
        conn = await asyncpg.connect(BASE_DB_URL)
        try:
            await conn.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{TEST_DB_NAME}' AND pid <> pg_backend_pid()
            """)
            await conn.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
            await conn.execute(f"CREATE DATABASE {TEST_DB_NAME}")
        finally:
            await conn.close()

    asyncio.run(_create())

    backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=backend_dir,
        env={**os.environ, "DATABASE_URL": TEST_DB_URL_ASYNC, "PYTHONPATH": backend_dir},
        check=True,
        capture_output=True,
    )

    yield

    async def _drop():
        conn = await asyncpg.connect(BASE_DB_URL)
        try:
            await conn.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{TEST_DB_NAME}' AND pid <> pg_backend_pid()
            """)
            await conn.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
        finally:
            await conn.close()

    asyncio.run(_drop())


# ---------------------------------------------------------------------------
# Test engine + session factory — NullPool to avoid cross-loop connection reuse
# ---------------------------------------------------------------------------

_test_engine = None
_test_sf = None


def _get_test_sf():
    """Lazily create a test-only SQLAlchemy engine with NullPool."""
    global _test_engine, _test_sf
    if _test_sf is None:
        import app.models  # noqa: F401 — register models
        _test_engine = create_async_engine(TEST_DB_URL_ASYNC, poolclass=NullPool)
        _test_sf = async_sessionmaker(_test_engine, class_=AsyncSession, expire_on_commit=False)
    return _test_sf


# ---------------------------------------------------------------------------
# Function-scoped: clean DB before each test
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def _clean_db(_test_db_setup):
    """Delete all rows from mutable tables before each test."""
    sf = _get_test_sf()
    async with sf() as session:
        await session.execute(text("DELETE FROM api_consumers"))
        await session.execute(text("DELETE FROM system_settings WHERE key != 'jwt_secret_key'"))
        await session.execute(text("DELETE FROM users"))
        await session.commit()
    yield


# ---------------------------------------------------------------------------
# async_client — HTTPX client backed by the FastAPI app
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture()
async def async_client(fake_redis, monkeypatch):
    """HTTPX AsyncClient with app's ORM pointing at test DB and stubbed infra."""
    import app.core.orm as orm_mod
    import app.core.database as db_mod
    import app.core.executor as exec_mod
    import app.core.api_keys as ak_mod

    # Override get_db to use our NullPool test sessions
    sf = _get_test_sf()

    async def _override_get_db():
        async with sf() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    # Stub asyncpg pool for health endpoint
    class _FakeConn:
        async def fetchval(self, q):
            return 1

    class _FakeAcquire:
        async def __aenter__(self):
            return _FakeConn()
        async def __aexit__(self, *a):
            pass

    class _FakePool:
        def acquire(self, timeout=5):
            return _FakeAcquire()

    monkeypatch.setattr(db_mod, "_pool", _FakePool())

    async def _healthy():
        return True
    monkeypatch.setattr(exec_mod, "check_executor_health", _healthy)
    monkeypatch.setattr(ak_mod, "get_api_key", lambda name: "stub-key")

    from app.core.orm import get_db
    from app.main import app
    app.dependency_overrides[get_db] = _override_get_db

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.pop(get_db, None)


# ---------------------------------------------------------------------------
# Auth helper fixtures
# ---------------------------------------------------------------------------

async def _create_user(email, password, role, display_name=None, is_active=True):
    """Insert a user into the test DB and return it."""
    from app.models.user import User
    from app.core.auth import hash_password

    user = User(
        email=email,
        password_hash=hash_password(password),
        display_name=display_name,
        role=role,
        is_active=is_active,
    )
    sf = _get_test_sf()
    async with sf() as session:
        session.add(user)
        await session.commit()
        await session.refresh(user)
    return user


@pytest_asyncio.fixture()
async def admin_user():
    return await _create_user("testadmin@stockpulse.dev", "TestPass123", "admin", "Test Admin")


@pytest.fixture()
def admin_headers(admin_user):
    from app.core.auth import create_access_token
    token = create_access_token(admin_user.id, admin_user.role)
    return {"Authorization": f"Bearer {token}"}


@pytest_asyncio.fixture()
async def regular_user():
    return await _create_user("user@stockpulse.dev", "UserPass123", "user", "Regular")


@pytest.fixture()
def user_headers(regular_user):
    from app.core.auth import create_access_token
    token = create_access_token(regular_user.id, regular_user.role)
    return {"Authorization": f"Bearer {token}"}


@pytest_asyncio.fixture()
async def inactive_user():
    return await _create_user("inactive@stockpulse.dev", "Pass123", "user", is_active=False)
