"""Integration tests for /health endpoint."""
from __future__ import annotations

import pytest


class TestHealth:
    async def test_returns_200(self, async_client):
        resp = await async_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["service"] == "stockpulse"

    async def test_includes_redis_status(self, async_client):
        resp = await async_client.get("/health")
        data = resp.json()
        assert "redis" in data

    async def test_includes_database_status(self, async_client):
        resp = await async_client.get("/health")
        data = resp.json()
        assert "database" in data

    async def test_includes_executor_status(self, async_client):
        resp = await async_client.get("/health")
        data = resp.json()
        assert data["executor"] == "ok"

    async def test_includes_providers_status(self, async_client):
        resp = await async_client.get("/health")
        data = resp.json()
        assert "providers" in data
        assert data["providers"]["yfinance"] == "ok"
