"""Integration tests for /api/v1/admin/consumers/* endpoints."""
from __future__ import annotations

import pytest


class TestListConsumers:
    async def test_admin_gets_list(self, async_client, admin_user, admin_headers):
        resp = await async_client.get("/api/v1/admin/consumers", headers=admin_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    async def test_non_admin_forbidden(self, async_client, regular_user, user_headers):
        resp = await async_client.get("/api/v1/admin/consumers", headers=user_headers)
        assert resp.status_code == 403

    async def test_no_auth_unauthorized(self, async_client):
        resp = await async_client.get("/api/v1/admin/consumers")
        assert resp.status_code == 401


class TestCreateConsumer:
    async def test_creates_and_returns_raw_key(self, async_client, admin_user, admin_headers):
        resp = await async_client.post("/api/v1/admin/consumers", json={
            "name": "test-consumer",
            "description": "for testing",
            "rateLimit": 50,
        }, headers=admin_headers)
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "test-consumer"
        assert "rawApiKey" in data
        assert len(data["rawApiKey"]) > 0
        assert data["isActive"] is True
        assert data["rateLimit"] == 50

    async def test_duplicate_name_conflict(self, async_client, admin_user, admin_headers):
        await async_client.post("/api/v1/admin/consumers", json={
            "name": "dup-consumer",
        }, headers=admin_headers)

        resp = await async_client.post("/api/v1/admin/consumers", json={
            "name": "dup-consumer",
        }, headers=admin_headers)
        assert resp.status_code == 409

    async def test_non_admin_forbidden(self, async_client, regular_user, user_headers):
        resp = await async_client.post("/api/v1/admin/consumers", json={
            "name": "sneaky",
        }, headers=user_headers)
        assert resp.status_code == 403


class TestDeactivateConsumer:
    async def test_soft_delete(self, async_client, admin_user, admin_headers):
        create = await async_client.post("/api/v1/admin/consumers", json={
            "name": "to-deactivate",
        }, headers=admin_headers)
        consumer_id = create.json()["id"]

        resp = await async_client.delete(
            f"/api/v1/admin/consumers/{consumer_id}",
            headers=admin_headers,
        )
        assert resp.status_code == 204

    async def test_not_found(self, async_client, admin_user, admin_headers):
        resp = await async_client.delete(
            "/api/v1/admin/consumers/00000000-0000-0000-0000-000000000000",
            headers=admin_headers,
        )
        assert resp.status_code == 404


class TestPermanentDelete:
    async def test_hard_delete(self, async_client, admin_user, admin_headers):
        create = await async_client.post("/api/v1/admin/consumers", json={
            "name": "to-delete-perm",
        }, headers=admin_headers)
        consumer_id = create.json()["id"]

        resp = await async_client.delete(
            f"/api/v1/admin/consumers/{consumer_id}/permanent",
            headers=admin_headers,
        )
        assert resp.status_code == 204

        # Verify it's gone
        usage = await async_client.get(
            f"/api/v1/admin/consumers/{consumer_id}/usage",
            headers=admin_headers,
        )
        assert usage.status_code == 404


class TestConsumerUsage:
    async def test_returns_usage(self, async_client, admin_user, admin_headers):
        create = await async_client.post("/api/v1/admin/consumers", json={
            "name": "usage-test",
        }, headers=admin_headers)
        consumer_id = create.json()["id"]

        resp = await async_client.get(
            f"/api/v1/admin/consumers/{consumer_id}/usage",
            headers=admin_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "usage-test"
        assert data["isActive"] is True

    async def test_not_found(self, async_client, admin_user, admin_headers):
        resp = await async_client.get(
            "/api/v1/admin/consumers/00000000-0000-0000-0000-000000000000/usage",
            headers=admin_headers,
        )
        assert resp.status_code == 404
