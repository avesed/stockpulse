"""Integration tests for /api/v1/admin/settings/* endpoints."""
from __future__ import annotations

import pytest


class TestListSettings:
    async def test_returns_list(self, async_client, admin_user, admin_headers):
        resp = await async_client.get("/api/v1/admin/settings", headers=admin_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    async def test_excludes_jwt_secret(self, async_client, admin_user, admin_headers):
        from tests.integration.conftest import _get_test_sf
        from sqlalchemy import text as sql_text
        sf = _get_test_sf()
        async with sf() as session:
            await session.execute(sql_text(
                "INSERT INTO system_settings (key, value) VALUES ('jwt_secret_key', 'secret') "
                "ON CONFLICT (key) DO NOTHING"
            ))
            await session.commit()

        resp = await async_client.get("/api/v1/admin/settings", headers=admin_headers)
        keys = [s["key"] for s in resp.json()]
        assert "jwt_secret_key" not in keys


class TestUpsertSetting:
    async def test_creates_new_setting(self, async_client, admin_user, admin_headers):
        resp = await async_client.put("/api/v1/admin/settings/test_key", json={
            "value": "test_value",
        }, headers=admin_headers)
        assert resp.status_code == 200
        assert resp.json()["key"] == "test_key"
        assert resp.json()["value"] == "test_value"

    async def test_updates_existing_setting(self, async_client, admin_user, admin_headers):
        await async_client.put("/api/v1/admin/settings/my_key", json={
            "value": "v1",
        }, headers=admin_headers)

        resp = await async_client.put("/api/v1/admin/settings/my_key", json={
            "value": "v2",
        }, headers=admin_headers)
        assert resp.status_code == 200
        assert resp.json()["value"] == "v2"

    async def test_protected_key_rejected(self, async_client, admin_user, admin_headers):
        resp = await async_client.put("/api/v1/admin/settings/jwt_secret_key", json={
            "value": "hacked",
        }, headers=admin_headers)
        assert resp.status_code == 403


class TestDeleteSetting:
    async def test_deletes_existing(self, async_client, admin_user, admin_headers):
        await async_client.put("/api/v1/admin/settings/del_me", json={
            "value": "temp",
        }, headers=admin_headers)

        resp = await async_client.delete(
            "/api/v1/admin/settings/del_me",
            headers=admin_headers,
        )
        assert resp.status_code == 204

    async def test_not_found(self, async_client, admin_user, admin_headers):
        resp = await async_client.delete(
            "/api/v1/admin/settings/nonexistent_key_xyz",
            headers=admin_headers,
        )
        assert resp.status_code == 404

    async def test_protected_key_rejected(self, async_client, admin_user, admin_headers):
        resp = await async_client.delete(
            "/api/v1/admin/settings/jwt_secret_key",
            headers=admin_headers,
        )
        assert resp.status_code == 403
