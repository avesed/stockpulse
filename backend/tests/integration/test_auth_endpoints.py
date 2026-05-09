"""Integration tests for /api/v1/auth/* endpoints."""
from __future__ import annotations

import pytest

ADMIN_EMAIL = "testadmin@stockpulse.dev"
ADMIN_PASSWORD = "TestPass123"


class TestLogin:
    async def test_valid_credentials(self, async_client, admin_user):
        resp = await async_client.post("/api/v1/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "accessToken" in data
        assert "refreshToken" in data
        assert data["tokenType"] == "bearer"

    async def test_wrong_password(self, async_client, admin_user):
        resp = await async_client.post("/api/v1/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": "WrongPassword",
        })
        assert resp.status_code == 401

    async def test_nonexistent_user(self, async_client):
        resp = await async_client.post("/api/v1/auth/login", json={
            "email": "nobody@stockpulse.dev",
            "password": "whatever",
        })
        assert resp.status_code == 401

    async def test_inactive_user(self, async_client, inactive_user):
        resp = await async_client.post("/api/v1/auth/login", json={
            "email": "inactive@stockpulse.dev",
            "password": "Pass123",
        })
        assert resp.status_code == 403

    async def test_username_without_at_appends_domain(self, async_client, admin_user):
        resp = await async_client.post("/api/v1/auth/login", json={
            "email": "testadmin",
            "password": ADMIN_PASSWORD,
        })
        assert resp.status_code == 200


class TestRefresh:
    async def test_valid_refresh(self, async_client, admin_user):
        login = await async_client.post("/api/v1/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
        })
        refresh_token = login.json()["refreshToken"]

        resp = await async_client.post("/api/v1/auth/refresh", json={
            "refreshToken": refresh_token,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "accessToken" in data
        assert "refreshToken" in data

    async def test_replay_detection(self, async_client, admin_user):
        login = await async_client.post("/api/v1/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
        })
        refresh_token = login.json()["refreshToken"]

        await async_client.post("/api/v1/auth/refresh", json={
            "refreshToken": refresh_token,
        })

        resp = await async_client.post("/api/v1/auth/refresh", json={
            "refreshToken": refresh_token,
        })
        assert resp.status_code == 401

    async def test_invalid_token(self, async_client):
        resp = await async_client.post("/api/v1/auth/refresh", json={
            "refreshToken": "garbage.token.value",
        })
        assert resp.status_code == 401


class TestLogout:
    async def test_with_refresh_token(self, async_client, admin_user):
        login = await async_client.post("/api/v1/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
        })
        refresh_token = login.json()["refreshToken"]

        resp = await async_client.post("/api/v1/auth/logout", json={
            "refreshToken": refresh_token,
        })
        assert resp.status_code == 204

    async def test_without_body(self, async_client):
        resp = await async_client.post("/api/v1/auth/logout")
        assert resp.status_code == 204

    async def test_with_invalid_token(self, async_client):
        resp = await async_client.post("/api/v1/auth/logout", json={
            "refreshToken": "invalid.token",
        })
        assert resp.status_code == 204


class TestMe:
    async def test_authenticated(self, async_client, admin_user, admin_headers):
        resp = await async_client.get("/api/v1/auth/me", headers=admin_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == ADMIN_EMAIL
        assert data["role"] == "admin"

    async def test_no_auth(self, async_client):
        resp = await async_client.get("/api/v1/auth/me")
        assert resp.status_code == 401


class TestChangePassword:
    async def test_success(self, async_client, admin_user, admin_headers):
        resp = await async_client.post("/api/v1/auth/change-password", json={
            "currentPassword": ADMIN_PASSWORD,
            "newPassword": "NewPass456",
        }, headers=admin_headers)
        assert resp.status_code == 204

    async def test_wrong_current_password(self, async_client, admin_user, admin_headers):
        resp = await async_client.post("/api/v1/auth/change-password", json={
            "currentPassword": "WrongOldPass",
            "newPassword": "NewPass456",
        }, headers=admin_headers)
        assert resp.status_code == 400

    async def test_new_password_too_short(self, async_client, admin_user, admin_headers):
        resp = await async_client.post("/api/v1/auth/change-password", json={
            "currentPassword": ADMIN_PASSWORD,
            "newPassword": "ab",
        }, headers=admin_headers)
        assert resp.status_code == 400
