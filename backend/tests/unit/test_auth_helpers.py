"""Tests for app.core.auth — password hashing, JWT creation/decoding, refresh jti."""
from __future__ import annotations

import time

import pytest
import redis.exceptions
from fastapi import HTTPException
from jose import jwt

from app.core.auth import (
    ALGORITHM,
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
    hash_password,
    verify_password,
    claim_refresh_jti,
)
from app.core.secrets import get_jwt_secret


# --- Password helpers ---

class TestPasswordHashing:
    def test_verify_correct_password(self):
        hashed = hash_password("my-password")
        assert verify_password("my-password", hashed) is True

    def test_reject_wrong_password(self):
        hashed = hash_password("my-password")
        assert verify_password("wrong-password", hashed) is False

    def test_truncates_at_72_chars(self):
        long_pw = "a" * 100
        hashed = hash_password(long_pw)
        assert verify_password("a" * 72, hashed) is True
        assert verify_password("a" * 71, hashed) is False


# --- JWT creation ---

class TestAccessToken:
    def test_decodes_with_expected_claims(self):
        token = create_access_token(user_id=42, role="admin")
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[ALGORITHM])
        assert payload["sub"] == "42"
        assert payload["role"] == "admin"
        assert payload["type"] == "access"
        assert "exp" in payload

    def test_different_users_produce_different_tokens(self):
        t1 = create_access_token(user_id=1, role="user")
        t2 = create_access_token(user_id=2, role="admin")
        assert t1 != t2


class TestRefreshToken:
    def test_decodes_with_expected_claims(self):
        token = create_refresh_token(user_id=7)
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[ALGORITHM])
        assert payload["sub"] == "7"
        assert payload["type"] == "refresh"
        assert "jti" in payload
        assert "exp" in payload

    def test_jti_is_unique(self):
        t1 = create_refresh_token(user_id=1)
        t2 = create_refresh_token(user_id=1)
        p1 = jwt.decode(t1, get_jwt_secret(), algorithms=[ALGORITHM])
        p2 = jwt.decode(t2, get_jwt_secret(), algorithms=[ALGORITHM])
        assert p1["jti"] != p2["jti"]


# --- decode_refresh_token ---

class TestDecodeRefreshToken:
    def test_valid_token_returns_tuple(self):
        token = create_refresh_token(user_id=5)
        user_id, jti, exp_ts = decode_refresh_token(token)
        assert user_id == 5
        assert isinstance(jti, str) and len(jti) > 0
        assert exp_ts > int(time.time())

    def test_access_token_rejected(self):
        token = create_access_token(user_id=1, role="user")
        with pytest.raises(HTTPException) as exc_info:
            decode_refresh_token(token)
        assert exc_info.value.status_code == 401

    def test_garbage_token_rejected(self):
        with pytest.raises(HTTPException) as exc_info:
            decode_refresh_token("not.a.token")
        assert exc_info.value.status_code == 401


# --- claim_refresh_jti ---

class TestClaimRefreshJti:
    @pytest.fixture(autouse=True)
    def _redis(self, fake_redis):
        pass

    async def test_first_claim_succeeds(self):
        future_ts = int(time.time()) + 3600
        result = await claim_refresh_jti("jti-abc", future_ts)
        assert result is True

    async def test_second_claim_fails(self):
        future_ts = int(time.time()) + 3600
        await claim_refresh_jti("jti-dup", future_ts)
        result = await claim_refresh_jti("jti-dup", future_ts)
        assert result is False

    async def test_redis_outage_raises_503(self, monkeypatch):
        async def _broken():
            raise redis.exceptions.ConnectionError("down")

        import app.core.auth as _auth_mod
        monkeypatch.setattr(_auth_mod, "get_redis", _broken)

        with pytest.raises(HTTPException) as exc_info:
            await claim_refresh_jti("jti-x", int(time.time()) + 3600)
        assert exc_info.value.status_code == 503
