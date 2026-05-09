"""Tests for app.config — Settings defaults and utilities."""
from __future__ import annotations

import pytest

from app.config import Settings, get_settings


class TestSettingsDefaults:
    def test_default_port(self):
        s = Settings(DATABASE_URL="x", REDIS_URL="x")
        assert s.PORT == 8010

    def test_default_jwt_expiry(self):
        s = Settings(DATABASE_URL="x", REDIS_URL="x")
        assert s.JWT_ACCESS_TOKEN_EXPIRE_MINUTES == 30
        assert s.JWT_REFRESH_TOKEN_EXPIRE_DAYS == 7

    def test_default_pool_sizes(self):
        s = Settings(DATABASE_URL="x", REDIS_URL="x")
        assert s.EXECUTOR_MAX_WORKERS == 20
        assert s.EXECUTOR_BACKGROUND_WORKERS == 10
        assert s.EXECUTOR_PROFILE_WORKERS == 5


class TestCorsOriginList:
    def test_parses_comma_separated(self):
        s = Settings(
            DATABASE_URL="x", REDIS_URL="x",
            CORS_ORIGINS="http://a.com,http://b.com",
        )
        assert s.cors_origin_list == ["http://a.com", "http://b.com"]

    def test_strips_whitespace(self):
        s = Settings(
            DATABASE_URL="x", REDIS_URL="x",
            CORS_ORIGINS="  http://a.com , http://b.com  ",
        )
        assert s.cors_origin_list == ["http://a.com", "http://b.com"]

    def test_filters_empty_segments(self):
        s = Settings(
            DATABASE_URL="x", REDIS_URL="x",
            CORS_ORIGINS="http://a.com,,http://b.com,",
        )
        assert s.cors_origin_list == ["http://a.com", "http://b.com"]

    def test_single_origin(self):
        s = Settings(
            DATABASE_URL="x", REDIS_URL="x",
            CORS_ORIGINS="http://localhost:3000",
        )
        assert s.cors_origin_list == ["http://localhost:3000"]


class TestGetSettingsCached:
    def test_returns_same_instance(self):
        a = get_settings()
        b = get_settings()
        assert a is b
