"""Tests for DataProvider ABC — default methods and contract."""
from __future__ import annotations

import pytest

from app.providers.base import DataProvider


class StubProvider(DataProvider):
    """Minimal concrete subclass for testing the ABC."""

    @property
    def name(self) -> str:
        return "stub"

    @property
    def supported_markets(self):
        return {"us", "hk"}

    async def get_quote(self, symbol, market):
        return {"symbol": symbol, "price": 100}

    async def get_history(self, symbol, market, period, interval, start=None, end=None):
        return {"bars": []}

    async def search(self, query, markets=None):
        return [{"symbol": "TEST"}]


class TestABCContract:
    def test_incomplete_subclass_raises_typeerror(self):
        class IncompleteProvider(DataProvider):
            @property
            def name(self):
                return "incomplete"

        with pytest.raises(TypeError):
            IncompleteProvider()


class TestSupportsMarket:
    def test_supported(self):
        p = StubProvider()
        assert p.supports_market("us") is True
        assert p.supports_market("hk") is True

    def test_unsupported(self):
        p = StubProvider()
        assert p.supports_market("sz") is False
        assert p.supports_market("metal") is False

    def test_case_insensitive(self):
        p = StubProvider()
        assert p.supports_market("US") is True


class TestDefaultMethods:
    async def test_get_info_returns_none(self):
        p = StubProvider()
        assert await p.get_info("AAPL", "us") is None

    async def test_get_financials_returns_none(self):
        p = StubProvider()
        assert await p.get_financials("AAPL", "us") is None

    async def test_get_analyst_ratings_returns_none(self):
        p = StubProvider()
        assert await p.get_analyst_ratings("AAPL") is None

    async def test_get_news_returns_empty_list(self):
        p = StubProvider()
        assert await p.get_news() == []

    async def test_is_available_returns_true(self):
        assert StubProvider.is_available() is True
