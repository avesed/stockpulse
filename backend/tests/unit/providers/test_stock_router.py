"""Tests for StockRouter — market routing, fallback chains, search dedup."""
from __future__ import annotations

import pytest

from app.providers.base import DataProvider
from app.services.stock_router import StockRouter


# ---------------------------------------------------------------------------
# Mock providers
# ---------------------------------------------------------------------------

class MockProvider(DataProvider):
    def __init__(self, name_val, markets, quote_data=None, raise_on_call=False):
        self._name = name_val
        self._markets = markets
        self._quote_data = quote_data
        self._raise_on_call = raise_on_call

    @property
    def name(self):
        return self._name

    @property
    def supported_markets(self):
        return self._markets

    async def get_quote(self, symbol, market):
        if self._raise_on_call:
            raise ConnectionError("provider down")
        return self._quote_data

    async def get_history(self, symbol, market, period, interval, start=None, end=None):
        if self._raise_on_call:
            raise ConnectionError("provider down")
        return {"bars": [], "source": self._name} if self._quote_data else None

    async def search(self, query, markets=None):
        if self._raise_on_call:
            raise ConnectionError("provider down")
        if self._quote_data:
            return [{"symbol": query.upper(), "name": f"From {self._name}", "market": "us"}]
        return []


def _make_router(yf_data=None, ak_data=None, yf_raise=False, ak_raise=False):
    yf = MockProvider("yfinance", {"us", "hk", "sh", "sz", "metal"}, yf_data, yf_raise)
    ak = MockProvider("akshare", {"hk", "sh", "sz"}, ak_data, ak_raise)
    return StockRouter(yfinance=yf, akshare=ak)


# ---------------------------------------------------------------------------
# Routing order
# ---------------------------------------------------------------------------

class TestGetProviders:
    def test_us_yfinance_first(self):
        router = _make_router()
        providers = router.get_providers("us")
        assert providers[0].name == "yfinance"

    def test_hk_akshare_first(self):
        router = _make_router()
        providers = router.get_providers("hk")
        assert providers[0].name == "akshare"
        assert providers[1].name == "yfinance"

    def test_sh_akshare_first(self):
        router = _make_router()
        providers = router.get_providers("sh")
        assert providers[0].name == "akshare"

    def test_metal_yfinance_only(self):
        router = _make_router()
        providers = router.get_providers("metal")
        assert len(providers) == 1
        assert providers[0].name == "yfinance"

    def test_unknown_market_falls_back_to_yfinance(self):
        router = _make_router()
        providers = router.get_providers("unknown")
        assert providers[0].name == "yfinance"


# ---------------------------------------------------------------------------
# Fallback chains
# ---------------------------------------------------------------------------

class TestFallback:
    async def test_primary_succeeds(self):
        router = _make_router(yf_data={"price": 150})
        result = await router.get_quote("AAPL", market="us")
        assert result is not None
        assert result["price"] == 150

    async def test_primary_none_fallback_succeeds(self):
        router = _make_router(yf_data=None, ak_data={"price": 50})
        result = await router.get_quote("0700.HK", market="hk")
        assert result is not None
        assert result["price"] == 50

    async def test_primary_raises_fallback_succeeds(self):
        router = _make_router(ak_data=None, ak_raise=True, yf_data={"price": 50})
        result = await router.get_quote("0700.HK", market="hk")
        assert result is not None
        assert result["price"] == 50

    async def test_all_providers_exhausted_returns_none(self):
        router = _make_router(yf_data=None, ak_data=None)
        result = await router.get_quote("0700.HK", market="hk")
        assert result is None

    async def test_auto_market_detection(self):
        router = _make_router(yf_data={"price": 100})
        result = await router.get_quote("AAPL")
        assert result is not None


# ---------------------------------------------------------------------------
# get_provider_by_name
# ---------------------------------------------------------------------------

class TestGetProviderByName:
    def test_existing_provider(self):
        router = _make_router()
        assert router.get_provider_by_name("yfinance") is not None
        assert router.get_provider_by_name("akshare") is not None

    def test_nonexistent_provider(self):
        router = _make_router()
        assert router.get_provider_by_name("nonexistent") is None


# ---------------------------------------------------------------------------
# History, info, financials routing
# ---------------------------------------------------------------------------

class TestOtherMethods:
    async def test_get_history_routes_correctly(self):
        router = _make_router(yf_data={"bars": []})
        result = await router.get_history("AAPL", period="1y", interval="1d", market="us")
        assert result is not None

    async def test_get_info_routes_correctly(self):
        yf = MockProvider("yfinance", {"us"}, {"price": 1})
        ak = MockProvider("akshare", {"hk"})

        class InfoYF(MockProvider):
            async def get_info(self, symbol, market):
                return {"name": "Apple Inc", "source": "yfinance"}

        yf_info = InfoYF("yfinance", {"us"}, {"price": 1})
        router = StockRouter(yfinance=yf_info, akshare=ak)
        result = await router.get_info("AAPL", market="us")
        assert result is not None
        assert result["name"] == "Apple Inc"

    async def test_get_financials_all_none(self):
        router = _make_router()
        result = await router.get_financials("AAPL", market="us")
        assert result is None
