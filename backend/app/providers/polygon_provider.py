"""Polygon.io data provider for US stocks.

Polygon.io provides comprehensive market data including:
- Real-time and delayed stock quotes
- Historical aggregates (OHLCV bars)
- Ticker search and details
- Company information (branding, SIC, locale)

API Documentation: https://polygon.io/docs
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from app.core.api_keys import get_api_key
from app.core.redis import cache_get, cache_set, jittered_ttl
from app.core.executor import run_in_executor
from app.providers.base import DataProvider
from app.providers.constants import US

logger = logging.getLogger(__name__)

# Cache TTL configurations (base_seconds, jitter_seconds)
CACHE_TTL = {
    "quote": (60, 30),
    "history": (300, 60),
    "info": (86400, 3600),
    "search": (3600, 300),
}

# Period string -> approximate days
_PERIOD_DAYS = {
    "1d": 1,
    "5d": 5,
    "1mo": 30,
    "3mo": 90,
    "6mo": 180,
    "1y": 365,
    "2y": 730,
    "5y": 1825,
    "max": 7300,
}

# Interval -> Polygon timespan + multiplier
_INTERVAL_MAP = {
    "1m": ("minute", 1),
    "5m": ("minute", 5),
    "15m": ("minute", 15),
    "30m": ("minute", 30),
    "1h": ("hour", 1),
    "1d": ("day", 1),
    "1wk": ("week", 1),
    "1mo": ("month", 1),
}


def _ttl(data_type: str) -> int:
    base, jitter = CACHE_TTL.get(data_type, (3600, 300))
    return jittered_ttl(base, jitter)


class PolygonProvider(DataProvider):
    """Polygon.io data provider for US stocks.

    Requires POLYGON_API_KEY setting.
    """

    _client = None

    @property
    def name(self) -> str:
        return "polygon"

    @property
    def supported_markets(self) -> Set[str]:
        return {US}

    @classmethod
    def is_available(cls) -> bool:
        return bool(get_api_key("polygon"))

    def _get_client(self):
        if PolygonProvider._client is None and self.is_available():
            try:
                from polygon import RESTClient

                PolygonProvider._client = RESTClient(api_key=get_api_key("polygon"))
            except ImportError:
                logger.warning(
                    "polygon-api-client package not installed. "
                    "Run: pip install polygon-api-client"
                )
                return None
            except Exception as e:
                logger.error("Failed to initialize Polygon client: %s", e)
                return None
        return PolygonProvider._client

    async def _cached_or_fetch(
        self,
        data_type: str,
        identifier: str,
        fetch_func,
    ) -> Optional[Dict[str, Any]]:
        cache_key = f"polygon:{data_type}:{identifier}"
        cached = await cache_get(cache_key)
        if cached is not None:
            return cached

        try:
            data = await fetch_func()
            if data:
                await cache_set(cache_key, data, ttl=_ttl(data_type))
            return data
        except Exception as e:
            logger.error("Fetch error for %s/%s: %s", data_type, identifier, e)
            return None

    # === Core Methods ===

    async def get_quote(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        if not self.is_available() or market != US:
            return None

        try:
            client = self._get_client()
            if not client:
                return None

            def fetch():
                try:
                    # Previous close for change calculation
                    prev = client.get_previous_close_agg(symbol)
                    if not prev or not prev:
                        return None
                    results = list(prev)
                    if not results:
                        return None
                    return results[0]
                except Exception as e:
                    logger.warning("Polygon quote error: %s", e)
                    return None

            agg = await run_in_executor(fetch)
            if not agg:
                return None

            price = float(agg.close) if agg.close else 0
            prev_close = float(agg.open) if agg.open else None
            change = price - prev_close if prev_close else 0
            change_pct = (change / prev_close * 100) if prev_close else 0

            return {
                "symbol": symbol,
                "name": None,
                "price": price,
                "change": round(change, 4),
                "change_percent": round(change_pct, 2),
                "volume": int(agg.volume) if agg.volume else 0,
                "market_cap": None,
                "high": float(agg.high) if agg.high else None,
                "low": float(agg.low) if agg.low else None,
                "open": float(agg.open) if agg.open else None,
                "prev_close": prev_close,
                "timestamp": datetime.utcnow().isoformat(),
                "market": market,
                "currency": "USD",
                "source": "polygon",
            }
        except Exception as e:
            logger.error("Polygon quote error for %s: %s", symbol, e)
            return None

    async def get_history(
        self,
        symbol: str,
        market: str,
        period: str,
        interval: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self.is_available() or market != US:
            return None

        mapping = _INTERVAL_MAP.get(interval)
        if not mapping:
            logger.debug("Polygon doesn't support interval: %s", interval)
            return None

        timespan, multiplier = mapping

        try:
            client = self._get_client()
            if not client:
                return None

            end_date = datetime.now()
            if end:
                end_date = datetime.fromisoformat(end)
            if start:
                start_date = datetime.fromisoformat(start)
            else:
                days = _PERIOD_DAYS.get(period, 365)
                start_date = end_date - timedelta(days=days)

            from_str = start_date.strftime("%Y-%m-%d")
            to_str = end_date.strftime("%Y-%m-%d")

            def fetch():
                try:
                    aggs = client.get_aggs(
                        ticker=symbol,
                        multiplier=multiplier,
                        timespan=timespan,
                        from_=from_str,
                        to=to_str,
                        limit=50000,
                    )
                    return list(aggs) if aggs else None
                except Exception as e:
                    logger.warning("Polygon history error: %s", e)
                    return None

            data = await run_in_executor(fetch)
            if not data:
                return None

            bars = []
            for agg in data:
                ts = agg.timestamp
                if ts:
                    # Polygon returns ms timestamp
                    date_val = datetime.utcfromtimestamp(ts / 1000)
                else:
                    continue

                bars.append({
                    "date": date_val.isoformat(),
                    "open": round(float(agg.open), 4) if agg.open else 0,
                    "high": round(float(agg.high), 4) if agg.high else 0,
                    "low": round(float(agg.low), 4) if agg.low else 0,
                    "close": round(float(agg.close), 4) if agg.close else 0,
                    "volume": int(agg.volume) if agg.volume else 0,
                })

            return {
                "symbol": symbol,
                "interval": interval,
                "bars": bars,
                "market": market,
                "source": "polygon",
            }
        except Exception as e:
            logger.error("Polygon history error for %s: %s", symbol, e)
            return None

    async def search(
        self, query: str, markets: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        if not self.is_available():
            return []

        try:
            client = self._get_client()
            if not client:
                return []

            def fetch():
                try:
                    results = client.list_tickers(
                        search=query,
                        market="stocks",
                        active=True,
                        limit=20,
                    )
                    return list(results) if results else []
                except Exception as e:
                    logger.warning("Polygon search error: %s", e)
                    return []

            tickers = await run_in_executor(fetch)
            return [
                {
                    "symbol": t.ticker if hasattr(t, "ticker") else str(t),
                    "name": getattr(t, "name", ""),
                    "exchange": getattr(t, "primary_exchange", ""),
                    "market": US,
                }
                for t in tickers
            ]
        except Exception as e:
            logger.error("Polygon search error for %s: %s", query, e)
            return []

    # === Optional Methods ===

    async def get_info(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        if not self.is_available() or market != US:
            return None

        async def fetch():
            client = self._get_client()
            if not client:
                return None

            def _fetch_sync():
                try:
                    details = client.get_ticker_details(symbol)
                    if not details:
                        return None
                    return {
                        "symbol": symbol,
                        "name": getattr(details, "name", ""),
                        "description": getattr(details, "description", None),
                        "sector": getattr(details, "sic_description", None),
                        "industry": None,
                        "website": getattr(details, "homepage_url", None),
                        "employees": getattr(details, "total_employees", None),
                        "market_cap": getattr(details, "market_cap", None),
                        "currency": getattr(details, "currency_name", "USD"),
                        "exchange": getattr(details, "primary_exchange", ""),
                        "market": market,
                        "source": "polygon",
                    }
                except Exception as e:
                    logger.warning("Polygon info error: %s", e)
                    return None

            return await run_in_executor(_fetch_sync)

        return await self._cached_or_fetch("info", symbol, fetch)
