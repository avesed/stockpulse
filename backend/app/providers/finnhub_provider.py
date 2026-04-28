"""Finnhub data provider for US stocks.

Finnhub provides:
- Real-time quotes (current, open, high, low, previous close)
- Stock candles (OHLCV history)
- Company profile (name, industry, market cap, logo)
- Symbol search

API Documentation: https://finnhub.io/docs/api
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

CACHE_TTL = {
    "quote": (30, 15),
    "history": (300, 60),
    "info": (86400, 3600),
    "search": (3600, 300),
}

_PERIOD_DAYS = {
    "1d": 1, "5d": 5, "1mo": 30, "3mo": 90,
    "6mo": 180, "1y": 365, "2y": 730, "5y": 1825, "max": 7300,
}

# Finnhub resolution: 1, 5, 15, 30, 60, D, W, M
_RESOLUTION_MAP = {
    "1m": "1", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "1d": "D", "1wk": "W", "1mo": "M",
}


def _ttl(data_type: str) -> int:
    base, jitter = CACHE_TTL.get(data_type, (3600, 300))
    return jittered_ttl(base, jitter)


class FinnhubProvider(DataProvider):
    """Finnhub data provider for US stocks.

    Requires FINNHUB_API_KEY setting.
    """

    _client = None
    _client_key: str | None = None

    @property
    def name(self) -> str:
        return "finnhub"

    @property
    def supported_markets(self) -> Set[str]:
        return {US}

    @classmethod
    def is_available(cls) -> bool:
        return bool(get_api_key("finnhub"))

    def _get_client(self):
        current_key = get_api_key("finnhub")
        if not current_key:
            return None

        if FinnhubProvider._client is not None and FinnhubProvider._client_key != current_key:
            logger.info("Finnhub API key changed, rebuilding client")
            FinnhubProvider._client = None

        if FinnhubProvider._client is None:
            try:
                import finnhub
                FinnhubProvider._client = finnhub.Client(api_key=current_key)
                FinnhubProvider._client_key = current_key
                logger.info("Finnhub client initialized")
            except ImportError:
                logger.warning("finnhub-python package not installed")
                return None
            except Exception as e:
                logger.error("Failed to initialize Finnhub client: %s", e)
                return None
        return FinnhubProvider._client

    async def get_quote(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        if not self.is_available() or market != US:
            return None

        client = self._get_client()
        if not client:
            return None

        try:
            def fetch():
                try:
                    return client.quote(symbol)
                except Exception as e:
                    logger.warning("Finnhub quote error for %s: %s", symbol, e)
                    return None

            q = await run_in_executor(fetch)
            if not q or q.get("c") is None or q.get("c") == 0:
                return None

            price = float(q["c"])
            prev_close = float(q.get("pc", 0))
            change = float(q.get("d", 0))
            change_pct = float(q.get("dp", 0))

            return {
                "symbol": symbol,
                "name": None,
                "price": price,
                "change": round(change, 4),
                "change_percent": round(change_pct, 2),
                "volume": None,
                "market_cap": None,
                "high": float(q.get("h", 0)) or None,
                "low": float(q.get("l", 0)) or None,
                "open": float(q.get("o", 0)) or None,
                "prev_close": prev_close or None,
                "timestamp": datetime.utcnow().isoformat(),
                "market": market,
                "currency": "USD",
                "source": "finnhub",
            }
        except Exception as e:
            logger.error("Finnhub quote error for %s: %s", symbol, e)
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

        resolution = _RESOLUTION_MAP.get(interval)
        if not resolution:
            logger.debug("Finnhub doesn't support interval: %s", interval)
            return None

        client = self._get_client()
        if not client:
            return None

        try:
            end_dt = datetime.now()
            if end:
                end_dt = datetime.fromisoformat(end)
            if start:
                start_dt = datetime.fromisoformat(start)
            else:
                days = _PERIOD_DAYS.get(period, 365)
                start_dt = end_dt - timedelta(days=days)

            from_ts = int(start_dt.timestamp())
            to_ts = int(end_dt.timestamp())

            def fetch():
                try:
                    return client.stock_candles(symbol, resolution, from_ts, to_ts)
                except Exception as e:
                    logger.warning("Finnhub candles error for %s: %s", symbol, e)
                    return None

            data = await run_in_executor(fetch)
            if not data or data.get("s") != "ok":
                return None

            bars = []
            timestamps = data.get("t", [])
            opens = data.get("o", [])
            highs = data.get("h", [])
            lows = data.get("l", [])
            closes = data.get("c", [])
            volumes = data.get("v", [])

            for i in range(len(timestamps)):
                date_val = datetime.utcfromtimestamp(timestamps[i])
                bars.append({
                    "date": date_val.isoformat(),
                    "open": round(float(opens[i]), 4),
                    "high": round(float(highs[i]), 4),
                    "low": round(float(lows[i]), 4),
                    "close": round(float(closes[i]), 4),
                    "volume": int(volumes[i]) if i < len(volumes) else 0,
                })

            return {
                "symbol": symbol,
                "interval": interval,
                "bars": bars,
                "market": market,
                "source": "finnhub",
            }
        except Exception as e:
            logger.error("Finnhub history error for %s: %s", symbol, e)
            return None

    async def search(
        self, query: str, markets: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        if not self.is_available():
            return []

        client = self._get_client()
        if not client:
            return []

        try:
            def fetch():
                try:
                    return client.symbol_lookup(query)
                except Exception as e:
                    logger.warning("Finnhub search error for %s: %s", query, e)
                    return None

            data = await run_in_executor(fetch)
            if not data or not data.get("result"):
                return []

            return [
                {
                    "symbol": r.get("symbol", ""),
                    "name": r.get("description", ""),
                    "exchange": r.get("displaySymbol", ""),
                    "market": US,
                }
                for r in data["result"][:20]
            ]
        except Exception as e:
            logger.error("Finnhub search error for %s: %s", query, e)
            return []

    async def get_info(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        if not self.is_available() or market != US:
            return None

        client = self._get_client()
        if not client:
            return None

        try:
            def fetch():
                try:
                    return client.company_profile2(symbol=symbol)
                except Exception as e:
                    logger.warning("Finnhub profile error for %s: %s", symbol, e)
                    return None

            data = await run_in_executor(fetch)
            if not data or not data.get("name"):
                return None

            return {
                "symbol": symbol,
                "name": data.get("name", ""),
                "description": None,
                "sector": data.get("finnhubIndustry"),
                "industry": data.get("finnhubIndustry"),
                "website": data.get("weburl"),
                "employees": None,
                "market_cap": (
                    data["marketCapitalization"] * 1_000_000
                    if data.get("marketCapitalization") else None
                ),
                "currency": data.get("currency", "USD"),
                "exchange": data.get("exchange", ""),
                "market": market,
                "source": "finnhub",
            }
        except Exception as e:
            logger.error("Finnhub info error for %s: %s", symbol, e)
            return None
