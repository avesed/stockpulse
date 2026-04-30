"""Tiingo data provider for US stocks.

Migrated from backend/app/services/providers/tiingo.py.
Uses the shared executor and cache helpers instead of per-provider ThreadPool/Redis.

Tiingo provides high-quality financial data including:
- End-of-Day (EOD) stock prices
- Real-time IEX quotes
- Fundamentals (daily metrics, quarterly statements)
- News (handled separately in news_service)

API Documentation: https://www.tiingo.com/documentation/
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from app.config import get_settings
from app.core.redis import cache_get, cache_set, jittered_ttl
from app.core.executor import run_in_executor
from app.providers.base import DataProvider
from app.providers.constants import US

logger = logging.getLogger(__name__)

# Cache TTL configurations (base_seconds, jitter_seconds)
CACHE_TTL = {
    "quote": (60, 30),  # 1min + rand(30s) for real-time quotes
    "history": (300, 60),  # 5min + rand(1min)
    "info": (86400, 3600),  # 24h + rand(1h)
    "financials": (86400, 3600),  # 24h + rand(1h)
    "fundamentals_daily": (3600, 600),  # 1h + rand(10min)
    "fundamentals_statements": (86400, 3600),  # 24h + rand(1h)
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
    "max": 7300,  # 20 years
}


def _ttl(data_type: str) -> int:
    """Get jittered TTL for a data type."""
    base, jitter = CACHE_TTL.get(data_type, (3600, 300))
    return jittered_ttl(base, jitter)


class TiingoProvider(DataProvider):
    """Tiingo data provider for US stocks.

    Primary use cases:
    - Alternative/fallback source for US stock data
    - High-quality EOD historical data
    - Fundamentals data (PE, EPS, market cap, etc.)

    Requires TIINGO_API_KEY setting.

    API Endpoints:
    - /tiingo/daily/{ticker} - Metadata
    - /tiingo/daily/{ticker}/prices - Historical prices
    - /iex/{ticker} - Real-time IEX quotes
    - /tiingo/fundamentals/{ticker}/daily - Daily fundamentals
    - /tiingo/fundamentals/{ticker}/statements - Quarterly statements
    """

    _api_key: Optional[str] = None
    _client = None

    def __init__(self):
        if TiingoProvider._api_key is None:
            settings = get_settings()
            TiingoProvider._api_key = settings.TIINGO_API_KEY

    @property
    def name(self) -> str:
        return "tiingo"

    @property
    def supported_markets(self) -> Set[str]:
        return {US}

    @classmethod
    def is_available(cls) -> bool:
        """Check if Tiingo API key is available."""
        if cls._api_key is None:
            settings = get_settings()
            cls._api_key = settings.TIINGO_API_KEY
        return bool(cls._api_key)

    def _get_client(self):
        """Get or create Tiingo client."""
        if TiingoProvider._client is None and self.is_available():
            try:
                from tiingo import TiingoClient

                config = {"api_key": self._api_key, "session": True}
                TiingoProvider._client = TiingoClient(config)
            except ImportError:
                logger.warning(
                    "tiingo package not installed. Run: pip install tiingo"
                )
                return None
            except Exception as e:
                logger.error("Failed to initialize Tiingo client: %s", e)
                return None
        return TiingoProvider._client

    # ------------------------------------------------------------------
    # Helper: cached fetch
    # ------------------------------------------------------------------
    async def _cached_or_fetch(
        self,
        data_type: str,
        identifier: str,
        fetch_func,
    ) -> Optional[Dict[str, Any]]:
        """Get data from cache or fetch from source."""
        cache_key = f"tiingo:{data_type}:{identifier}"
        cached = await cache_get(cache_key)
        if cached is not None:
            logger.debug("Cache hit: %s", cache_key)
            return cached

        try:
            data = await fetch_func()
            if data:
                await cache_set(cache_key, data, ttl=_ttl(data_type))
                logger.debug("Cached: %s", cache_key)
            return data
        except Exception as e:
            logger.error("Fetch error for %s/%s: %s", data_type, identifier, e)
            return None

    # === Core Methods ===

    async def get_quote(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get real-time quote from Tiingo IEX endpoint."""
        if not self.is_available():
            logger.debug("Tiingo API key not configured, skipping")
            return None

        if market != US:
            return None

        try:
            client = self._get_client()
            if not client:
                return None

            def fetch():
                try:
                    data = client.get_ticker_price(symbol)
                    if not data:
                        return None
                    return data[-1] if isinstance(data, list) else data
                except Exception as e:
                    logger.warning("Tiingo IEX quote error: %s", e)
                    return None

            data = await run_in_executor(fetch)
            if not data:
                return None

            price = float(data.get("close") or data.get("adjClose", 0))
            prev_close = (
                float(data.get("prevClose", 0))
                if data.get("prevClose")
                else None
            )
            change = price - prev_close if prev_close else 0
            change_pct = (change / prev_close * 100) if prev_close else 0

            return {
                "symbol": symbol,
                "name": None,  # Tiingo price endpoint doesn't include name
                "price": price,
                "change": round(change, 4),
                "change_percent": round(change_pct, 2),
                "volume": int(data.get("volume", 0)),
                "market_cap": None,
                "high": (
                    float(data.get("high", 0))
                    if data.get("high")
                    else None
                ),
                "low": (
                    float(data.get("low", 0))
                    if data.get("low")
                    else None
                ),
                "open": (
                    float(data.get("open", 0))
                    if data.get("open")
                    else None
                ),
                "prev_close": prev_close,
                "timestamp": datetime.utcnow().isoformat(),
                "market": market,
                "currency": "USD",
                "source": "tiingo",
            }
        except Exception as e:
            logger.error("Tiingo quote error for %s: %s", symbol, e)
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
        """Get historical data from Tiingo EOD endpoint."""
        if not self.is_available():
            return None

        if market != US:
            return None

        # Tiingo only supports daily data for EOD
        if interval not in ("1d", "1wk", "1mo"):
            logger.debug(
                "Tiingo doesn't support intraday intervals: %s", interval
            )
            return None

        try:
            client = self._get_client()
            if not client:
                return None

            # Calculate date range
            end_date = datetime.now()
            days = _PERIOD_DAYS.get(period, 365)
            start_date = end_date - timedelta(days=days)

            # Map interval to Tiingo frequency
            frequency_map = {
                "1d": "daily",
                "1wk": "weekly",
                "1mo": "monthly",
            }
            frequency = frequency_map.get(interval, "daily")

            def fetch():
                data = client.get_ticker_price(
                    symbol,
                    startDate=start_date.strftime("%Y-%m-%d"),
                    endDate=end_date.strftime("%Y-%m-%d"),
                    frequency=frequency,
                )
                return data

            data = await run_in_executor(fetch)
            if not data:
                return None

            bars = []
            for row in data:
                date_str = row.get("date", "")
                if isinstance(date_str, str):
                    date_val = datetime.fromisoformat(
                        date_str.replace("Z", "+00:00").split("T")[0]
                    )
                else:
                    date_val = date_str

                bars.append({
                    "date": date_val.isoformat(),
                    "open": round(
                        float(row.get("adjOpen") or row.get("open", 0)), 4
                    ),
                    "high": round(
                        float(row.get("adjHigh") or row.get("high", 0)), 4
                    ),
                    "low": round(
                        float(row.get("adjLow") or row.get("low", 0)), 4
                    ),
                    "close": round(
                        float(
                            row.get("adjClose") or row.get("close", 0)
                        ),
                        4,
                    ),
                    "volume": int(
                        row.get("adjVolume") or row.get("volume", 0)
                    ),
                })

            return {
                "symbol": symbol,
                "interval": interval,
                "bars": bars,
                "market": market,
                "source": "tiingo",
            }
        except Exception as e:
            logger.error("Tiingo history error for %s: %s", symbol, e)
            return None

    async def search(
        self, query: str, markets: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        """Search for tickers (limited -- Tiingo doesn't have a search API)."""
        if not self.is_available():
            return []

        try:
            client = self._get_client()
            if not client:
                return []

            def fetch():
                try:
                    metadata = client.get_ticker_metadata(query.upper())
                    if metadata:
                        return [metadata]
                except Exception:
                    pass
                return []

            results = await run_in_executor(fetch)
            return [
                {
                    "symbol": r.get("ticker", query.upper()),
                    "name": r.get("name", ""),
                    "exchange": r.get("exchangeCode", ""),
                    "market": US,
                }
                for r in results
            ]
        except Exception as e:
            logger.error("Tiingo search error for %s: %s", query, e)
            return []

    # === Optional Methods ===

    async def get_info(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get company info from Tiingo metadata endpoint."""
        if not self.is_available() or market != US:
            return None

        async def fetch():
            client = self._get_client()
            if not client:
                return None

            def _fetch_sync():
                try:
                    metadata = client.get_ticker_metadata(symbol)
                    if not metadata:
                        return None
                    return metadata
                except Exception as e:
                    logger.warning("Tiingo metadata error: %s", e)
                    return None

            return await run_in_executor(_fetch_sync)

        data = await self._cached_or_fetch("info", symbol, fetch)
        if not data:
            return None

        return {
            "symbol": symbol,
            "name": data.get("name", ""),
            "description": data.get("description"),
            "sector": None,  # Tiingo metadata doesn't include sector
            "industry": None,
            "website": None,
            "employees": None,
            "market_cap": None,
            "currency": "USD",
            "exchange": data.get("exchangeCode", ""),
            "market": market,
            "source": "tiingo",
        }

    async def get_financials(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get financial data from Tiingo fundamentals endpoint."""
        if not self.is_available() or market != US:
            return None

        async def fetch():
            client = self._get_client()
            if not client:
                return None

            def _fetch_sync():
                try:
                    fundamentals = client.get_fundamentals_daily(symbol)
                    if not fundamentals:
                        return None
                    return (
                        fundamentals[-1]
                        if isinstance(fundamentals, list)
                        else fundamentals
                    )
                except Exception as e:
                    logger.warning("Tiingo fundamentals error: %s", e)
                    return None

            return await run_in_executor(_fetch_sync)

        data = await self._cached_or_fetch("financials", symbol, fetch)
        if not data:
            return None

        return {
            "symbol": symbol,
            "pe_ratio": (
                float(data.get("peRatio", 0))
                if data.get("peRatio")
                else None
            ),
            "forward_pe": None,
            "eps": (
                float(data.get("epsTTM", 0))
                if data.get("epsTTM")
                else None
            ),
            "dividend_yield": (
                float(data.get("divYield", 0))
                if data.get("divYield")
                else None
            ),
            "dividend_rate": None,
            "book_value": (
                float(data.get("bookVal", 0))
                if data.get("bookVal")
                else None
            ),
            "price_to_book": (
                float(data.get("pbRatio", 0))
                if data.get("pbRatio")
                else None
            ),
            "revenue": (
                float(data.get("revenue", 0))
                if data.get("revenue")
                else None
            ),
            "revenue_growth": None,
            "net_income": (
                float(data.get("netIncome", 0))
                if data.get("netIncome")
                else None
            ),
            "profit_margin": (
                float(data.get("profitMargin", 0))
                if data.get("profitMargin")
                else None
            ),
            "gross_margin": (
                float(data.get("grossMargin", 0))
                if data.get("grossMargin")
                else None
            ),
            "operating_margin": (
                float(data.get("opMargin", 0))
                if data.get("opMargin")
                else None
            ),
            "roe": (
                float(data.get("roe", 0)) if data.get("roe") else None
            ),
            "roa": (
                float(data.get("roa", 0)) if data.get("roa") else None
            ),
            "debt_to_equity": (
                float(data.get("debtEquity", 0))
                if data.get("debtEquity")
                else None
            ),
            "current_ratio": (
                float(data.get("currentRatio", 0))
                if data.get("currentRatio")
                else None
            ),
            "eps_growth": None,
            "payout_ratio": None,
            "market": market,
            "source": "tiingo",
        }

    # === Extended Methods ===

    async def get_fundamentals_statements(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get quarterly financial statements from Tiingo."""
        if not self.is_available():
            return None

        async def fetch():
            client = self._get_client()
            if not client:
                return None

            def _fetch_sync():
                try:
                    statements = client.get_fundamentals_statements(symbol)
                    if not statements:
                        return None

                    if isinstance(statements, list) and statements:
                        latest = statements[-1]
                        return {
                            "symbol": symbol,
                            "date": latest.get("date"),
                            "quarter": latest.get("quarter"),
                            "year": latest.get("year"),
                            "revenue": latest.get("revenue"),
                            "gross_profit": latest.get("grossProfit"),
                            "operating_income": latest.get(
                                "operatingIncome"
                            ),
                            "net_income": latest.get("netIncome"),
                            "eps": latest.get("eps"),
                            "total_assets": latest.get("totalAssets"),
                            "total_liabilities": latest.get(
                                "totalLiabilities"
                            ),
                            "shareholders_equity": latest.get(
                                "shareholdersEquity"
                            ),
                            "cash_and_equivalents": latest.get(
                                "cashAndEquiv"
                            ),
                            "total_debt": latest.get("totalDebt"),
                            "source": "tiingo",
                        }
                    return None
                except Exception as e:
                    logger.warning("Tiingo statements error: %s", e)
                    return None

            return await run_in_executor(_fetch_sync)

        return await self._cached_or_fetch(
            "fundamentals_statements", symbol, fetch
        )

    async def get_news(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Tiingo news via REST: ``GET /tiingo/news``.

        Works for global or per-symbol (US). HK / A-share unsupported.
        """
        if not self.is_available():
            return []
        if market and market != US:
            return []

        try:
            import requests

            params: Dict[str, Any] = {
                "token": self._api_key,
                "limit": min(limit, 100),
            }
            if symbol:
                params["tickers"] = symbol
            if since:
                # Tiingo accepts YYYY-MM-DD
                params["startDate"] = since[:10]

            def _fetch():
                try:
                    r = requests.get(
                        "https://api.tiingo.com/tiingo/news",
                        params=params,
                        timeout=10,
                    )
                    if r.status_code != 200:
                        logger.warning(
                            "Tiingo news http %d: %s",
                            r.status_code, r.text[:200],
                        )
                        return None
                    return r.json()
                except Exception as exc:
                    logger.warning("Tiingo news fetch error: %s", exc)
                    return None

            items = await run_in_executor(_fetch)
            if not items:
                return []

            out: List[Dict[str, Any]] = []
            for it in items[:limit]:
                tickers = it.get("tickers") or []
                tickers_upper = [str(t).upper() for t in tickers]
                out.append({
                    "source": "tiingo",
                    "id": str(it.get("id")) if it.get("id") is not None else None,
                    "title": it.get("title") or "",
                    "summary": it.get("description"),
                    "url": it.get("url"),
                    "publisher": it.get("source"),
                    "published_at": it.get("publishedDate"),
                    "symbols": tickers_upper,
                    "image_url": None,
                    "language": "en",
                    "raw": it,
                })
            return out
        except Exception as e:
            logger.error("Tiingo news error: %s", e)
            return []
