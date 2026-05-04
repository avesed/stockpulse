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

from app.core.api_keys import get_api_key, get_api_keys, get_next_api_key, mark_key_rate_limited
from app.core.redis import cache_get, cache_set, jittered_ttl
from app.core.executor import ExecutorPool
from app.core.provider_queue import Priority, submit
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


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check if an exception indicates a Finnhub 429 rate limit."""
    # finnhub.FinnhubAPIException has status_code attribute
    status = getattr(exc, "status_code", None)
    if status == 429:
        return True
    # Also check message for "API limit" as fallback
    msg = str(exc).lower()
    return "429" in msg or "api limit" in msg or "rate limit" in msg


class FinnhubProvider(DataProvider):
    """Finnhub data provider for US stocks.

    Requires FINNHUB_API_KEY setting.  Supports multiple keys with
    round-robin rotation for load balancing across free-tier limits.
    """

    # Client pool: api_key -> finnhub.Client
    _clients: Dict[str, Any] = {}
    _pool_snapshot: list[str] = []

    @property
    def name(self) -> str:
        return "finnhub"

    @property
    def supported_markets(self) -> Set[str]:
        return {US}

    @classmethod
    def is_available(cls) -> bool:
        return bool(get_api_key("finnhub"))

    def _get_client(self) -> tuple[Any, str] | tuple[None, None]:
        """Return (client, api_key) tuple with rate-limit-aware key selection."""
        current_pool = get_api_keys("finnhub")
        if not current_pool:
            return None, None

        # Detect pool changes — rebuild stale clients
        if current_pool != FinnhubProvider._pool_snapshot:
            stale = set(FinnhubProvider._clients.keys()) - set(current_pool)
            for k in stale:
                FinnhubProvider._clients.pop(k, None)
            FinnhubProvider._pool_snapshot = list(current_pool)

        # Pick next key via round-robin (skips rate-limited keys)
        key = get_next_api_key("finnhub")
        if not key:
            return None, None

        if key not in FinnhubProvider._clients:
            try:
                import finnhub
                FinnhubProvider._clients[key] = finnhub.Client(api_key=key)
                logger.info("Finnhub client initialized (pool size: %d)", len(FinnhubProvider._clients))
            except ImportError:
                logger.warning("finnhub-python package not installed")
                return None, None
            except Exception as e:
                logger.error("Failed to initialize Finnhub client: %s", e)
                return None, None
        return FinnhubProvider._clients[key], key

    async def get_quote(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        if not self.is_available() or market != US:
            return None

        client, key = self._get_client()
        if not client:
            return None

        try:
            def fetch():
                try:
                    return client.quote(symbol)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub quote error for %s: %s", symbol, e)
                    return None

            q = await submit("finnhub", fetch, priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND)
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

        client, key = self._get_client()
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
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub candles error for %s: %s", symbol, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND)
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

        client, key = self._get_client()
        if not client:
            return []

        try:
            def fetch():
                try:
                    return client.symbol_lookup(query)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub search error for %s: %s", query, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND)
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

        client, key = self._get_client()
        if not client:
            return None

        try:
            def fetch():
                try:
                    return client.company_profile2(symbol=symbol)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub profile error for %s: %s", symbol, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND)
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

    async def get_news(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Finnhub news. US-only.

        - With ``symbol``: ``company_news(symbol, _from, to)``
        - Without ``symbol``: ``general_news('general')``
        """
        if not self.is_available():
            return []
        if market and market != US:
            return []

        client, key = self._get_client()
        if not client:
            return []

        end_dt = datetime.utcnow()
        # Default 90 days — Finnhub company_news *requires* from/to, and a
        # tight default starves coverage for less-popular tickers. 90d is
        # still bounded by the per-call ``limit`` cap, so worst-case payload
        # size doesn't change.
        _DEFAULT_LOOKBACK = timedelta(days=90)
        if since:
            try:
                start_dt = datetime.fromisoformat(
                    since.replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except Exception:
                start_dt = end_dt - _DEFAULT_LOOKBACK
        else:
            start_dt = end_dt - _DEFAULT_LOOKBACK

        try:
            if symbol:
                def _fetch_sym():
                    try:
                        return client.company_news(
                            symbol,
                            _from=start_dt.strftime("%Y-%m-%d"),
                            to=end_dt.strftime("%Y-%m-%d"),
                        )
                    except Exception as e:
                        if _is_rate_limit_error(e):
                            mark_key_rate_limited("finnhub", key)
                        else:
                            logger.warning(
                                "Finnhub company_news error for %s: %s",
                                symbol, e,
                            )
                        return None

                items = await submit("finnhub", _fetch_sym, priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND)
            else:
                def _fetch_global():
                    try:
                        return client.general_news("general", min_id=0)
                    except Exception as e:
                        if _is_rate_limit_error(e):
                            mark_key_rate_limited("finnhub", key)
                        else:
                            logger.warning(
                                "Finnhub general_news error: %s", e,
                            )
                        return None

                items = await submit("finnhub", _fetch_global, priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND)

            if not items:
                return []

            out: List[Dict[str, Any]] = []
            for it in items[:limit]:
                ts = it.get("datetime")
                pub_iso = (
                    datetime.utcfromtimestamp(ts).isoformat() + "Z"
                    if isinstance(ts, (int, float))
                    else None
                )
                related = it.get("related") or ""
                symbols = (
                    [s.strip() for s in related.split(",") if s.strip()]
                    if isinstance(related, str)
                    else []
                )
                if symbol and symbol not in symbols:
                    symbols = [symbol] + symbols

                out.append({
                    "source": "finnhub",
                    "id": str(it.get("id")) if it.get("id") is not None else None,
                    "title": it.get("headline") or "",
                    "summary": it.get("summary"),
                    "url": it.get("url"),
                    "publisher": it.get("source"),
                    "published_at": pub_iso,
                    "symbols": symbols,
                    "image_url": it.get("image") or None,
                    "language": "en",
                    "raw": it,
                })
            return out
        except Exception as e:
            logger.error("Finnhub news error for %s: %s", symbol, e)
            return []

    async def get_peers(self, symbol: str) -> Optional[list[str]]:
        """Get peer company symbols from Finnhub."""
        if not self.is_available():
            return None

        client, key = self._get_client()
        if not client:
            return None

        try:
            def fetch():
                try:
                    return client.company_peers(symbol=symbol)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub peers error for %s: %s", symbol, e)
                    return None

            peers = await submit("finnhub", fetch, priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND)
            if not peers or not isinstance(peers, list):
                return None
            # Exclude self
            return [p for p in peers if p != symbol]
        except Exception as e:
            logger.error("Finnhub peers error for %s: %s", symbol, e)
            return None

    # ------------------------------------------------------------------
    # ML / collection-oriented methods (SCHEDULED priority)
    # ------------------------------------------------------------------

    async def get_valuation_series(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Annual & quarterly valuation/margin/ratio time-series."""
        if not self.is_available():
            return None

        client, key = self._get_client()
        if not client:
            return None

        try:
            def fetch():
                try:
                    return client.company_basic_financials(symbol, 'all')
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub valuation_series error for %s: %s", symbol, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND)
            if not data:
                return None
            return data.get("series") or None
        except Exception as e:
            logger.error("Finnhub valuation_series error for %s: %s", symbol, e)
            return None

    async def get_insider_sentiment(
        self, symbol: str, from_date: str, to_date: str,
    ) -> List[Dict[str, Any]]:
        """Monthly insider sentiment (MSPR & change)."""
        if not self.is_available():
            return []

        client, key = self._get_client()
        if not client:
            return []

        try:
            def fetch():
                try:
                    return client.stock_insider_sentiment(symbol, from_date, to_date)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub insider_sentiment error for %s: %s", symbol, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND)
            if not data:
                return []
            return data.get("data") or []
        except Exception as e:
            logger.error("Finnhub insider_sentiment error for %s: %s", symbol, e)
            return []

    async def get_earnings_surprises(self, symbol: str) -> List[Dict[str, Any]]:
        """Last 20 quarters of EPS actual vs estimate."""
        if not self.is_available():
            return []

        client, key = self._get_client()
        if not client:
            return []

        try:
            def fetch():
                try:
                    return client.company_earnings(symbol, limit=20)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub earnings_surprises error for %s: %s", symbol, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND)
            if not data or not isinstance(data, list):
                return []
            return data
        except Exception as e:
            logger.error("Finnhub earnings_surprises error for %s: %s", symbol, e)
            return []

    async def get_recommendation_trends(self, symbol: str) -> List[Dict[str, Any]]:
        """Analyst recommendation trends (buy/hold/sell breakdown)."""
        if not self.is_available():
            return []

        client, key = self._get_client()
        if not client:
            return []

        try:
            def fetch():
                try:
                    return client.recommendation_trends(symbol)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub recommendation_trends error for %s: %s", symbol, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND)
            if not data or not isinstance(data, list):
                return []
            return data
        except Exception as e:
            logger.error("Finnhub recommendation_trends error for %s: %s", symbol, e)
            return []

    async def get_sec_financials(self, symbol: str) -> List[Dict[str, Any]]:
        """SEC-filed quarterly financial statements (bs/ic/cf)."""
        if not self.is_available():
            return []

        client, key = self._get_client()
        if not client:
            return []

        try:
            def fetch():
                try:
                    return client.financials_reported(symbol=symbol, freq='quarterly')
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub sec_financials error for %s: %s", symbol, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND)
            if not data:
                return []
            return data.get("data") or []
        except Exception as e:
            logger.error("Finnhub sec_financials error for %s: %s", symbol, e)
            return []

    async def get_earnings_calendar(
        self, from_date: str, to_date: str,
    ) -> List[Dict[str, Any]]:
        """Earnings calendar for a date range (not per-symbol)."""
        if not self.is_available():
            return []

        client, key = self._get_client()
        if not client:
            return []

        try:
            def fetch():
                try:
                    import requests
                    r = requests.get(
                        "https://finnhub.io/api/v1/calendar/earnings",
                        params={"from": from_date, "to": to_date, "token": key},
                        timeout=15,
                    )
                    r.raise_for_status()
                    return r.json()
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub earnings_calendar error: %s", e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND)
            if not data:
                return []
            return data.get("earningsCalendar") or []
        except Exception as e:
            logger.error("Finnhub earnings_calendar error: %s", e)
            return []

    async def get_economic_indicator(self, code: str) -> List[Dict[str, Any]]:
        """Economic indicator time-series by FRED code (not per-symbol)."""
        if not self.is_available():
            return []

        client, key = self._get_client()
        if not client:
            return []

        try:
            def fetch():
                try:
                    return client.economic_data(code)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub economic_indicator error for %s: %s", code, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND)
            if not data:
                return []
            return data.get("data") or []
        except Exception as e:
            logger.error("Finnhub economic_indicator error for %s: %s", code, e)
            return []

    async def get_us_spending(
        self, symbol: str, from_date: str, to_date: str,
    ) -> List[Dict[str, Any]]:
        """US government spending data for a symbol."""
        if not self.is_available():
            return []

        client, key = self._get_client()
        if not client:
            return []

        try:
            def fetch():
                try:
                    return client.stock_usa_spending(symbol, from_date, to_date)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub us_spending error for %s: %s", symbol, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND)
            if not data:
                return []
            return data.get("data") or []
        except Exception as e:
            logger.error("Finnhub us_spending error for %s: %s", symbol, e)
            return []

    async def get_lobbying(
        self, symbol: str, from_date: str, to_date: str,
    ) -> List[Dict[str, Any]]:
        """Lobbying activity data for a symbol."""
        if not self.is_available():
            return []

        client, key = self._get_client()
        if not client:
            return []

        try:
            def fetch():
                try:
                    return client.stock_lobbying(symbol, from_date, to_date)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        mark_key_rate_limited("finnhub", key)
                    else:
                        logger.warning("Finnhub lobbying error for %s: %s", symbol, e)
                    return None

            data = await submit("finnhub", fetch, priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND)
            if not data:
                return []
            return data.get("data") or []
        except Exception as e:
            logger.error("Finnhub lobbying error for %s: %s", symbol, e)
            return []
