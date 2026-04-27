"""Tushare data provider for A-shares (fallback).

Migrated from backend/app/services/providers/tushare.py.
Uses the shared executor instead of per-provider ThreadPool.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from app.config import get_settings
from app.core.executor import run_in_executor
from app.providers.base import DataProvider
from app.providers.constants import SH, SZ, normalize_symbol

logger = logging.getLogger(__name__)


class TushareProvider(DataProvider):
    """Tushare data provider for A-shares.

    This is a fallback provider that only activates if TUSHARE_TOKEN
    setting is configured. It provides quote data for A-shares when
    AKShare fails.

    Limitations:
    - Requires API token (paid service)
    - Only implements get_quote (no history, info, financials)
    - Daily data only (no intraday)
    """

    _token: Optional[str] = None

    def __init__(self):
        if TushareProvider._token is None:
            settings = get_settings()
            TushareProvider._token = settings.TUSHARE_TOKEN

    @property
    def name(self) -> str:
        return "tushare"

    @property
    def supported_markets(self) -> Set[str]:
        return {SH, SZ}

    @classmethod
    def is_available(cls) -> bool:
        """Check if Tushare API key is available."""
        if cls._token is None:
            settings = get_settings()
            cls._token = settings.TUSHARE_TOKEN
        return bool(cls._token)

    async def get_quote(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get quote from Tushare -- skip if no API key."""
        if not self.is_available():
            logger.debug("Tushare API key not configured, skipping")
            return None

        if market not in (SH, SZ):
            return None

        try:
            import tushare as ts

            ts.set_token(self._token)
            pro = ts.pro_api()

            code = normalize_symbol(symbol, market)
            ts_code = f"{code}.{'SH' if market == SH else 'SZ'}"

            def fetch():
                df = pro.daily(
                    ts_code=ts_code,
                    start_date=(
                        datetime.now() - timedelta(days=5)
                    ).strftime("%Y%m%d"),
                )
                if df is None or df.empty:
                    return None
                return df.iloc[0].to_dict()

            data = await run_in_executor(fetch)
            if not data:
                return None

            price = float(data.get("close", 0))
            prev_close = float(data.get("pre_close", price))
            change = price - prev_close
            change_pct = float(data.get("pct_chg", 0))

            return {
                "symbol": symbol,
                "name": None,  # Tushare daily doesn't include name
                "price": price,
                "change": round(change, 4),
                "change_percent": round(change_pct, 2),
                "volume": int(data.get("vol", 0) * 100),  # Tushare uses lots
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
                "currency": "CNY",
                "source": "tushare",
            }
        except Exception as e:
            logger.error("Tushare quote error for %s: %s", symbol, e)
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
        """History not implemented for Tushare fallback."""
        return None

    async def search(
        self, query: str, markets: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        """Search not implemented for Tushare fallback."""
        return []
