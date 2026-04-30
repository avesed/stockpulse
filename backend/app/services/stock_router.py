"""Provider router with market-based routing and fallback chains.

Migrated from backend/app/services/providers/router.py.
Routes data requests to appropriate providers with fallback support.
Also exposes extended methods from akshare (northbound, fund_holdings, etc.)
and yfinance (institutional, indices, sector) for the analysis/market API layers.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar

from app.providers.base import DataProvider
from app.providers.constants import (
    HK,
    METAL,
    SH,
    SZ,
    US,
    detect_market,
    search_metals,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class StockRouter:
    """Routes data requests to appropriate providers with fallback support.

    Market routing strategy:
    - US stocks: yfinance primary, Tiingo fallback (if available)
    - METAL: yfinance only
    - HK: AKShare primary, yfinance fallback
    - A-shares (SH/SZ): AKShare primary, Tushare fallback (if available),
      yfinance fallback
    """

    def __init__(
        self,
        yfinance: DataProvider,
        akshare: DataProvider,
        tushare: Optional[DataProvider] = None,
        tiingo: Optional[DataProvider] = None,
        massive: Optional[DataProvider] = None,
        finnhub: Optional[DataProvider] = None,
    ):
        self._yfinance = yfinance
        self._akshare = akshare
        self._tushare = tushare
        self._tiingo = tiingo
        self._massive = massive
        self._finnhub = finnhub

        # Build routing table: market str -> list of providers (priority order)
        tushare_list = (
            [tushare] if tushare and tushare.is_available() else []
        )
        tiingo_list = (
            [tiingo] if tiingo and tiingo.is_available() else []
        )
        massive_list = (
            [massive] if massive and massive.is_available() else []
        )
        finnhub_list = (
            [finnhub] if finnhub and finnhub.is_available() else []
        )

        self._routing: Dict[str, List[DataProvider]] = {
            US: [yfinance] + massive_list + finnhub_list + tiingo_list,
            METAL: [yfinance],
            HK: [akshare, yfinance],
            SH: [akshare] + tushare_list + [yfinance],
            SZ: [akshare] + tushare_list + [yfinance],
        }

        # Name-based lookup for per-provider endpoints
        self._providers_by_name: Dict[str, DataProvider] = {
            "yfinance": yfinance,
            "akshare": akshare,
        }
        if tushare:
            self._providers_by_name["tushare"] = tushare
        if tiingo:
            self._providers_by_name["tiingo"] = tiingo
        if massive:
            self._providers_by_name["massive"] = massive
        if finnhub:
            self._providers_by_name["finnhub"] = finnhub

    def get_providers(self, market: str) -> List[DataProvider]:
        """Get ordered list of providers for a market."""
        return self._routing.get(market, [self._yfinance])

    def get_provider_by_name(self, name: str) -> Optional[DataProvider]:
        """Get a specific provider by name, or None if not available."""
        return self._providers_by_name.get(name)

    async def _try_providers(
        self,
        market: str,
        operation: str,
        func: Callable[[DataProvider], T],
    ) -> Optional[T]:
        """Try providers in order until one succeeds.

        Args:
            market: Target market string
            operation: Operation name for logging
            func: Async function that takes a provider and returns result

        Returns:
            Result from first successful provider, or None
        """
        providers = self.get_providers(market)

        for i, provider in enumerate(providers):
            try:
                result = await func(provider)
                if result is not None:
                    if i > 0:
                        logger.info(
                            "%s: Fallback to %s succeeded",
                            operation, provider.name,
                        )
                    return result
                logger.debug(
                    "%s: %s returned None, trying next",
                    operation, provider.name,
                )
            except Exception as e:
                logger.warning(
                    "%s: %s failed: %s", operation, provider.name, e
                )
                continue

        logger.warning(
            "%s: all %d providers exhausted, returning None",
            operation, len(providers),
        )
        return None

    # === Core Routing Methods ===

    async def get_quote(
        self, symbol: str, market: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get quote with automatic fallback."""
        if market is None:
            market = detect_market(symbol)

        return await self._try_providers(
            market,
            f"get_quote({symbol})",
            lambda p: p.get_quote(symbol, market),
        )

    async def get_history(
        self,
        symbol: str,
        period: str,
        interval: str,
        market: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get history with automatic fallback."""
        if market is None:
            market = detect_market(symbol)

        return await self._try_providers(
            market,
            f"get_history({symbol})",
            lambda p: p.get_history(
                symbol, market, period, interval, start=start, end=end
            ),
        )

    async def get_info(
        self, symbol: str, market: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get info with automatic fallback."""
        if market is None:
            market = detect_market(symbol)

        return await self._try_providers(
            market,
            f"get_info({symbol})",
            lambda p: p.get_info(symbol, market),
        )

    async def get_financials(
        self, symbol: str, market: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get financials with automatic fallback."""
        if market is None:
            market = detect_market(symbol)

        return await self._try_providers(
            market,
            f"get_financials({symbol})",
            lambda p: p.get_financials(symbol, market),
        )

    async def search(
        self,
        query: str,
        markets: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Search across markets with deduplication.

        Metal search is handled specially and always included first.
        """
        if markets is None:
            markets = [US, HK, SH, SZ, METAL]

        results: List[Dict[str, Any]] = []

        # Metal search first (special handling)
        if METAL in markets:
            metal_results = search_metals(query)
            results.extend(metal_results)

        # Parallel search across providers
        tasks = []
        if US in markets:
            tasks.append(self._yfinance.search(query, {US}))
        if HK in markets:
            tasks.append(self._akshare.search(query, {HK}))
        if SH in markets or SZ in markets:
            tasks.append(self._akshare.search(query, {SH, SZ}))

        if tasks:
            search_results = await asyncio.gather(
                *tasks, return_exceptions=True
            )

            for result in search_results:
                if isinstance(result, Exception):
                    logger.error("Search error: %s", result)
                    continue
                results.extend(result)

        # Deduplicate by symbol (metals added first have priority)
        seen: Set[str] = set()
        unique: List[Dict[str, Any]] = []
        for r in results:
            sym = r.get("symbol", "")
            if sym not in seen:
                seen.add(sym)
                unique.append(r)

        return unique[:50]

    # === Direct Provider Access for Extended Features ===

    @property
    def yfinance(self) -> DataProvider:
        """Direct access to yfinance provider for extended features."""
        return self._yfinance

    @property
    def akshare(self) -> DataProvider:
        """Direct access to akshare provider for extended features."""
        return self._akshare

    @property
    def tushare(self) -> Optional[DataProvider]:
        """Direct access to tushare provider (may be None)."""
        return self._tushare

    @property
    def tiingo(self) -> Optional[DataProvider]:
        """Direct access to tiingo provider (may be None)."""
        return self._tiingo

    @property
    def massive(self) -> Optional[DataProvider]:
        """Direct access to massive provider (may be None)."""
        return self._massive

    @property
    def finnhub(self) -> Optional[DataProvider]:
        """Direct access to finnhub provider (may be None)."""
        return self._finnhub

    # === Convenience Methods (combining data from multiple providers) ===

    async def get_market_context(self) -> Dict[str, Any]:
        """Get aggregated market context for sentiment analysis.

        Combines:
        - Major market indices (from yfinance)
        - Northbound capital flow summary (from akshare)
        """
        indices_task = self._yfinance.get_all_market_indices(period="5d")
        northbound_task = self._akshare.get_northbound_flow(
            "\u5317\u5411\u8d44\u91d1", days=10
        )

        indices, northbound = await asyncio.gather(
            indices_task, northbound_task, return_exceptions=True
        )

        # Handle exceptions
        if isinstance(indices, Exception):
            logger.error("Error fetching indices: %s", indices)
            indices = {}
        if isinstance(northbound, Exception):
            logger.error("Error fetching northbound: %s", northbound)
            northbound = None

        # Build northbound summary
        northbound_summary = None
        if northbound and northbound.get("flows"):
            flows = northbound["flows"]
            valid_flows = [
                f for f in flows if f.get("net_buy") is not None
            ]
            if valid_flows:
                latest = valid_flows[-1]
                total_5d = sum(
                    f["net_buy"]
                    for f in valid_flows[-5:]
                    if f.get("net_buy")
                )
                northbound_summary = {
                    "latest_date": latest.get("date"),
                    "latest_net_buy": latest.get("net_buy"),
                    "last_5d_net_buy": round(total_5d, 2),
                    "cumulative_net_buy": latest.get("cumulative_net_buy"),
                    "data_cutoff_notice": northbound.get(
                        "data_cutoff_notice"
                    ),
                }

        return {
            "sp500": indices.get("sp500") if indices else None,
            "hang_seng": indices.get("hang_seng") if indices else None,
            "shanghai_composite": (
                indices.get("shanghai") if indices else None
            ),
            "shenzhen_component": (
                indices.get("shenzhen") if indices else None
            ),
            "northbound_summary": northbound_summary,
            "fetched_at": datetime.utcnow().isoformat(),
            "source": "mixed",
        }

    # === Delegated Extended Methods ===

    async def get_analyst_ratings(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get analyst ratings (delegated to yfinance)."""
        return await self._yfinance.get_analyst_ratings(symbol)

    async def get_technical_info(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get technical info (delegated to yfinance)."""
        return await self._yfinance.get_technical_info(symbol)

    async def get_institutional_holders(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get institutional holders (delegated to yfinance)."""
        return await self._yfinance.get_institutional_holders(symbol)

    async def get_all_market_indices(
        self, period: str = "5d"
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Get all major market indices (delegated to yfinance)."""
        return await self._yfinance.get_all_market_indices(period=period)

    async def get_sector_industry(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get sector/industry (delegated to yfinance)."""
        return await self._yfinance.get_sector_industry(symbol)

    async def get_northbound_holding(
        self, symbol: str, days: int = 30
    ) -> Optional[Dict[str, Any]]:
        """Get northbound holding (delegated to akshare)."""
        return await self._akshare.get_northbound_holding(symbol, days=days)

    async def get_northbound_flow(
        self, direction: str = "\u5317\u5411\u8d44\u91d1", days: int = 30
    ) -> Optional[Dict[str, Any]]:
        """Get northbound flow (delegated to akshare)."""
        return await self._akshare.get_northbound_flow(
            direction=direction, days=days
        )

    async def get_fund_holdings_cn(
        self, symbol: str, quarter: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get fund holdings (delegated to akshare)."""
        return await self._akshare.get_fund_holdings_cn(
            symbol, quarter=quarter
        )

    async def get_industry_sector_list(
        self,
    ) -> Optional[Dict[str, Any]]:
        """Get industry sector list (delegated to akshare)."""
        return await self._akshare.get_industry_sector_list()

    async def get_stock_industry_cn(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get stock industry (delegated to akshare)."""
        return await self._akshare.get_stock_industry_cn(symbol)

    async def get_sector_history(
        self,
        sector_name: str,
        period: str = "\u65e5k",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get sector history (delegated to akshare)."""
        return await self._akshare.get_sector_history(
            sector_name,
            period=period,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_hk_stock_history(
        self, symbol: str, days: int = 30
    ) -> Optional[Dict[str, Any]]:
        """Get HK stock history with yfinance fallback (delegated to akshare)."""
        return await self._akshare.get_hk_stock_history(symbol, days=days)

    # === News fan-out ===

    async def get_news_fanout(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Pass-through fan-out: call every eligible provider in parallel
        and concatenate results. No dedup — downstream (NewsForge) owns it.

        Eligibility:
        - If ``symbol`` given, ``market`` is auto-detected (unless explicit).
          Only providers whose ``supported_markets`` contains that market
          run, plus akshare/tushare for zh-language global feed when applicable.
        - If no ``symbol``, every available provider runs against its global
          feed (those that don't have a global feed return []).
        """
        if symbol and market is None:
            market = detect_market(symbol)

        # Build candidate provider list
        candidates: List[DataProvider] = []
        if market:
            for p in (
                self._yfinance, self._akshare, self._tushare,
                self._tiingo, self._massive, self._finnhub,
            ):
                if p is None:
                    continue
                if not p.is_available():
                    continue
                if p.supports_market(market):
                    candidates.append(p)
        else:
            for p in (
                self._yfinance, self._akshare, self._tushare,
                self._tiingo, self._massive, self._finnhub,
            ):
                if p is None:
                    continue
                if not p.is_available():
                    continue
                candidates.append(p)

        if not candidates:
            return []

        async def _run(p: DataProvider) -> List[Dict[str, Any]]:
            try:
                return await p.get_news(
                    symbol=symbol, market=market, since=since, limit=limit,
                )
            except Exception as exc:
                logger.warning(
                    "news fan-out: %s failed: %s", p.name, exc,
                )
                return []

        results = await asyncio.gather(*(_run(p) for p in candidates))
        merged: List[Dict[str, Any]] = []
        for r in results:
            if r:
                merged.extend(r)
        return merged


# ---------------------------------------------------------------------------
# Singleton instance management
# ---------------------------------------------------------------------------
_router: Optional[StockRouter] = None
_router_lock = asyncio.Lock()


def reset_router() -> None:
    """Reset singleton so the next call to get_stock_router() rebuilds it."""
    global _router
    _router = None
    logger.info("StockRouter singleton reset — will reinitialize on next request")


async def get_stock_router() -> StockRouter:
    """Get singleton StockRouter instance."""
    global _router
    if _router is None:
        async with _router_lock:
            if _router is None:
                from app.providers.yfinance_provider import YFinanceProvider
                from app.providers.akshare_provider import AKShareProvider
                from app.providers.tushare_provider import TushareProvider
                from app.providers.tiingo_provider import TiingoProvider
                from app.providers.massive_provider import MassiveProvider
                from app.providers.finnhub_provider import FinnhubProvider

                yfinance = YFinanceProvider()
                akshare = AKShareProvider()
                tushare = (
                    TushareProvider()
                    if TushareProvider.is_available()
                    else None
                )
                tiingo = (
                    TiingoProvider()
                    if TiingoProvider.is_available()
                    else None
                )
                massive = (
                    MassiveProvider()
                    if MassiveProvider.is_available()
                    else None
                )
                finnhub = (
                    FinnhubProvider()
                    if FinnhubProvider.is_available()
                    else None
                )

                _router = StockRouter(
                    yfinance, akshare, tushare, tiingo, massive, finnhub
                )

                providers = ["yfinance", "akshare"]
                if tushare:
                    providers.append("tushare")
                if tiingo:
                    providers.append("tiingo")
                if massive:
                    providers.append("massive")
                if finnhub:
                    providers.append("finnhub")
                logger.info(
                    "StockRouter initialized: %s", ", ".join(providers)
                )
    return _router
