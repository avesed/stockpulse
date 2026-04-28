"""Per-provider REST endpoint factory.

Creates dedicated API routes for each data provider, bypassing
the auto-routing fallback chain.  Example:

    GET /api/v1/massive/quote/AAPL   → MassiveProvider.get_quote()
    GET /api/v1/yfinance/quote/AAPL  → YFinanceProvider.get_quote()

The existing ``/api/v1/data/*`` endpoints (auto-routed) remain unchanged.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.auth import verify_api_key
from app.schemas.base import ApiResponse
from app.schemas.stock import (
    FinancialsData,
    HistoryData,
    InfoData,
    OHLCVBar,
    QuoteData,
    SearchItem,
)
from app.services.stock_router import get_stock_router

logger = logging.getLogger(__name__)


def create_provider_router(provider_name: str) -> APIRouter:
    """Build a router with quote/history/info/financials/search endpoints
    that call a specific provider directly (no fallback chain)."""

    router = APIRouter(
        prefix=f"/api/v1/{provider_name}",
        tags=[provider_name],
        dependencies=[Depends(verify_api_key)],
    )

    @router.get("/quote/{symbol}", response_model=ApiResponse[QuoteData])
    async def get_quote(
        symbol: str,
        market: str = Query("us"),
    ):
        """Get real-time quote from {provider_name}."""
        t0 = time.monotonic()
        sr = await get_stock_router()
        provider = sr.get_provider_by_name(provider_name)
        if provider is None:
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.warning("Provider '%s' not available for quote/%s", provider_name, symbol)
            return ApiResponse(
                success=False,
                error=f"Provider '{provider_name}' is not available",
                elapsed_ms=elapsed,
            )

        data = await provider.get_quote(symbol, market)
        elapsed = int((time.monotonic() - t0) * 1000)

        if data is None:
            return ApiResponse(
                success=False,
                error=f"No quote data for {symbol} from {provider_name}",
                elapsed_ms=elapsed,
            )

        source = data.get("source", provider_name)
        quote = QuoteData(
            symbol=data.get("symbol", symbol),
            name=data.get("name"),
            price=data.get("price", 0),
            change=data.get("change", 0),
            change_percent=data.get("change_percent", 0),
            volume=data.get("volume"),
            market_cap=data.get("market_cap"),
            day_high=data.get("high") or data.get("day_high"),
            day_low=data.get("low") or data.get("day_low"),
            open=data.get("open"),
            previous_close=data.get("prev_close") or data.get("previous_close"),
            timestamp=data.get("timestamp"),
            market=data.get("market", market),
            currency=data.get("currency"),
            source=source,
        )

        logger.debug(
            "%s/quote/%s: price=%.2f elapsed=%dms",
            provider_name, symbol, quote.price, elapsed,
        )
        return ApiResponse(data=quote, source=source, elapsed_ms=elapsed)

    @router.get("/history/{symbol}", response_model=ApiResponse[HistoryData])
    async def get_history(
        symbol: str,
        period: str = Query("1y"),
        interval: str = Query("1d"),
        market: str = Query("us"),
        start: Optional[str] = Query(None),
        end: Optional[str] = Query(None),
    ):
        """Get historical OHLCV data from {provider_name}."""
        t0 = time.monotonic()
        sr = await get_stock_router()
        provider = sr.get_provider_by_name(provider_name)
        if provider is None:
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.warning("Provider '%s' not available for history/%s", provider_name, symbol)
            return ApiResponse(
                success=False,
                error=f"Provider '{provider_name}' is not available",
                elapsed_ms=elapsed,
            )

        data = await provider.get_history(
            symbol, market, period=period, interval=interval,
            start=start, end=end,
        )
        elapsed = int((time.monotonic() - t0) * 1000)

        if data is None:
            return ApiResponse(
                success=False,
                error=f"No history data for {symbol} from {provider_name}",
                elapsed_ms=elapsed,
            )

        bars = [
            OHLCVBar(
                date=b.get("date", ""),
                open=b.get("open", 0),
                high=b.get("high", 0),
                low=b.get("low", 0),
                close=b.get("close", 0),
                volume=b.get("volume"),
            )
            for b in data.get("bars", [])
        ]

        history = HistoryData(
            symbol=data.get("symbol", symbol),
            bars=bars,
            interval=data.get("interval", interval),
            market=data.get("market", market),
        )

        logger.debug(
            "%s/history/%s: %d bars elapsed=%dms",
            provider_name, symbol, len(bars), elapsed,
        )
        return ApiResponse(
            data=history,
            source=data.get("source", provider_name),
            elapsed_ms=elapsed,
        )

    @router.get("/info/{symbol}", response_model=ApiResponse[InfoData])
    async def get_info(
        symbol: str,
        market: str = Query("us"),
    ):
        """Get company / instrument info from {provider_name}."""
        t0 = time.monotonic()
        sr = await get_stock_router()
        provider = sr.get_provider_by_name(provider_name)
        if provider is None:
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.warning("Provider '%s' not available for info/%s", provider_name, symbol)
            return ApiResponse(
                success=False,
                error=f"Provider '{provider_name}' is not available",
                elapsed_ms=elapsed,
            )

        data = await provider.get_info(symbol, market)
        elapsed = int((time.monotonic() - t0) * 1000)

        if data is None:
            return ApiResponse(
                success=False,
                error=f"No info data for {symbol} from {provider_name}",
                elapsed_ms=elapsed,
            )

        source = data.get("source", provider_name)
        info = InfoData(
            symbol=data.get("symbol", symbol),
            name=data.get("name", ""),
            description=data.get("description"),
            sector=data.get("sector"),
            industry=data.get("industry"),
            website=data.get("website"),
            employees=data.get("employees"),
            market_cap=data.get("market_cap"),
            currency=data.get("currency"),
            exchange=data.get("exchange"),
            market=data.get("market", market),
            source=source,
        )
        return ApiResponse(data=info, source=source, elapsed_ms=elapsed)

    @router.get("/financials/{symbol}", response_model=ApiResponse[FinancialsData])
    async def get_financials(
        symbol: str,
        market: str = Query("us"),
    ):
        """Get financial metrics from {provider_name}."""
        t0 = time.monotonic()
        sr = await get_stock_router()
        provider = sr.get_provider_by_name(provider_name)
        if provider is None:
            elapsed = int((time.monotonic() - t0) * 1000)
            return ApiResponse(
                success=False,
                error=f"Provider '{provider_name}' is not available",
                elapsed_ms=elapsed,
            )

        data = await provider.get_financials(symbol, market)
        elapsed = int((time.monotonic() - t0) * 1000)

        if data is None:
            return ApiResponse(
                success=False,
                error=f"No financials data for {symbol} from {provider_name}",
                elapsed_ms=elapsed,
            )

        source = data.get("source", provider_name)
        financials = FinancialsData(
            symbol=data.get("symbol", symbol),
            pe_ratio=data.get("pe_ratio"),
            forward_pe=data.get("forward_pe"),
            eps=data.get("eps"),
            dividend_yield=data.get("dividend_yield"),
            dividend_rate=data.get("dividend_rate"),
            book_value=data.get("book_value"),
            price_to_book=data.get("price_to_book"),
            revenue=data.get("revenue"),
            revenue_growth=data.get("revenue_growth"),
            net_income=data.get("net_income"),
            profit_margin=data.get("profit_margin"),
            gross_margin=data.get("gross_margin"),
            operating_margin=data.get("operating_margin"),
            roe=data.get("roe"),
            roa=data.get("roa"),
            debt_to_equity=data.get("debt_to_equity"),
            current_ratio=data.get("current_ratio"),
            eps_growth=data.get("eps_growth"),
            payout_ratio=data.get("payout_ratio"),
            market=data.get("market", market),
            source=source,
        )
        return ApiResponse(data=financials, source=source, elapsed_ms=elapsed)

    @router.get("/search", response_model=ApiResponse[list[SearchItem]])
    async def search_stocks(
        q: str = Query(..., min_length=1),
    ):
        """Search for stocks via {provider_name}."""
        t0 = time.monotonic()
        sr = await get_stock_router()
        provider = sr.get_provider_by_name(provider_name)
        if provider is None:
            elapsed = int((time.monotonic() - t0) * 1000)
            return ApiResponse(
                success=False,
                error=f"Provider '{provider_name}' is not available",
                elapsed_ms=elapsed,
            )

        results = await provider.search(q)
        elapsed = int((time.monotonic() - t0) * 1000)

        items = [
            SearchItem(
                symbol=r.get("symbol", ""),
                name=r.get("name", ""),
                exchange=r.get("exchange"),
                market=r.get("market", "us"),
            )
            for r in results
        ]

        logger.debug(
            "%s/search?q=%s: %d results elapsed=%dms",
            provider_name, q, len(items), elapsed,
        )
        return ApiResponse(
            data=items,
            source=provider_name,
            elapsed_ms=elapsed,
        )

    return router
