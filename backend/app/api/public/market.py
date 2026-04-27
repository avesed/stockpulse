"""Market data API — indices, context, forex rates, HSI constituents.

GET /api/v1/data/market/indices
    Major market indices (S&P 500, HSI, SSE, SZSE) from yfinance.

GET /api/v1/data/market/context
    Aggregated market overview (indices + northbound flow).

GET /api/v1/data/market/forex
    Exchange rates from USD to all available currencies (Finnhub + Redis cache).

GET /api/v1/data/market/hsi
    Hang Seng Index constituent symbols (akshare + Redis cache + static fallback).
"""
from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends, Query

from app.core.auth import verify_api_key
from app.schemas.base import ApiResponse
from app.schemas.market import CurrencyInfo, ForexRates, HSIConstituents

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/v1/data/market",
    tags=["market"],
    dependencies=[Depends(verify_api_key)],
)


# --- Stock-provider-backed routes (Step 2) ---


@router.get("/indices", response_model=ApiResponse[dict])
async def get_market_indices(
    period: str = Query("5d"),
):
    """Get all major market indices (S&P 500, HSI, SSE, SZSE)."""
    from app.services.stock_router import get_stock_router

    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_all_market_indices(period=period)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    return ApiResponse(
        data=data,
        source="yfinance",
        elapsed_ms=elapsed_ms,
    )


@router.get("/context", response_model=ApiResponse[dict])
async def get_market_context():
    """Get aggregated market overview (indices + northbound flow)."""
    from app.services.stock_router import get_stock_router

    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_market_context()
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    return ApiResponse(
        data=data,
        source="mixed",
        elapsed_ms=elapsed_ms,
    )


# --- Existing routes (Step 3) ---


@router.get("/forex", response_model=ApiResponse[ForexRates])
async def get_forex_rates():
    """Get foreign exchange rates from USD base currency.

    Returns all available rates from Finnhub, plus the list of currencies
    that the conversion API recognises. Results are cached in Redis for 1 hour.
    """
    from app.services.currency_service import get_exchange_rates, get_supported_currencies

    t0 = time.monotonic()
    try:
        rates = await get_exchange_rates()
        currencies = get_supported_currencies()
        elapsed_ms = int((time.monotonic() - t0) * 1000)

        # Determine if this came from cache (very fast) or API
        is_cached = elapsed_ms < 50
        source = "finnhub"

        return ApiResponse(
            success=True,
            data=ForexRates(
                rates=rates,
                supported_currencies=[
                    CurrencyInfo(**c) for c in currencies
                ],
            ),
            source=source,
            cached=is_cached,
            elapsed_ms=elapsed_ms,
        )
    except Exception as e:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.exception("Forex rates fetch failed: %s", e)
        return ApiResponse(
            success=False,
            error=str(e),
            elapsed_ms=elapsed_ms,
        )


@router.get("/hsi", response_model=ApiResponse[HSIConstituents])
async def get_hsi_constituents_endpoint():
    """Get Hang Seng Index constituent symbols.

    Tries Redis cache (24h) -> akshare API -> static fallback.
    Always returns a non-empty result.
    """
    from app.services.hsi_service import get_hsi_constituents

    t0 = time.monotonic()
    try:
        result = await get_hsi_constituents()
        elapsed_ms = int((time.monotonic() - t0) * 1000)

        is_cached = result.pop("cached", False)

        return ApiResponse(
            success=True,
            data=HSIConstituents(
                symbols=result["symbols"],
                count=result["count"],
                source=result.get("source"),
            ),
            source=result.get("source", "unknown"),
            cached=is_cached,
            elapsed_ms=elapsed_ms,
        )
    except Exception as e:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.exception("HSI constituents fetch failed: %s", e)
        return ApiResponse(
            success=False,
            error=str(e),
            elapsed_ms=elapsed_ms,
        )
