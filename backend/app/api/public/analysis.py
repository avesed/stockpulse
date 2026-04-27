"""Analysis-oriented data API routes.

Analyst ratings, technicals, northbound data, institutional holders,
fund holdings, and sector/industry classification.

All routes are protected by API key and return ApiResponse envelopes.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.auth import verify_api_key
from app.schemas.base import ApiResponse
from app.services.stock_router import get_stock_router

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/v1/data/analysis",
    tags=["analysis"],
    dependencies=[Depends(verify_api_key)],
)


@router.get("/analyst-ratings/{symbol}", response_model=ApiResponse[dict])
async def get_analyst_ratings(symbol: str):
    """Get analyst consensus ratings and price targets (yfinance)."""
    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_analyst_ratings(symbol)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No analyst ratings for {symbol}",
            elapsed_ms=elapsed,
        )

    return ApiResponse(
        data=data,
        source=data.get("source", "yfinance"),
        elapsed_ms=elapsed,
    )


@router.get("/technical/{symbol}", response_model=ApiResponse[dict])
async def get_technical_info(symbol: str):
    """Get pre-calculated technical data (50/200 SMA, beta, 52w range)."""
    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_technical_info(symbol)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No technical data for {symbol}",
            elapsed_ms=elapsed,
        )

    return ApiResponse(
        data=data,
        source=data.get("source", "yfinance"),
        elapsed_ms=elapsed,
    )


@router.get(
    "/northbound/holding/{code}",
    response_model=ApiResponse[dict],
)
async def get_northbound_holding(
    code: str,
    days: int = Query(30, ge=1, le=365),
):
    """Get northbound (Stock Connect) holding for an A-share stock."""
    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_northbound_holding(code, days=days)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No northbound holding data for {code}",
            elapsed_ms=elapsed,
        )

    return ApiResponse(
        data=data,
        source=data.get("source", "akshare"),
        elapsed_ms=elapsed,
    )


@router.get(
    "/northbound/flow/{indicator}",
    response_model=ApiResponse[dict],
)
async def get_northbound_flow(
    indicator: str,
    days: int = Query(30, ge=1, le=365),
):
    """Get northbound capital flow history.

    indicator: e.g. "\u5317\u5411\u8d44\u91d1", "\u6caa\u80a1\u901a", "\u6df1\u80a1\u901a"
    """
    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_northbound_flow(direction=indicator, days=days)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No northbound flow data for {indicator}",
            elapsed_ms=elapsed,
        )

    return ApiResponse(
        data=data,
        source=data.get("source", "akshare"),
        elapsed_ms=elapsed,
    )


@router.get(
    "/institutional/{symbol}",
    response_model=ApiResponse[dict],
)
async def get_institutional_holders(symbol: str):
    """Get institutional holders for a stock (yfinance)."""
    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_institutional_holders(symbol)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No institutional holder data for {symbol}",
            elapsed_ms=elapsed,
        )

    return ApiResponse(
        data=data,
        source=data.get("source", "yfinance"),
        elapsed_ms=elapsed,
    )


@router.get(
    "/fund-holdings/{code}",
    response_model=ApiResponse[dict],
)
async def get_fund_holdings(
    code: str,
    quarter: Optional[str] = Query(None),
):
    """Get fund holdings for an A-share stock (akshare)."""
    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_fund_holdings_cn(code, quarter=quarter)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No fund holdings data for {code}",
            elapsed_ms=elapsed,
        )

    return ApiResponse(
        data=data,
        source=data.get("source", "akshare"),
        elapsed_ms=elapsed,
    )


@router.get(
    "/sector/{symbol}",
    response_model=ApiResponse[dict],
)
async def get_sector_info(
    symbol: str,
    market: str = Query("us"),
):
    """Get sector and industry classification for a stock.

    For US stocks: yfinance sector/industry.
    For A-shares: akshare industry info.
    """
    t0 = time.monotonic()
    sr = await get_stock_router()

    if market in ("sh", "sz"):
        data = await sr.get_stock_industry_cn(symbol)
    else:
        data = await sr.get_sector_industry(symbol)

    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No sector data for {symbol}",
            elapsed_ms=elapsed,
        )

    return ApiResponse(
        data=data,
        source=data.get("source", "unknown"),
        elapsed_ms=elapsed,
    )


@router.get(
    "/sector-list",
    response_model=ApiResponse[dict],
)
async def get_industry_sector_list():
    """Get list of all industry sectors with real-time data (akshare, A-share market)."""
    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_industry_sector_list()
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error="No sector list data",
            elapsed_ms=elapsed,
        )

    return ApiResponse(
        data=data,
        source=data.get("source", "akshare"),
        elapsed_ms=elapsed,
    )


@router.get(
    "/sector-history/{sector_name}",
    response_model=ApiResponse[dict],
)
async def get_sector_history(
    sector_name: str,
    period: str = Query("\u65e5k"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Get historical data for an industry sector (akshare)."""
    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_sector_history(
        sector_name,
        period=period,
        start_date=start_date,
        end_date=end_date,
    )
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No sector history for {sector_name}",
            elapsed_ms=elapsed,
        )

    return ApiResponse(
        data=data,
        source=data.get("source", "akshare"),
        elapsed_ms=elapsed,
    )
