"""Batch ML data API routes for AlphaForge ML training pipeline.

POST endpoints accept multi-symbol batch requests with date range filtering.
GET market endpoints provide market-level aggregation time series.
All routes protected by X-API-Key.
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Path, Query
from pydantic import BaseModel, Field

from app.core.auth import verify_api_key
from app.schemas.base import ApiResponse

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/data/ml",
    tags=["ml-batch"],
    dependencies=[Depends(verify_api_key)],
)

VALID_MARKETS = {"us", "hk", "cn"}


# ---------------------------------------------------------------------------
# Request model
# ---------------------------------------------------------------------------

class BatchRequest(BaseModel):
    symbols: list[str] = Field(..., min_length=1, max_length=3000)
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    limit_per_symbol: int = Field(500, ge=1, le=5000)


def _fill_defaults(req: BatchRequest) -> tuple[date | None, date | None]:
    start, end = req.start_date, req.end_date
    if start is None and end is None:
        end = date.today()
        start = end - timedelta(days=365)
    return start, end


def _market_defaults(
    start_date: date | None, end_date: date | None,
    days: int | None = None,
) -> tuple[date, date]:
    end = end_date or date.today()
    start = start_date or (end - timedelta(days=days or 30))
    return start, end


# ---------------------------------------------------------------------------
# 7 Batch POST endpoints
# ---------------------------------------------------------------------------

@router.post("/batch/financials", response_model=ApiResponse[dict])
async def batch_financials(req: BatchRequest):
    """Batch SEC financials + valuation history, merged per symbol."""
    t0 = time.monotonic()
    start, end = _fill_defaults(req)
    from app.services import ml_batch_service
    data = await ml_batch_service.batch_financials(
        req.symbols, start, end, req.limit_per_symbol,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    return ApiResponse(
        data={"symbols": data, "count": len(data)},
        source="db", elapsed_ms=elapsed,
    )


@router.post("/batch/analyst", response_model=ApiResponse[dict])
async def batch_analyst(req: BatchRequest):
    """Batch analyst recommendations + upgrades/downgrades, merged per symbol."""
    t0 = time.monotonic()
    start, end = _fill_defaults(req)
    from app.services import ml_batch_service
    data = await ml_batch_service.batch_analyst(
        req.symbols, start, end, req.limit_per_symbol,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    return ApiResponse(
        data={"symbols": data, "count": len(data)},
        source="db", elapsed_ms=elapsed,
    )


@router.post("/batch/options", response_model=ApiResponse[dict])
async def batch_options(req: BatchRequest):
    """Batch options sentiment (put/call ratios, OI)."""
    t0 = time.monotonic()
    start, end = _fill_defaults(req)
    from app.services import ml_batch_service
    data = await ml_batch_service.batch_options(
        req.symbols, start, end, req.limit_per_symbol,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    return ApiResponse(
        data={"symbols": data, "count": len(data)},
        source="db", elapsed_ms=elapsed,
    )


@router.post("/batch/short-interest", response_model=ApiResponse[dict])
async def batch_short_interest(req: BatchRequest):
    """Batch short interest metrics (US only)."""
    t0 = time.monotonic()
    start, end = _fill_defaults(req)
    from app.services import ml_batch_service
    data = await ml_batch_service.batch_short_interest(
        req.symbols, start, end, req.limit_per_symbol,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    return ApiResponse(
        data={"symbols": data, "count": len(data)},
        source="db", elapsed_ms=elapsed,
    )


@router.post("/batch/insider", response_model=ApiResponse[dict])
async def batch_insider(req: BatchRequest):
    """Batch insider transactions + sentiment (MSPR), merged per symbol."""
    t0 = time.monotonic()
    start, end = _fill_defaults(req)
    from app.services import ml_batch_service
    data = await ml_batch_service.batch_insider(
        req.symbols, start, end, req.limit_per_symbol,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    return ApiResponse(
        data={"symbols": data, "count": len(data)},
        source="db", elapsed_ms=elapsed,
    )


@router.post("/batch/earnings", response_model=ApiResponse[dict])
async def batch_earnings(req: BatchRequest):
    """Batch earnings surprises + calendar, merged per symbol."""
    t0 = time.monotonic()
    start, end = _fill_defaults(req)
    from app.services import ml_batch_service
    data = await ml_batch_service.batch_earnings(
        req.symbols, start, end, req.limit_per_symbol,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    return ApiResponse(
        data={"symbols": data, "count": len(data)},
        source="db", elapsed_ms=elapsed,
    )


@router.post("/batch/sectors", response_model=ApiResponse[dict])
async def batch_sectors(req: BatchRequest):
    """Batch sector/industry classification from stock profiles."""
    t0 = time.monotonic()
    from app.services import ml_batch_service
    data = await ml_batch_service.batch_sectors(req.symbols)
    elapsed = int((time.monotonic() - t0) * 1000)
    return ApiResponse(
        data={"symbols": data, "count": len(data)},
        source="db", elapsed_ms=elapsed,
    )


# ---------------------------------------------------------------------------
# 3 Market GET endpoints
# ---------------------------------------------------------------------------

@router.get("/market/breadth/{market}", response_model=ApiResponse[dict])
async def get_market_breadth(
    market: str = Path(..., description="us / hk / cn"),
    days: Optional[int] = Query(None, ge=1, le=1500, description="Lookback calendar days"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
):
    """Market breadth: advancers/decliners/unchanged per trading day."""
    if market.lower() not in VALID_MARKETS:
        return ApiResponse(success=False, error=f"Invalid market: {market}. Use: us, hk, cn")
    t0 = time.monotonic()
    start, end = _market_defaults(start_date, end_date, days)
    from app.services import ml_batch_service
    data = await ml_batch_service.market_breadth(market, start, end)
    elapsed = int((time.monotonic() - t0) * 1000)
    return ApiResponse(
        data={"market": market, "days": len(data), "data": data},
        source="db", elapsed_ms=elapsed,
    )


@router.get("/market/volume/{market}", response_model=ApiResponse[dict])
async def get_market_volume(
    market: str = Path(..., description="us / hk / cn"),
    days: Optional[int] = Query(None, ge=1, le=1500, description="Lookback calendar days"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
):
    """Market volume: total volume/turnover, up/down volume, MA20, volume ratio."""
    if market.lower() not in VALID_MARKETS:
        return ApiResponse(success=False, error=f"Invalid market: {market}. Use: us, hk, cn")
    t0 = time.monotonic()
    start, end = _market_defaults(start_date, end_date, days)
    from app.services import ml_batch_service
    data = await ml_batch_service.market_volume(market, start, end)
    elapsed = int((time.monotonic() - t0) * 1000)
    return ApiResponse(
        data={"market": market, "days": len(data), "data": data},
        source="db", elapsed_ms=elapsed,
    )


@router.get("/market/sector-returns/{market}", response_model=ApiResponse[dict])
async def get_market_sector_returns(
    market: str = Path(..., description="us / hk / cn"),
    days: Optional[int] = Query(None, ge=1, le=1500, description="Lookback calendar days"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
):
    """Sector returns: avg daily return per sector per trading day."""
    if market.lower() not in VALID_MARKETS:
        return ApiResponse(success=False, error=f"Invalid market: {market}. Use: us, hk, cn")
    t0 = time.monotonic()
    start, end = _market_defaults(start_date, end_date, days)
    from app.services import ml_batch_service
    data = await ml_batch_service.market_sector_returns(market, start, end)
    elapsed = int((time.monotonic() - t0) * 1000)
    return ApiResponse(
        data={"market": market, "records": len(data), "data": data},
        source="db", elapsed_ms=elapsed,
    )
