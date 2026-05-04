"""ML data API routes.

Read-only access to the 13 ML data tables: valuation history, insider
transactions/sentiment, earnings, recommendations, SEC filings, options,
short interest, macro indicators, and CN alternative data.

All routes are protected by API key and return ApiResponse envelopes.
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query

from app.core.auth import verify_api_key
from app.schemas.base import ApiResponse

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/v1/data/ml",
    tags=["ml-data"],
    dependencies=[Depends(verify_api_key)],
)


# --- Per-symbol endpoints ---------------------------------------------------

@router.get("/{symbol}/valuation-history", response_model=ApiResponse[dict])
async def get_valuation_history(
    symbol: str,
    period_type: str = Query("quarterly", description="quarterly or annual"),
    limit: int = Query(40, ge=1, le=200),
):
    """Historical valuation multiples and profitability ratios."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_valuation_history(symbol, period_type=period_type, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No valuation history for {symbol}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/{symbol}/insider-transactions", response_model=ApiResponse[dict])
async def get_insider_transactions(
    symbol: str,
    limit: int = Query(100, ge=1, le=500),
):
    """Insider buy/sell transactions."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_insider_transactions_from_db(symbol, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No insider transactions for {symbol}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/{symbol}/insider-sentiment", response_model=ApiResponse[dict])
async def get_insider_sentiment(
    symbol: str,
    limit: int = Query(24, ge=1, le=120),
):
    """Monthly insider sentiment (MSPR) from Finnhub."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_insider_sentiment_from_db(symbol, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No insider sentiment for {symbol}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/{symbol}/earnings-surprises", response_model=ApiResponse[dict])
async def get_earnings_surprises(
    symbol: str,
    limit: int = Query(20, ge=1, le=100),
):
    """Quarterly EPS actual vs estimate history."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_earnings_surprises_from_db(symbol, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No earnings surprises for {symbol}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/{symbol}/recommendation-trends", response_model=ApiResponse[dict])
async def get_recommendation_trends(
    symbol: str,
    limit: int = Query(12, ge=1, le=60),
):
    """Analyst recommendation trend breakdown by month."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_recommendation_trends_from_db(symbol, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No recommendation trends for {symbol}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/{symbol}/upgrades-downgrades", response_model=ApiResponse[dict])
async def get_upgrades_downgrades(
    symbol: str,
    limit: int = Query(100, ge=1, le=500),
):
    """Analyst upgrades and downgrades history."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_upgrades_downgrades_from_db(symbol, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No upgrades/downgrades for {symbol}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/{symbol}/sec-financials", response_model=ApiResponse[dict])
async def get_sec_financials(
    symbol: str,
    limit: int = Query(20, ge=1, le=100),
):
    """SEC financial statements (10-K/10-Q) with balance sheet, income, cash flow."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_sec_financials_from_db(symbol, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No SEC financials for {symbol}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/{symbol}/options-sentiment", response_model=ApiResponse[dict])
async def get_options_sentiment(
    symbol: str,
    limit: int = Query(30, ge=1, le=365),
):
    """Options put/call volume and open interest ratios."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_options_sentiment_from_db(symbol, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No options sentiment for {symbol}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/{symbol}/short-interest", response_model=ApiResponse[dict])
async def get_short_interest(
    symbol: str,
    limit: int = Query(30, ge=1, le=365),
):
    """Short interest: shares short, short ratio, % of float."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_short_interest_from_db(symbol, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No short interest for {symbol}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


# --- Non-symbol endpoints ---------------------------------------------------

@router.get("/earnings-calendar", response_model=ApiResponse[dict])
async def get_earnings_calendar(
    from_date: date = Query(default=None, description="Start date (YYYY-MM-DD)"),
    to_date: date = Query(default=None, description="End date (YYYY-MM-DD)"),
):
    """Earnings calendar for a date range (defaults to next 7 days)."""
    if from_date is None:
        from_date = date.today()
    if to_date is None:
        to_date = from_date + timedelta(days=7)
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_earnings_calendar_from_db(from_date, to_date)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No earnings calendar data for {from_date}~{to_date}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/economic/{code}", response_model=ApiResponse[dict])
async def get_economic_indicator(
    code: str,
    limit: int = Query(100, ge=1, le=1000),
):
    """Economic indicator time series (GDP, CPI, unemployment, etc.)."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_economic_indicator_from_db(code, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No economic indicator data for {code}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/macro/{ticker}", response_model=ApiResponse[dict])
async def get_macro_daily(
    ticker: str,
    limit: int = Query(365, ge=1, le=3000),
):
    """Macro daily bars (VIX, DXY, TNX, gold futures, etc.)."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_macro_daily_from_db(ticker, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No macro daily data for {ticker}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


@router.get("/cn/{symbol}/alternative", response_model=ApiResponse[dict])
async def get_cn_alternative(
    symbol: str,
    data_type: str = Query(..., description="margin, block_trade, etc."),
    limit: int = Query(30, ge=1, le=365),
):
    """CN A-share alternative data (margin, block trades, etc.)."""
    t0 = time.monotonic()
    from app.services import ml_db_service
    data = await ml_db_service.get_cn_alternative_from_db(symbol, data_type, limit=limit)
    elapsed = int((time.monotonic() - t0) * 1000)
    if data is None:
        return ApiResponse(success=False, error=f"No CN alternative data for {symbol}/{data_type}", elapsed_ms=elapsed)
    return ApiResponse(data=data, source="db", elapsed_ms=elapsed)


# --- Queue stats (no auth for admin monitoring) -----------------------------

queue_stats_router = APIRouter(
    prefix="/api/v1/data/ml",
    tags=["ml-data"],
)


@queue_stats_router.get("/queue-stats", response_model=ApiResponse[dict])
async def get_queue_stats_endpoint():
    """Provider queue stats (no auth, for monitoring dashboards)."""
    from app.core.provider_queue import get_queue_stats
    stats = get_queue_stats()
    return ApiResponse(data=stats, source="memory", elapsed_ms=0)
