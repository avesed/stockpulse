"""Internal API endpoints for machine-to-machine data access.

These endpoints replicate the main backend's internal API so that
qlib-service can read directly from data-service instead of going
through the main backend.  The response format must match exactly.

Endpoints:
    GET  /api/v1/data/internal/symbols/{market}
    POST /api/v1/data/internal/history/batch
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.core.auth import verify_api_key
from app.core.database import get_db_pool
from app.services import bar_persistence_service

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Static fallbacks (same as main backend)
# ---------------------------------------------------------------------------

_US_FALLBACK_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "BRK-B", "JPM", "V",
]

_CN_FALLBACK_SYMBOLS = [
    "600519.SS", "601318.SS", "600036.SS", "000858.SZ", "600276.SS",
    "601166.SS", "000333.SZ", "002415.SZ", "600900.SS", "601888.SS",
]

_METAL_SYMBOLS = ["GC=F", "SI=F", "PL=F", "PA=F"]

# US major exchanges — same filter as backend's internal.py
_US_MAJOR_EXCHANGES = {"XNAS", "XNYS", "ARCX", "BATS", "XASE"}


# ---------------------------------------------------------------------------
# Pydantic schemas — match backend CamelModel serialization
# ---------------------------------------------------------------------------

class SymbolsResponse(BaseModel):
    """Response for GET /symbols/{market}."""

    market: str
    symbols: list[str]
    count: int


class HistoryBatchRequest(BaseModel):
    """Request body for POST /history/batch.

    qlib-service sends camelCase keys (``startDate``, ``endDate``)
    because the main backend uses CamelModel.  We accept both forms.
    """

    symbols: list[str]
    market: str
    start_date: Optional[date] = Field(None, alias="startDate")
    end_date: Optional[date] = Field(None, alias="endDate")

    model_config = {"populate_by_name": True}


class HistoryBatchResponse(BaseModel):
    """Response for POST /history/batch.

    Meta keys are camelCase to match CamelModel serialization
    from the main backend (``symbolCount``, ``totalBars``).
    """

    data: dict[str, Any]
    meta: dict[str, Any]
    errors: list[str] = []


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(
    prefix="/api/v1/data/internal",
    tags=["Internal"],
    dependencies=[Depends(verify_api_key)],
)


@router.get(
    "/symbols/{market}",
    response_model=SymbolsResponse,
    summary="Get symbol list for a market",
    description=(
        "Returns tradeable symbols for the specified market. "
        "Used by qlib-service for data synchronization."
    ),
)
async def get_symbols(market: str) -> SymbolsResponse:
    """Return the symbol list for the requested market.

    Strategy per market:
    - us/cn: distinct symbols already stored in stock_daily_bars
      (falls back to stock_list_service or hardcoded list)
    - hk: HSI constituents via hsi_service
    - metal: hardcoded precious metals list
    """
    market = market.lower()
    logger.info("Internal API: get_symbols requested for market=%s", market)

    if market == "us":
        symbols = await _get_us_symbols()
    elif market == "hk":
        symbols = await _get_hk_symbols()
    elif market == "cn":
        symbols = await _get_cn_symbols()
    elif market == "metal":
        symbols = list(_METAL_SYMBOLS)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown market: {market}. Supported: us, hk, cn, metal",
        )

    logger.info(
        "Internal API: returning %d symbols for market=%s",
        len(symbols), market,
    )
    return SymbolsResponse(market=market, symbols=symbols, count=len(symbols))


@router.post(
    "/history/batch",
    response_model=HistoryBatchResponse,
    summary="Batch query daily bar data",
    description=(
        "Returns daily OHLCV data from the database in columnar format "
        "for efficient DataFrame construction. Max 50 symbols per request."
    ),
)
async def get_history_batch(request: HistoryBatchRequest) -> HistoryBatchResponse:
    """Return daily bar data for a batch of symbols."""
    if len(request.symbols) > 50:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Maximum 50 symbols per request, got {len(request.symbols)}",
        )

    logger.info(
        "Internal API: history batch requested: market=%s, symbols=%d, dates=%s..%s",
        request.market,
        len(request.symbols),
        request.start_date,
        request.end_date,
    )

    pool = get_db_pool()

    try:
        data = await bar_persistence_service.get_bars_batch(
            pool,
            request.symbols,
            request.start_date,
            request.end_date,
        )
    except Exception as exc:
        logger.error(
            "Failed to query history batch: market=%s, symbols=%d: %s",
            request.market, len(request.symbols), exc,
        )
        raise

    total_bars = sum(len(cols.get("dates", [])) for cols in data.values())
    errors: list[str] = []
    missing = set(request.symbols) - set(data.keys())
    if missing:
        errors.extend(f"{s}: No data in DB" for s in sorted(missing))

    logger.info(
        "Internal API: returning %d bars for %d/%d symbols",
        total_bars, len(data), len(request.symbols),
    )

    return HistoryBatchResponse(
        data=data,
        meta={
            "symbolCount": len(data),
            "totalBars": total_bars,
        },
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Per-market symbol helpers
# ---------------------------------------------------------------------------

async def _get_symbols_from_db(market: str) -> list[str]:
    """Query distinct symbols from stock_daily_bars for a given market."""
    pool = get_db_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT symbol FROM stock_daily_bars WHERE market = $1 ORDER BY symbol",
        market,
    )
    return [row["symbol"] for row in rows]


async def _get_us_symbols() -> list[str]:
    """Get US symbols: DB first, then stock_list_service, then fallback."""
    # Try DB first (fast, reliable once bars have been collected)
    symbols = await _get_symbols_from_db("us")
    if symbols:
        logger.info("Fetched %d US symbols from stock_daily_bars", len(symbols))
        return symbols

    # Fallback: build from stock_list_service (uses Finnhub API)
    try:
        from app.services.stock_list_service import build_stock_list

        items = await build_stock_list()
        symbols = [
            s["symbol"] for s in items
            if s.get("market") == "us"
            and s.get("exchange", "") in _US_MAJOR_EXCHANGES
        ]
        if symbols:
            logger.info(
                "Fetched %d US symbols from stock_list_service", len(symbols),
            )
            return symbols
        logger.warning("stock_list_service returned 0 US symbols")
    except Exception as exc:
        logger.warning("Failed to load US symbols from stock_list_service: %s", exc)

    logger.warning("Using US fallback symbols (%d)", len(_US_FALLBACK_SYMBOLS))
    return list(_US_FALLBACK_SYMBOLS)


async def _get_hk_symbols() -> list[str]:
    """Get HK symbols via HSI constituents service."""
    try:
        from app.services.hsi_service import get_hsi_constituents

        result = await get_hsi_constituents()
        symbols = result.get("symbols", [])
        logger.info("Fetched %d HK (HSI) symbols", len(symbols))
        return symbols
    except Exception as exc:
        logger.warning("Failed to fetch HK symbols: %s", exc)
        # Try DB as fallback
        symbols = await _get_symbols_from_db("hk")
        if symbols:
            logger.info("Fetched %d HK symbols from stock_daily_bars", len(symbols))
            return symbols
        raise


async def _get_cn_symbols() -> list[str]:
    """Get CN symbols: DB first, then stock_list_service, then fallback."""
    # Try DB first
    symbols = await _get_symbols_from_db("cn")
    if symbols:
        logger.info("Fetched %d CN symbols from stock_daily_bars", len(symbols))
        return symbols

    # Fallback: build from stock_list_service (uses AKShare API)
    try:
        from app.services.stock_list_service import build_stock_list

        items = await build_stock_list()
        symbols = [
            s["symbol"] for s in items
            if s.get("market") in ("sh", "sz")
        ]
        if symbols:
            logger.info(
                "Fetched %d CN symbols from stock_list_service", len(symbols),
            )
            return symbols
        logger.warning("stock_list_service returned 0 CN symbols")
    except Exception as exc:
        logger.warning("Failed to load CN symbols from stock_list_service: %s", exc)

    logger.warning("Using CN fallback symbols (%d)", len(_CN_FALLBACK_SYMBOLS))
    return list(_CN_FALLBACK_SYMBOLS)
