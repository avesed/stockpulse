"""Reference data API — stock lists and profiles.

POST /api/v1/data/reference/stock-list
    Build the full stock list (~37K symbols) and save to stock_symbols table.
    Long-running (~2 min). Returns summary (total + by_market).

POST /api/v1/data/reference/stock-profiles/{market}
    Collect stock profiles for a given market (cn, us, hk).
    For CN: symbols body is ignored (collects all from concept boards).
    For US/HK: ``symbols`` specifies which stocks to collect.
    Long-running (can take minutes). Returns profile dicts.
    **Legacy** — prefer the granular endpoints below.

POST /api/v1/data/reference/cn-concept-mapping
    Collect A-share concept board -> stock mapping (inversion only).
    Returns concepts dict + names dict. Timeout hint: 300s.

POST /api/v1/data/reference/stock-profiles-batch
    Fetch stock profiles for a small batch (max 50 symbols) of any market.
    Timeout hint: 60s.
"""
from __future__ import annotations

import logging
import time
from typing import List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.core.auth import verify_api_key
from app.schemas.base import ApiResponse
from app.schemas.reference import (
    BatchProfileRequest,
    ConceptMappingResult,
    IndexConstituentsResult,
    StockListResult,
    StockProfileResult,
)

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/v1/data/reference",
    tags=["reference"],
    dependencies=[Depends(verify_api_key)],
)


class ProfileRequest(BaseModel):
    """Request body for stock profile collection."""

    symbols: List[str] = []


@router.post("/stock-list", response_model=ApiResponse[StockListResult])
async def build_stock_list_endpoint():
    """Build the full stock list and save to the ``stock_symbols`` table.

    Fetches ~37K symbols from Finnhub (US) and AKShare (CN/HK) in parallel,
    generates pinyin for Chinese names, deduplicates, and persists to PostgreSQL.

    Timeout hint: 300s.
    """
    from app.services.stock_list_persistence import build_and_save_stock_list

    t0 = time.monotonic()
    try:
        result = await build_and_save_stock_list()
        elapsed_ms = int((time.monotonic() - t0) * 1000)

        if result.get("status") != "success":
            return ApiResponse(
                success=False,
                error=result.get("reason", "unknown error"),
                elapsed_ms=elapsed_ms,
            )

        return ApiResponse(
            success=True,
            data=StockListResult(items=[], count=result.get("total_stocks", 0)),
            source="finnhub+akshare",
            elapsed_ms=elapsed_ms,
        )
    except Exception as e:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.exception("Stock list build failed: %s", e)
        return ApiResponse(
            success=False,
            error=str(e),
            elapsed_ms=elapsed_ms,
        )


@router.post(
    "/stock-profiles/{market}",
    response_model=ApiResponse[StockProfileResult],
)
async def collect_profiles_endpoint(
    market: str,
    body: Optional[ProfileRequest] = None,
):
    """Collect stock profiles for a given market.

    - ``cn``: Collects all A-share profiles from concept boards + individual info.
      The ``symbols`` field in the body is ignored.
    - ``us``: Collects profiles for the given list of US stock symbols via yfinance.
    - ``hk``: Collects profiles for the given list of HK stock symbols via yfinance.

    Timeout hint: 300s.
    """
    from app.services.stock_profile_service import (
        collect_cn_profiles,
        collect_hk_profiles,
        collect_us_profiles,
    )

    market = market.lower().strip()
    symbols = body.symbols if body else []

    t0 = time.monotonic()
    try:
        if market == "cn":
            profiles = await collect_cn_profiles()
            source = "akshare"
        elif market == "us":
            if not symbols:
                return ApiResponse(
                    success=False,
                    error="symbols list required for US market",
                )
            profiles = await collect_us_profiles(symbols)
            source = "yfinance"
        elif market == "hk":
            if not symbols:
                return ApiResponse(
                    success=False,
                    error="symbols list required for HK market",
                )
            profiles = await collect_hk_profiles(symbols)
            source = "yfinance"
        else:
            return ApiResponse(
                success=False,
                error=f"Unsupported market: {market}. Must be cn, us, or hk.",
            )

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.info(
            "Profile collection for %s: %d profiles in %dms",
            market, len(profiles), elapsed_ms,
        )

        return ApiResponse(
            success=True,
            data=StockProfileResult(
                profiles=profiles,
                count=len(profiles),
                market=market,
            ),
            source=source,
            elapsed_ms=elapsed_ms,
        )
    except Exception as e:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.exception("Profile collection failed for %s: %s", market, e)
        return ApiResponse(
            success=False,
            error=str(e),
            elapsed_ms=elapsed_ms,
        )


# ---------------------------------------------------------------------------
# Granular endpoints (new) — small batches, short timeouts
# ---------------------------------------------------------------------------


@router.post(
    "/cn-concept-mapping",
    response_model=ApiResponse[ConceptMappingResult],
)
async def cn_concept_mapping_endpoint():
    """Collect A-share concept board -> stock mapping.

    Fetches all ~400 concept boards and inverts them to build a
    stock code -> concept names mapping. Does NOT fetch individual stock info.

    Timeout hint: 300s.
    """
    from app.services.stock_profile_service import collect_concept_mapping

    t0 = time.monotonic()
    try:
        concepts, names = await collect_concept_mapping()
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.info(
            "Concept mapping collected: %d stocks in %dms",
            len(concepts), elapsed_ms,
        )
        return ApiResponse(
            success=True,
            data=ConceptMappingResult(
                concepts=concepts,
                names=names,
                count=len(concepts),
            ),
            source="akshare",
            elapsed_ms=elapsed_ms,
        )
    except Exception as e:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.exception("Concept mapping failed: %s", e)
        return ApiResponse(
            success=False,
            error=str(e),
            elapsed_ms=elapsed_ms,
        )


@router.post(
    "/stock-profiles-batch",
    response_model=ApiResponse[StockProfileResult],
)
async def stock_profiles_batch_endpoint(body: BatchProfileRequest):
    """Fetch stock profiles for a small batch of symbols.

    Max 50 symbols per request. Per-market behaviour:
    - ``cn``: calls ``akshare.stock_individual_info_em`` per 6-digit code.
      Returns profiles WITHOUT concepts (caller merges from concept mapping).
    - ``us``: calls ``yfinance.Ticker.info`` per symbol.
    - ``hk``: calls ``yfinance.Ticker.info`` per symbol (handles 4/5-digit
      conversion internally).

    Timeout hint: 60s.
    """
    from app.services.stock_profile_service import (
        fetch_cn_stock_info_batch,
        fetch_hk_profiles_batch,
        fetch_us_profiles_batch,
    )

    market = body.market
    symbols = body.symbols

    if not symbols:
        return ApiResponse(success=False, error="symbols list is empty")

    t0 = time.monotonic()
    try:
        if market == "cn":
            profiles = await fetch_cn_stock_info_batch(symbols)
            source = "akshare"
        elif market == "us":
            profiles = await fetch_us_profiles_batch(symbols)
            source = "yfinance"
        else:  # hk — validated by Literal
            profiles = await fetch_hk_profiles_batch(symbols)
            source = "yfinance"

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.info(
            "Profile batch for %s: %d/%d profiles in %dms",
            market, len(profiles), len(symbols), elapsed_ms,
        )

        return ApiResponse(
            success=True,
            data=StockProfileResult(
                profiles=profiles,
                count=len(profiles),
                market=market,
            ),
            source=source,
            elapsed_ms=elapsed_ms,
        )
    except Exception as e:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.exception("Profile batch failed for %s: %s", market, e)
        return ApiResponse(
            success=False,
            error=str(e),
            elapsed_ms=elapsed_ms,
        )


# ---------------------------------------------------------------------------
# Index constituent resolution
# ---------------------------------------------------------------------------


@router.get(
    "/index-constituents/{index_code}",
    response_model=ApiResponse[IndexConstituentsResult],
)
async def get_index_constituents_endpoint(
    index_code: str,
    market: str = "us",
):
    """Get constituent symbols for a market index.

    Supported indices:
    - ``000300`` (CSI300, market=cn): ~300 A-share stocks via akshare
    - ``SPX`` (S&P500, market=us): ~500 US stocks via Finnhub
    - ``HSI`` (Hang Seng, market=hk): ~80 HK stocks via akshare

    Uses 3-layer fallback: Redis cache (24h) -> external API -> static fallback.
    Timeout hint: 30s.
    """
    from app.services.index_constituents_service import get_index_constituents

    t0 = time.monotonic()
    try:
        result = await get_index_constituents(index_code, market)
        elapsed_ms = int((time.monotonic() - t0) * 1000)

        return ApiResponse(
            success=True,
            data=IndexConstituentsResult(
                symbols=result["symbols"],
                count=result["count"],
                index_code=result.get("index_code", index_code),
                market=result.get("market", market),
                source=result.get("source"),
            ),
            source=result.get("source"),
            elapsed_ms=elapsed_ms,
            cached=result.get("cached", False),
        )
    except Exception as e:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.exception(
            "Index constituents fetch failed for %s (%s): %s",
            index_code, market, e,
        )
        return ApiResponse(
            success=False,
            error=str(e),
            elapsed_ms=elapsed_ms,
        )
