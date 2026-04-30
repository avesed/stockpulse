"""News data API — pass-through fan-out across all configured providers.

Each item carries a ``source`` field; no deduplication is performed on
the StockPulse side. Downstream consumers (e.g. NewsForge) handle
dedup, ranking, and enrichment.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.auth import verify_api_key
from app.schemas.base import ApiResponse
from app.schemas.news import NewsItem
from app.services.stock_router import get_stock_router

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/v1/data",
    tags=["news"],
    dependencies=[Depends(verify_api_key)],
)


@router.get("/news", response_model=ApiResponse[list[NewsItem]])
async def get_news(
    symbol: Optional[str] = Query(
        None, description="Per-symbol news. Omit for global market feed.",
    ),
    market: Optional[str] = Query(
        None, description="Market hint (us/hk/sh/sz). Auto-detected from symbol when omitted.",
    ),
    since: Optional[str] = Query(
        None, description="ISO 8601 timestamp lower bound (best-effort, providers may ignore).",
    ),
    limit: int = Query(50, ge=1, le=500, description="Max items per provider."),
):
    """Fan-out news across all eligible providers (pass-through, no dedup)."""
    t0 = time.monotonic()
    sr = await get_stock_router()
    items_raw = await sr.get_news_fanout(
        symbol=symbol, market=market, since=since, limit=limit,
    )
    elapsed = int((time.monotonic() - t0) * 1000)

    items = [NewsItem(**it) for it in items_raw]
    by_source: dict[str, int] = {}
    for it in items:
        by_source[it.source] = by_source.get(it.source, 0) + 1

    logger.info(
        "news fan-out: symbol=%s market=%s items=%d by_source=%s elapsed=%dms",
        symbol, market, len(items), by_source, elapsed,
    )
    return ApiResponse(
        data=items,
        source="+".join(by_source.keys()) if by_source else None,
        elapsed_ms=elapsed,
    )
