"""Per-market symbol resolution for collection and internal API.

Resolves the complete symbol list for each supported market:
- US: From stock list index (major exchanges only: XNAS, XNYS, ARCX, BATS, XASE)
- HK: HSI constituent stocks via hsi_service
- CN: From stock list index (SH + SZ markets)
- Metal: Static list (GC=F, SI=F, PL=F, PA=F)

Results are cached in Redis for 24h to avoid rebuilding stock list on every
collection run.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from app.core.redis import get_redis

logger = logging.getLogger(__name__)

# Redis cache settings
_CACHE_KEY_TEMPLATE = "ds:symbols:{market}"
_CACHE_TTL = 86400  # 24 hours

# Major US exchanges — excludes OTC (OOTC) due to poor data coverage
_US_MAJOR_EXCHANGES = {"XNAS", "XNYS", "ARCX", "BATS", "XASE"}

# Static fallbacks (same as backend/app/api/v1/internal.py)
_US_FALLBACK_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "BRK-B", "JPM", "V",
]

_CN_FALLBACK_SYMBOLS = [
    "600519.SS", "601318.SS", "600036.SS", "000858.SZ", "600276.SS",
    "601166.SS", "000333.SZ", "002415.SZ", "600900.SS", "601888.SS",
]

_METAL_SYMBOLS = ["GC=F", "SI=F", "PL=F", "PA=F"]


async def get_symbols(market: str) -> list[str]:
    """Get the list of tradeable symbols for a given market.

    Checks Redis cache first (24h TTL), falls back to live resolution.

    Args:
        market: One of 'us', 'hk', 'cn', 'metal'.

    Returns:
        List of symbol strings.

    Raises:
        ValueError: If market is not recognized.
    """
    market = market.lower()

    if market == "metal":
        return list(_METAL_SYMBOLS)

    # Check cache
    cache_key = _CACHE_KEY_TEMPLATE.format(market=market)
    cached = await _cache_get_symbols(cache_key)
    if cached is not None:
        logger.info(
            "Symbol resolution for %s: %d symbols from cache",
            market, len(cached),
        )
        return cached

    # Resolve from live sources
    if market == "us":
        symbols = await _resolve_us_symbols()
    elif market == "hk":
        symbols = await _resolve_hk_symbols()
    elif market == "cn":
        symbols = await _resolve_cn_symbols()
    else:
        raise ValueError(
            f"Unknown market: {market}. Supported: us, hk, cn, metal"
        )

    # Cache the result
    if symbols:
        await _cache_set_symbols(cache_key, symbols)

    logger.info(
        "Symbol resolution for %s: %d symbols (live)",
        market, len(symbols),
    )
    return symbols


async def invalidate_cache(market: str) -> None:
    """Clear the cached symbol list for a market.

    Useful after stock list updates to force re-resolution.
    """
    try:
        r = await get_redis()
        await r.delete(_CACHE_KEY_TEMPLATE.format(market=market))
    except Exception as e:
        logger.warning("Failed to invalidate symbol cache for %s: %s", market, e)


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


async def _cache_get_symbols(key: str) -> Optional[list[str]]:
    """Read symbol list from Redis cache."""
    try:
        r = await get_redis()
        data = await r.get(key)
        if data is not None:
            symbols = json.loads(data)
            if isinstance(symbols, list) and symbols:
                return symbols
    except Exception as e:
        logger.warning("Cache read error for symbols: %s", e)
    return None


async def _cache_set_symbols(key: str, symbols: list[str]) -> None:
    """Write symbol list to Redis cache with 24h TTL."""
    try:
        r = await get_redis()
        await r.setex(key, _CACHE_TTL, json.dumps(symbols))
    except Exception as e:
        logger.warning("Cache write error for symbols: %s", e)


# ---------------------------------------------------------------------------
# Per-market resolvers
# ---------------------------------------------------------------------------


async def _resolve_us_symbols() -> list[str]:
    """Get US symbols from the local stock list index.

    Filters to major exchanges only (XNAS, XNYS, ARCX, BATS, XASE).
    Falls back to a static list of top US stocks on failure.
    """
    try:
        from app.services.stock_list_service import build_stock_list

        all_stocks = await build_stock_list()
        symbols = [
            s["symbol"] for s in all_stocks
            if s.get("market") == "us"
            and s.get("exchange") in _US_MAJOR_EXCHANGES
        ]
        if symbols:
            logger.info("Resolved %d US symbols from stock list", len(symbols))
            return symbols
        logger.warning("Stock list returned 0 US symbols, using fallback")
    except Exception as exc:
        logger.warning("Failed to resolve US symbols from stock list: %s", exc)
    return list(_US_FALLBACK_SYMBOLS)


async def _resolve_hk_symbols() -> list[str]:
    """Get HK symbols via HSI constituents service."""
    try:
        from app.services.hsi_service import get_hsi_constituents

        result = await get_hsi_constituents()
        symbols = result.get("symbols", [])
        if symbols:
            logger.info("Resolved %d HK (HSI) symbols", len(symbols))
            return symbols
        logger.warning("HSI service returned 0 symbols")
    except Exception as exc:
        logger.warning("Failed to resolve HK symbols: %s", exc)
    return []


async def _resolve_cn_symbols() -> list[str]:
    """Get CN symbols from the local stock list index.

    Includes both Shanghai (sh) and Shenzhen (sz) markets.
    Falls back to a static list of major A-shares on failure.
    """
    try:
        from app.services.stock_list_service import build_stock_list

        all_stocks = await build_stock_list()
        symbols = [
            s["symbol"] for s in all_stocks
            if s.get("market") in ("sh", "sz")
        ]
        if symbols:
            logger.info("Resolved %d CN symbols from stock list", len(symbols))
            return symbols
        logger.warning("Stock list returned 0 CN symbols, using fallback")
    except Exception as exc:
        logger.warning("Failed to resolve CN symbols from stock list: %s", exc)
    return list(_CN_FALLBACK_SYMBOLS)
