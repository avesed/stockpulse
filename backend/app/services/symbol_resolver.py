"""Per-market symbol resolution for collection and internal API.

Resolves the complete symbol list for each supported market by reading
from the ``stock_symbols`` PostgreSQL table (populated by stock list build).

Fallback order:
  1. Redis cache (24h TTL)
  2. PostgreSQL ``stock_symbols`` table
  3. Static hardcoded fallback list

- US: Major exchanges (XNAS, XNYS, ARCX, BATS, XASE)
- HK: HSI constituent stocks via hsi_service
- CN: SH + SZ + BJ markets from DB
- Metal: Static list (GC=F, SI=F, PL=F, PA=F)
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

# Non-common-stock suffixes to exclude (preferred, warrants, units, rights)
_EXCLUDED_SUFFIXES = (".WS", ".U", ".RT")


def _is_common_stock(symbol: str) -> bool:
    """Filter out preferred shares, warrants, units, and rights."""
    if any(symbol.endswith(s) for s in _EXCLUDED_SUFFIXES):
        return False
    # .PRx = preferred share (e.g. ABR.PRD, ACP.PRA)
    parts = symbol.split(".")
    if len(parts) == 2 and parts[1].startswith("PR") and len(parts[1]) <= 4:
        return False
    return True

# Market code mapping: collection market -> DB market values
_MARKET_DB_MAP = {
    "us": ("us",),
    "cn": ("sh", "sz", "bj"),
    "hk": ("hk",),
}

# Static fallbacks
_US_FALLBACK_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "BRK-B", "JPM", "V",
]

_CN_FALLBACK_SYMBOLS = [
    "600519.SS", "601318.SS", "600036.SS", "000858.SZ", "600276.SS",
    "601166.SS", "000333.SZ", "002415.SZ", "600900.SS", "601888.SS",
]

_METAL_SYMBOLS = ["GC=F", "SI=F", "PL=F", "PA=F"]

_HK_FALLBACK_SYMBOLS = [
    "00700.HK", "09988.HK", "00941.HK", "01810.HK", "02318.HK",
    "03690.HK", "09999.HK", "00388.HK", "02020.HK", "01024.HK",
    "00005.HK", "01398.HK", "00939.HK", "02628.HK", "00883.HK",
    "01211.HK", "00027.HK", "00669.HK", "09618.HK", "09888.HK",
]

# Fallback map per market
_FALLBACK_MAP = {
    "us": _US_FALLBACK_SYMBOLS,
    "cn": _CN_FALLBACK_SYMBOLS,
    "hk": _HK_FALLBACK_SYMBOLS,
    "metal": _METAL_SYMBOLS,
}


async def get_symbols(market: str) -> list[str]:
    """Get the list of tradeable symbols for a given market.

    Checks Redis cache first (24h TTL), then reads from DB, finally
    falls back to static lists.

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

    if market not in _MARKET_DB_MAP:
        raise ValueError(
            f"Unknown market: {market}. Supported: us, hk, cn, metal"
        )

    # Check cache
    cache_key = _CACHE_KEY_TEMPLATE.format(market=market)
    cached = await _cache_get_symbols(cache_key)
    if cached is not None:
        logger.info(
            "Symbol resolution for %s: %d symbols from cache",
            market, len(cached),
        )
        return cached

    # Resolve from DB (fast) → fallback to static list
    if market == "hk":
        symbols = await _resolve_hk_symbols()
    else:
        symbols = await _resolve_from_db(market)

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


async def _resolve_from_db(market: str) -> list[str]:
    """Read symbols from stock_symbols table for the given market.

    Maps collection market codes to DB market values:
      'cn' -> ('sh', 'sz', 'bj')
      'us' -> ('us',)

    For US, also filters to major exchanges only.
    Falls back to static list on DB error or empty result.
    """
    db_markets = _MARKET_DB_MAP.get(market, (market,))
    fallback = list(_FALLBACK_MAP.get(market, []))

    try:
        from app.core.database import get_db_pool

        pool = get_db_pool()
        placeholders = ", ".join(f"${i+1}" for i in range(len(db_markets)))

        if market == "us":
            # Filter to major exchanges for US
            exchange_list = tuple(_US_MAJOR_EXCHANGES)
            ex_placeholders = ", ".join(
                f"${i+1+len(db_markets)}" for i in range(len(exchange_list))
            )
            sql = (
                f"SELECT symbol FROM stock_symbols "
                f"WHERE market IN ({placeholders}) "
                f"AND exchange IN ({ex_placeholders}) "
                f"ORDER BY symbol"
            )
            rows = await pool.fetch(sql, *db_markets, *exchange_list)
        else:
            sql = (
                f"SELECT symbol FROM stock_symbols "
                f"WHERE market IN ({placeholders}) "
                f"ORDER BY symbol"
            )
            rows = await pool.fetch(sql, *db_markets)

        symbols = [row["symbol"] for row in rows]

        if market == "us":
            before = len(symbols)
            symbols = [s for s in symbols if _is_common_stock(s)]
            if before != len(symbols):
                logger.info(
                    "Filtered %d non-common-stock US symbols (%d -> %d)",
                    before - len(symbols), before, len(symbols),
                )

        if symbols:
            logger.info(
                "Resolved %d %s symbols from DB (markets=%s)",
                len(symbols), market.upper(), db_markets,
            )
            return symbols

        logger.warning(
            "DB returned 0 symbols for %s (markets=%s), using fallback",
            market, db_markets,
        )
    except Exception as exc:
        logger.warning(
            "Failed to resolve %s symbols from DB: %s", market, exc,
        )

    return fallback


async def _resolve_hk_symbols() -> list[str]:
    """Get HK symbols via HSI constituents service, with static fallback."""
    try:
        from app.services.hsi_service import get_hsi_constituents

        result = await get_hsi_constituents()
        symbols = result.get("symbols", [])
        if symbols:
            logger.info("Resolved %d HK (HSI) symbols", len(symbols))
            return symbols
        logger.warning("HSI service returned 0 symbols, using fallback")
    except Exception as exc:
        logger.warning("Failed to resolve HK symbols: %s, using fallback", exc)
    return list(_HK_FALLBACK_SYMBOLS)
