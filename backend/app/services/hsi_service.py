"""Hang Seng Index constituent stocks service.

Provides HSI constituent list with multi-layer fallback:
  1. Redis cache (24h TTL)
  2. akshare API (ak.index_stock_cons("HSI"))
  3. Static hardcoded list (~79 constituents as of late 2025)

Results are cached in Redis DB 5 for 24 hours.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.core.redis import cache_get, cache_set
from app.core.executor import run_in_executor

logger = logging.getLogger(__name__)

# Redis cache settings
_CACHE_KEY = "ds:hsi:constituents"
_CACHE_TTL = 86400  # 24 hours

# Static fallback list of HSI constituents (as of late 2025)
_STATIC_HSI = [
    "0001.HK", "0002.HK", "0003.HK", "0005.HK", "0006.HK",
    "0011.HK", "0012.HK", "0016.HK", "0017.HK", "0027.HK",
    "0066.HK", "0101.HK", "0175.HK", "0241.HK", "0267.HK",
    "0288.HK", "0291.HK", "0316.HK", "0386.HK", "0388.HK",
    "0669.HK", "0688.HK", "0700.HK", "0762.HK", "0823.HK",
    "0857.HK", "0868.HK", "0881.HK", "0883.HK", "0939.HK",
    "0941.HK", "0960.HK", "0968.HK", "0981.HK", "1038.HK",
    "1044.HK", "1093.HK", "1109.HK", "1113.HK", "1177.HK",
    "1209.HK", "1211.HK", "1299.HK", "1378.HK", "1398.HK",
    "1810.HK", "1876.HK", "1928.HK", "1997.HK", "2007.HK",
    "2018.HK", "2269.HK", "2313.HK", "2318.HK", "2319.HK",
    "2331.HK", "2382.HK", "2388.HK", "2628.HK", "2688.HK",
    "3311.HK", "3328.HK", "3690.HK", "3692.HK", "3968.HK",
    "3988.HK", "6060.HK", "6078.HK", "6098.HK", "6618.HK",
    "6690.HK", "6862.HK", "9618.HK", "9626.HK", "9633.HK",
    "9888.HK", "9961.HK", "9988.HK", "9999.HK",
]


# ---------------------------------------------------------------------------
# Symbol normalization
# ---------------------------------------------------------------------------

def _normalize_hk_symbol(code: str) -> str:
    """Normalize a raw HK stock code to WebStock format (e.g. '0700.HK').

    Handles codes like '00700', '0700', '700', '09988', '9988'.
    - 5-digit codes (e.g. '09988'): keep as-is, add .HK suffix.
    - 4-digit or shorter: zero-pad to 4 digits, add .HK suffix.
    """
    code = code.strip()
    # Remove any existing .HK suffix for uniform processing
    if code.upper().endswith(".HK"):
        code = code[:-3]

    # Strip leading zeros then re-pad based on length
    digits = code.lstrip("0") or "0"

    if len(digits) >= 5:
        return f"{digits}.HK"
    else:
        return f"{digits.zfill(4)}.HK"


# ---------------------------------------------------------------------------
# AKShare fetchers (synchronous -- run via executor)
# ---------------------------------------------------------------------------

def _fetch_via_index_cons() -> Optional[List[str]]:
    """Fetch HSI constituents via ak.index_stock_cons('HSI').

    Returns a list of normalized symbols, or None on failure.
    """
    import akshare as ak

    df = ak.index_stock_cons("HSI")
    if df is None or df.empty:
        return None

    # The column name varies across akshare versions
    code_col = None
    for candidate in ("\u54c1\u79cd\u4ee3\u7801", "constituent_code", "code", "\u6210\u5206\u5238\u4ee3\u7801"):
        if candidate in df.columns:
            code_col = candidate
            break

    if code_col is None:
        if df.columns.empty:
            logger.warning("index_stock_cons returned DataFrame with no columns")
            return None
        code_col = df.columns[0]
        logger.info(
            "HSI index_stock_cons: using first column '%s' (available: %s)",
            code_col, list(df.columns),
        )

    symbols = []
    for raw_code in df[code_col].astype(str):
        sym = _normalize_hk_symbol(raw_code)
        symbols.append(sym)

    return sorted(set(symbols)) if symbols else None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def get_hsi_constituents() -> Dict[str, Any]:
    """Get HSI constituent symbols in WebStock format (e.g. '0700.HK').

    Tries: Redis cache -> akshare API -> static fallback.
    Always returns a non-empty result.

    Returns:
        Dict with keys: symbols (list[str]), count (int), source (str).
    """
    # Layer 0: Redis cache
    cached = await cache_get(_CACHE_KEY)
    if cached and isinstance(cached, dict) and cached.get("symbols"):
        logger.debug("HSI constituents from cache: %d stocks", len(cached["symbols"]))
        return {**cached, "cached": True}

    # Layer 1: akshare API
    try:
        symbols = await run_in_executor(_fetch_via_index_cons, timeout=30.0)
        if symbols:
            logger.info(
                "Fetched %d HSI constituents via index_stock_cons", len(symbols)
            )
            result = {
                "symbols": symbols,
                "count": len(symbols),
                "source": "akshare",
            }
            await cache_set(_CACHE_KEY, result, _CACHE_TTL)
            return {**result, "cached": False}
    except Exception as e:
        logger.warning("akshare index_stock_cons('HSI') failed: %s", e)

    # Layer 2: Static fallback
    logger.info("Using static HSI constituent list (%d stocks)", len(_STATIC_HSI))
    result = {
        "symbols": list(_STATIC_HSI),
        "count": len(_STATIC_HSI),
        "source": "static_fallback",
    }
    await cache_set(_CACHE_KEY, result, _CACHE_TTL)
    return {**result, "cached": False}
