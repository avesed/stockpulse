"""Market constants, symbol utilities, and precious metals metadata.

Migrated from backend/app/services/stock_types.py. These are the subset of
constants and utility functions needed by the data-service providers.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# --- Market string constants ---
US = "us"
HK = "hk"
SH = "sh"
SZ = "sz"
METAL = "metal"

ALL_MARKETS = {US, HK, SH, SZ, METAL}

# --- Precious metals metadata ---
PRECIOUS_METALS: Dict[str, Dict[str, str]] = {
    "GC=F": {
        "name": "Gold Futures",
        "name_zh": "\u9ec4\u91d1\u671f\u8d27",
        "unit": "troy oz",
        "exchange": "COMEX",
        "currency": "USD",
    },
    "SI=F": {
        "name": "Silver Futures",
        "name_zh": "\u767d\u94f6\u671f\u8d27",
        "unit": "troy oz",
        "exchange": "COMEX",
        "currency": "USD",
    },
    "PL=F": {
        "name": "Platinum Futures",
        "name_zh": "\u94c2\u91d1\u671f\u8d27",
        "unit": "troy oz",
        "exchange": "NYMEX",
        "currency": "USD",
    },
    "PA=F": {
        "name": "Palladium Futures",
        "name_zh": "\u94af\u91d1\u671f\u8d27",
        "unit": "troy oz",
        "exchange": "NYMEX",
        "currency": "USD",
    },
}

# Metal search keywords mapping
METAL_KEYWORDS: Dict[str, List[str]] = {
    "GC=F": ["gold", "\u9ec4\u91d1", "gc", "xau", "gc=f"],
    "SI=F": ["silver", "\u767d\u94f6", "si=f", "xag"],  # "si" alone matches stock
    "PL=F": ["platinum", "\u94c2\u91d1", "pl", "pl=f"],
    "PA=F": ["palladium", "\u94af\u91d1", "pa", "pa=f"],
}


def is_precious_metal(symbol: str) -> bool:
    """Check if symbol is a precious metal future."""
    return symbol.upper() in PRECIOUS_METALS


def detect_market(symbol: str) -> str:
    """Detect market from symbol format.

    Formats:
    - Precious metals: GC=F, SI=F, PL=F, PA=F (checked FIRST to avoid conflicts)
    - US: AAPL, MSFT (no suffix)
    - HK: 0700.HK, 9988.HK
    - Shanghai: 600519.SS, 600036.SS
    - Shenzhen: 000001.SZ, 000858.SZ
    """
    symbol = symbol.upper()

    # Check precious metals FIRST (SI=F would otherwise match US pattern)
    if symbol in PRECIOUS_METALS:
        logger.debug("Detected market METAL for symbol: %s", symbol)
        return METAL

    if symbol.endswith(".HK"):
        return HK
    elif symbol.endswith(".SS"):
        return SH
    elif symbol.endswith(".SZ"):
        return SZ
    else:
        return US


def normalize_symbol(symbol: str, market: str) -> str:
    """Normalize symbol format for different markets/providers.

    - HK: Remove .HK suffix, pad to 5 digits for akshare
    - SH/SZ: Remove .SS/.SZ suffix for akshare
    - US/METAL: Return as-is
    """
    symbol = symbol.upper().strip()

    if market == HK:
        code = symbol.replace(".HK", "")
        return code.zfill(5)
    elif market in (SH, SZ):
        return symbol.replace(".SS", "").replace(".SZ", "")
    else:
        return symbol


def search_metals(query: str) -> List[Dict[str, Any]]:
    """Search precious metals by keyword.

    Supports keywords in English and Chinese:
    - gold/\u9ec4\u91d1/gc/xau -> GC=F (Gold Futures)
    - silver/\u767d\u94f6/si=f/xag -> SI=F (Silver Futures)
    - platinum/\u94c2\u91d1/pl -> PL=F (Platinum Futures)
    - palladium/\u94af\u91d1/pa -> PA=F (Palladium Futures)

    Returns:
        List of matching metal dicts (plain dicts, not dataclasses).
    """
    query_lower = query.lower().strip()
    results: List[Dict[str, Any]] = []

    for symbol, keywords in METAL_KEYWORDS.items():
        for kw in keywords:
            # For Chinese keywords, use exact match or contains
            if any("\u4e00" <= c <= "\u9fff" for c in kw):
                if kw in query_lower:
                    meta = PRECIOUS_METALS[symbol]
                    results.append({
                        "symbol": symbol,
                        "name": meta["name"],
                        "exchange": meta["exchange"],
                        "market": METAL,
                    })
                    logger.debug(
                        "Metal search matched (Chinese): %s for query '%s'",
                        symbol, query,
                    )
                    break
            else:
                # English/symbol - use word boundary or exact match
                pattern = (
                    rf"\b{re.escape(kw)}\b" if len(kw) > 2
                    else rf"^{re.escape(kw)}$"
                )
                if re.search(pattern, query_lower):
                    meta = PRECIOUS_METALS[symbol]
                    results.append({
                        "symbol": symbol,
                        "name": meta["name"],
                        "exchange": meta["exchange"],
                        "market": METAL,
                    })
                    logger.debug(
                        "Metal search matched (English): %s for query '%s'",
                        symbol, query,
                    )
                    break

    if results:
        logger.info(
            "Metal search found %d results for query '%s'", len(results), query
        )

    return results
