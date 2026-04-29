"""Generic index constituent resolution service.

Provides constituent lists for major market indices with 3-layer fallback:
  1. Redis cache (24h TTL)
  2. External API (akshare for CN, Finnhub for US)
  3. Static hardcoded fallback (~50 stocks per index)

Supported indices:
  - 000300 (CSI300, market=cn) -> akshare index_stock_cons
  - SPX    (S&P500, market=us) -> Finnhub indices_const
  - HSI    (Hang Seng, market=hk) -> delegates to hsi_service
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.core.redis import cache_get, cache_set
from app.core.executor import run_in_background_executor as run_in_executor

logger = logging.getLogger(__name__)

# Redis cache settings -- per-index keys, 24h TTL
_CACHE_TTL = 86400  # 24 hours


def _cache_key(index_code: str) -> str:
    return f"ds:index_constituents:{index_code}"


# ---------------------------------------------------------------------------
# Static fallback lists (top ~50 by weight, last updated 2026-02)
# ---------------------------------------------------------------------------

_STATIC_CSI300: List[str] = [
    "600519.SS", "601318.SS", "600036.SS", "000858.SZ", "000333.SZ",
    "601166.SS", "600276.SS", "000651.SZ", "601398.SS", "600030.SS",
    "600900.SS", "601888.SS", "000002.SZ", "600809.SS", "601012.SS",
    "000568.SZ", "002714.SZ", "600309.SS", "601668.SS", "002475.SZ",
    "601288.SS", "600585.SS", "601899.SS", "000725.SZ", "601225.SS",
    "600690.SS", "601669.SS", "603259.SS", "600438.SS", "002304.SZ",
    "002352.SZ", "601138.SS", "300750.SZ", "600048.SS", "000001.SZ",
    "601601.SS", "000776.SZ", "002271.SZ", "600000.SS", "601211.SS",
    "002594.SZ", "300015.SZ", "601688.SS", "000661.SZ", "300059.SZ",
    "601919.SS", "600050.SS", "002027.SZ", "601857.SS", "600104.SS",
]

_STATIC_SP500: List[str] = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B",
    "UNH", "XOM", "LLY", "JPM", "JNJ", "V", "PG", "MA", "AVGO", "HD",
    "CVX", "MRK", "ABBV", "COST", "PEP", "KO", "ADBE", "WMT", "MCD",
    "CRM", "CSCO", "BAC", "TMO", "ACN", "ABT", "NFLX", "LIN", "AMD",
    "DHR", "ORCL", "CMCSA", "TXN", "PM", "WFC", "NEE", "RTX", "INTC",
    "QCOM", "UPS", "AMGN", "LOW", "IBM",
]


# ---------------------------------------------------------------------------
# CN symbol normalization
# ---------------------------------------------------------------------------

def _normalize_cn_symbol(code: str) -> str:
    """Normalize raw A-share code to WebStock format.

    600519 -> 600519.SS  (Shanghai: 6xxxxx, 9xxxxx)
    000001 -> 000001.SZ  (Shenzhen: 0xxxxx, 3xxxxx, 2xxxxx)
    """
    code = str(code).strip().zfill(6)
    return f"{code}.SS" if code[0] in ("6", "9") else f"{code}.SZ"


# ---------------------------------------------------------------------------
# Fetchers (synchronous -- run via executor)
# ---------------------------------------------------------------------------

def _fetch_csi300_constituents() -> Optional[List[str]]:
    """Fetch CSI300 constituents via akshare.

    Returns normalized symbols or None on failure.
    """
    import akshare as ak

    df = ak.index_stock_cons("000300")
    if df is None or df.empty:
        return None

    # Flexible column detection (same pattern as hsi_service)
    code_col = None
    for candidate in ("\u54c1\u79cd\u4ee3\u7801", "constituent_code", "code", "\u6210\u5206\u5238\u4ee3\u7801"):
        if candidate in df.columns:
            code_col = candidate
            break

    if code_col is None:
        if df.columns.empty:
            logger.warning("index_stock_cons('000300') returned no columns")
            return None
        code_col = df.columns[0]
        logger.info(
            "CSI300 index_stock_cons: using first column '%s' (available: %s)",
            code_col, list(df.columns),
        )

    symbols = []
    for raw_code in df[code_col].astype(str):
        sym = _normalize_cn_symbol(raw_code)
        symbols.append(sym)

    return sorted(set(symbols)) if symbols else None


def _fetch_sp500_constituents() -> Optional[List[str]]:
    """Fetch S&P 500 constituents.

    Tries Finnhub first (structured API), falls back to Wikipedia scrape.
    Returns symbol list or None on failure.
    """
    # Attempt 1: Finnhub (requires premium plan)
    symbols = _fetch_sp500_via_finnhub()
    if symbols:
        return symbols

    # Attempt 2: Wikipedia table scrape
    return _fetch_sp500_via_wikipedia()


def _fetch_sp500_via_finnhub() -> Optional[List[str]]:
    """Try Finnhub indices_const API (may fail with 403 on free tier)."""
    try:
        import finnhub

        from app.core.api_keys import get_next_api_key, mark_key_rate_limited

        api_key = get_next_api_key("finnhub")
        if not api_key:
            return None

        client = finnhub.Client(api_key=api_key)
        result = client.indices_const(symbol="^GSPC")
        if not result:
            return None

        constituents = result.get("constituents", [])
        if not constituents:
            return None

        symbols = [s.replace(".", "-") for s in constituents]
        logger.info("Fetched %d S&P500 constituents via Finnhub", len(symbols))
        return sorted(set(symbols))
    except Exception as e:
        if getattr(e, "status_code", None) == 429:
            mark_key_rate_limited("finnhub", api_key)
        logger.info("Finnhub indices_const failed (expected on free tier): %s", e)
        return None


def _fetch_sp500_via_wikipedia() -> Optional[List[str]]:
    """Scrape S&P 500 constituent list from Wikipedia."""
    try:
        import io
        import urllib.request

        import pandas as pd

        # Wikipedia blocks requests without User-Agent
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; StockPulse/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8")

        tables = pd.read_html(io.StringIO(html))
        if not tables:
            return None

        df = tables[0]

        # Find the symbol column
        sym_col = None
        for candidate in ("Symbol", "Ticker symbol", "Ticker"):
            if candidate in df.columns:
                sym_col = candidate
                break
        if sym_col is None and not df.columns.empty:
            sym_col = df.columns[0]

        if sym_col is None:
            return None

        # Normalize: BRK.B -> BRK-B for yfinance compatibility
        symbols = [str(s).strip().replace(".", "-") for s in df[sym_col] if s]
        symbols = [s for s in symbols if s and len(s) <= 10]

        logger.info("Fetched %d S&P500 constituents via Wikipedia", len(symbols))
        return sorted(set(symbols)) if symbols else None
    except Exception as e:
        logger.warning("Wikipedia S&P500 scrape failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def get_index_constituents(index_code: str, market: str) -> Dict[str, Any]:
    """Get constituent symbols for a market index.

    Dispatches to index-specific fetcher with 3-layer fallback:
      1. Redis cache (24h)
      2. External API (akshare / Finnhub)
      3. Static fallback list

    Args:
        index_code: Index identifier (e.g. '000300', 'SPX', 'HSI').
        market: Market code (cn, us, hk).

    Returns:
        Dict with keys: symbols, count, source, index_code, market, cached.
    """
    # HSI -- delegate to existing specialized service
    if index_code.upper() == "HSI":
        from app.services.hsi_service import get_hsi_constituents

        hsi = await get_hsi_constituents()
        return {
            "symbols": hsi["symbols"],
            "count": hsi["count"],
            "source": hsi.get("source", "hsi_service"),
            "index_code": "HSI",
            "market": "hk",
            "cached": hsi.get("cached", False),
        }

    # Pick fetcher and static fallback based on index_code
    fetcher = None
    static_fallback: List[str] = []
    label = index_code

    if index_code == "000300":
        fetcher = _fetch_csi300_constituents
        static_fallback = _STATIC_CSI300
        label = "CSI300"
    elif index_code.upper() == "SPX":
        fetcher = _fetch_sp500_constituents
        static_fallback = _STATIC_SP500
        label = "S&P500"
    else:
        logger.warning(
            "Unsupported index_code=%s market=%s, returning empty",
            index_code, market,
        )
        return {
            "symbols": [],
            "count": 0,
            "source": "unsupported",
            "index_code": index_code,
            "market": market,
            "cached": False,
        }

    key = _cache_key(index_code)

    # Layer 0: Redis cache
    cached = await cache_get(key)
    if cached and isinstance(cached, dict) and cached.get("symbols"):
        logger.debug(
            "%s constituents from cache: %d stocks",
            label, len(cached["symbols"]),
        )
        return {**cached, "cached": True}

    # Layer 1: External API
    try:
        symbols = await run_in_executor(fetcher, timeout=30.0)
        if symbols:
            logger.info(
                "Fetched %d %s constituents via external API", len(symbols), label,
            )
            result: Dict[str, Any] = {
                "symbols": symbols,
                "count": len(symbols),
                "source": "akshare" if index_code == "000300" else "finnhub_or_wikipedia",
                "index_code": index_code,
                "market": market,
            }
            await cache_set(key, result, _CACHE_TTL)
            return {**result, "cached": False}
    except Exception as e:
        logger.warning("%s constituent fetch failed: %s", label, e)

    # Layer 2: Static fallback
    logger.info("Using static %s constituent list (%d stocks)", label, len(static_fallback))
    result = {
        "symbols": list(static_fallback),
        "count": len(static_fallback),
        "source": "static_fallback",
        "index_code": index_code,
        "market": market,
    }
    await cache_set(key, result, _CACHE_TTL)
    return {**result, "cached": False}
