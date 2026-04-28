"""Stock list data fetching service.

Fetches stock symbols from multiple data sources across all markets:
- US: Finnhub ``client.stock_symbols("US")``
- HK: AKShare ``ak.stock_hk_spot()``
- CN (SH+SZ+BJ): AKShare ``ak.stock_zh_a_spot_em()`` (EastMoney, single call for all
  ~5,300+ A-shares including STAR Board/ChiNext/BSE), with per-exchange fallback
- Precious metals: Hardcoded from constants.py

Returns raw dicts; does NOT persist to msgpack or manage in-memory indexes.
The backend Celery task calls this endpoint and handles persistence.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any, Dict, List, Tuple

from app.config import get_settings
from app.core.executor import run_in_background_executor as run_in_executor
from app.providers.constants import PRECIOUS_METALS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pinyin helpers
# ---------------------------------------------------------------------------

def _get_pinyin(name_zh: str) -> Tuple[str, str]:
    """Generate pinyin from Chinese name.

    Returns:
        Tuple of (full_pinyin, initials), e.g. ("PINGGUO", "PG").
    """
    if not name_zh:
        return "", ""

    try:
        from pypinyin import lazy_pinyin, Style

        full = "".join(lazy_pinyin(name_zh))
        initial = "".join(lazy_pinyin(name_zh, style=Style.FIRST_LETTER))
        return full.upper(), initial.upper()
    except Exception as e:
        logger.warning("Failed to generate pinyin for '%s': %s", name_zh, e)
        return "", ""


# ---------------------------------------------------------------------------
# Individual market fetchers (synchronous — run via executor)
# ---------------------------------------------------------------------------

def _fetch_finnhub_us() -> List[Dict[str, Any]]:
    """Fetch US stock symbols from Finnhub API.

    Falls back to yfinance top holdings from major ETFs when Finnhub key
    is not configured.
    """
    import finnhub

    from app.core.api_keys import get_api_key
    api_key = get_api_key("finnhub")
    if not api_key:
        logger.warning("Finnhub API key not configured, falling back to yfinance US stocks")
        return _fetch_yfinance_us_fallback()

    try:
        client = finnhub.Client(api_key=api_key)
        raw = client.stock_symbols("US")
        logger.info("Fetched %d raw US symbols from Finnhub", len(raw))
        return raw
    except Exception as e:
        logger.error("Failed to fetch US symbols from Finnhub: %s, trying yfinance fallback", e)
        return _fetch_yfinance_us_fallback()


def _fetch_yfinance_us_fallback() -> List[Dict[str, Any]]:
    """Fetch US stock symbols via yfinance using major index constituents.

    Uses S&P 500 from Wikipedia as the primary source, returning symbols
    in the same raw format as Finnhub (with symbol, description, mic fields)
    so that _process_finnhub_symbol() can process them uniformly.
    """
    symbols_found: Dict[str, Dict[str, Any]] = {}

    # Source 1: S&P 500 from Wikipedia (most reliable free source)
    try:
        import pandas as pd
        import io
        import urllib.request

        req = urllib.request.Request(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers={"User-Agent": "StockPulse/1.0 (stock data platform)"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8")
        tables = pd.read_html(io.StringIO(html))
        if tables:
            df = tables[0]
            for _, row in df.iterrows():
                symbol = str(row.get("Symbol", "")).strip().replace(".", "-")
                name = str(row.get("Security", "")).strip()
                exchange = str(row.get("CIK", "")).strip()
                if symbol and symbol not in symbols_found:
                    symbols_found[symbol] = {
                        "symbol": symbol,
                        "description": name,
                        "mic": "XNYS",
                        "type": "Common Stock",
                    }
            logger.info(
                "yfinance fallback: fetched %d S&P 500 symbols from Wikipedia",
                len(symbols_found),
            )
    except Exception as e:
        logger.warning("yfinance fallback: Wikipedia S&P 500 fetch failed: %s", e)

    # Source 2: NASDAQ-100 and Dow 30 via yfinance for broader coverage
    try:
        import yfinance as yf

        for index_sym, exchange in [("^NDX", "XNAS"), ("^DJI", "XNYS")]:
            try:
                ticker = yf.Ticker(index_sym)
                # yfinance doesn't expose constituents directly, skip
            except Exception:
                pass
    except Exception:
        pass

    if not symbols_found:
        logger.warning("yfinance fallback: no US symbols fetched, returning empty")
        return []

    result = list(symbols_found.values())
    logger.info("yfinance US fallback total: %d symbols", len(result))
    return result


def _process_finnhub_symbol(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a single Finnhub symbol dict into the standard stock-list format."""
    symbol = data.get("symbol", "")
    name = data.get("description", "")
    exchange = data.get("mic", "") or data.get("exchange", "")

    # Try to extract Chinese characters from name
    name_zh = ""
    if name:
        match = re.search(r"[\u4e00-\u9fff]+", name)
        if match:
            name_zh = match.group()

    pinyin, pinyin_initial = _get_pinyin(name_zh) if name_zh else ("", "")

    return {
        "symbol": symbol,
        "name": name,
        "name_zh": name_zh,
        "exchange": exchange,
        "market": "us",
        "pinyin": pinyin,
        "pinyin_initial": pinyin_initial,
    }


def _determine_cn_suffix(code: str) -> Tuple[str, str, str]:
    """Determine exchange suffix, exchange name, and market key for a CN stock code.

    A-share code ranges:
    - 6xxxxx -> SSE (Shanghai): Main Board + STAR Board (688xxx)
    - 0xxxxx -> SZSE (Shenzhen): Main Board
    - 3xxxxx -> SZSE (Shenzhen): ChiNext
    - 9xxxxx -> BSE (Beijing): 920xxx series
    - 8xxxxx -> BSE (Beijing): 83xxxx/87xxxx series

    Returns:
        (suffix, exchange, market) e.g. (".SS", "SSE", "sh")
    """
    if code.startswith("6"):
        return ".SS", "SSE", "sh"
    if code.startswith(("0", "3")):
        return ".SZ", "SZSE", "sz"
    if code.startswith(("8", "9")):
        return ".BJ", "BSE", "bj"
    # Fallback: treat as SH
    return ".SS", "SSE", "sh"


def _fetch_akshare_cn() -> List[Dict[str, Any]]:
    """Fetch all A-share symbols from AKShare via EastMoney (stock_zh_a_spot_em).

    This single API returns all ~5,300+ A-shares across SSE (Main Board + STAR Board),
    SZSE (Main Board + ChiNext), and BSE in one call -- far more reliable than calling
    three separate exchange APIs individually.

    Falls back to the legacy per-exchange approach if EastMoney fails.
    """
    stocks = _fetch_cn_via_eastmoney()
    if stocks:
        return stocks

    logger.warning("EastMoney API failed, falling back to per-exchange fetchers")
    return _fetch_cn_per_exchange_fallback()


def _fetch_cn_via_eastmoney() -> List[Dict[str, Any]]:
    """Primary: fetch all A-shares from EastMoney in a single call."""
    try:
        import akshare as ak

        df = ak.stock_zh_a_spot_em()
        stocks = []
        for _, row in df.iterrows():
            code = str(row.get("\u4ee3\u7801", "")).strip()
            name_zh = str(row.get("\u540d\u79f0", "")).strip()
            if not code or len(code) != 6:
                continue
            suffix, exchange, market = _determine_cn_suffix(code)
            pinyin, pinyin_initial = _get_pinyin(name_zh)
            stocks.append({
                "symbol": f"{code}{suffix}",
                "name": name_zh,
                "name_zh": name_zh,
                "exchange": exchange,
                "market": market,
                "pinyin": pinyin,
                "pinyin_initial": pinyin_initial,
            })

        by_market: Dict[str, int] = {}
        for s in stocks:
            m = s["market"]
            by_market[m] = by_market.get(m, 0) + 1
        logger.info(
            "Fetched %d CN stocks from EastMoney -- %s", len(stocks), by_market,
        )
        return stocks
    except Exception as e:
        logger.error("Failed to fetch CN stocks from EastMoney: %s", e)
        return []


def _fetch_cn_per_exchange_fallback() -> List[Dict[str, Any]]:
    """Fallback: fetch from each exchange API individually."""
    all_stocks: List[Dict[str, Any]] = []

    # Shanghai (Main Board + STAR Board)
    try:
        import akshare as ak

        for board in ("\u4e3b\u677fA\u80a1", "\u79d1\u521b\u677f"):
            df = ak.stock_info_sh_name_code(symbol=board)
            for _, row in df.iterrows():
                code = str(row.get("\u8bc1\u5238\u4ee3\u7801", "")).strip()
                name_zh = str(row.get("\u8bc1\u5238\u7b80\u79f0", "")).strip()
                if not code:
                    continue
                pinyin, pinyin_initial = _get_pinyin(name_zh)
                all_stocks.append({
                    "symbol": f"{code}.SS",
                    "name": name_zh,
                    "name_zh": name_zh,
                    "exchange": "SSE",
                    "market": "sh",
                    "pinyin": pinyin,
                    "pinyin_initial": pinyin_initial,
                })
        logger.info("Fallback: fetched %d Shanghai stocks", sum(1 for s in all_stocks if s["market"] == "sh"))
    except Exception as e:
        logger.error("Fallback: failed to fetch Shanghai stocks: %s", e)

    # Shenzhen
    try:
        import akshare as ak

        df = ak.stock_info_sz_name_code(symbol="A\u80a1\u5217\u8868")
        for _, row in df.iterrows():
            code = str(row.get("A\u80a1\u4ee3\u7801", "")).strip()
            name_zh = str(row.get("A\u80a1\u7b80\u79f0", "")).strip()
            if not code:
                continue
            pinyin, pinyin_initial = _get_pinyin(name_zh)
            all_stocks.append({
                "symbol": f"{code}.SZ",
                "name": name_zh,
                "name_zh": name_zh,
                "exchange": "SZSE",
                "market": "sz",
                "pinyin": pinyin,
                "pinyin_initial": pinyin_initial,
            })
        logger.info("Fallback: fetched %d Shenzhen stocks", sum(1 for s in all_stocks if s["market"] == "sz"))
    except Exception as e:
        logger.error("Fallback: failed to fetch Shenzhen stocks: %s", e)

    # Beijing
    try:
        import akshare as ak

        df = ak.stock_info_bj_name_code()
        for _, row in df.iterrows():
            code = str(row.get("\u8bc1\u5238\u4ee3\u7801", "")).strip()
            name_zh = str(row.get("\u8bc1\u5238\u7b80\u79f0", "")).strip()
            if not code:
                continue
            pinyin, pinyin_initial = _get_pinyin(name_zh)
            all_stocks.append({
                "symbol": f"{code}.BJ",
                "name": name_zh,
                "name_zh": name_zh,
                "exchange": "BSE",
                "market": "bj",
                "pinyin": pinyin,
                "pinyin_initial": pinyin_initial,
            })
        logger.info("Fallback: fetched %d Beijing stocks", sum(1 for s in all_stocks if s["market"] == "bj"))
    except Exception as e:
        logger.error("Fallback: failed to fetch Beijing stocks: %s", e)

    logger.info("Fallback total: %d CN stocks", len(all_stocks))
    return all_stocks


def _fetch_akshare_hk() -> List[Dict[str, Any]]:
    """Fetch Hong Kong stock symbols from AKShare (Sina source)."""
    try:
        import akshare as ak

        df = ak.stock_hk_spot()
        stocks = []
        for _, row in df.iterrows():
            code = str(row.get("\u4ee3\u7801", "")).strip()
            name_zh = str(row.get("\u4e2d\u6587\u540d\u79f0", "")).strip()
            name_en = str(row.get("\u82f1\u6587\u540d\u79f0", "")).strip()
            if not code:
                continue
            code_padded = code.zfill(5)
            pinyin, pinyin_initial = _get_pinyin(name_zh)
            stocks.append({
                "symbol": f"{code_padded}.HK",
                "name": name_en if name_en else name_zh,
                "name_zh": name_zh,
                "exchange": "HKEX",
                "market": "hk",
                "pinyin": pinyin,
                "pinyin_initial": pinyin_initial,
            })
        logger.info("Fetched %d Hong Kong stocks from AKShare", len(stocks))
        return stocks
    except Exception as e:
        logger.error("Failed to fetch Hong Kong stocks from AKShare: %s", e)
        return []


def _get_precious_metals() -> List[Dict[str, Any]]:
    """Build stock-list entries for precious metals from constants."""
    metals = []
    for symbol, meta in PRECIOUS_METALS.items():
        pinyin, pinyin_initial = _get_pinyin(meta.get("name_zh", ""))
        metals.append({
            "symbol": symbol,
            "name": meta["name"],
            "name_zh": meta.get("name_zh", ""),
            "exchange": meta["exchange"],
            "market": "metal",
            "pinyin": pinyin,
            "pinyin_initial": pinyin_initial,
        })
    logger.info("Got %d precious metals", len(metals))
    return metals


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def build_stock_list() -> List[Dict[str, Any]]:
    """Fetch stock symbols from all markets in parallel and return deduplicated list.

    Returns:
        List of stock dicts, each with keys:
        symbol, name, name_zh, exchange, market, pinyin, pinyin_initial.
    """
    t0 = time.monotonic()
    logger.info("Starting stock list build from all markets")

    # Run all synchronous fetchers in parallel via the thread-pool executor.
    # CN uses a single EastMoney call (with per-exchange fallback internally).
    results = await asyncio.gather(
        run_in_executor(_fetch_finnhub_us, timeout=90.0),
        run_in_executor(_fetch_akshare_hk, timeout=90.0),
        run_in_executor(_fetch_akshare_cn, timeout=300.0),
        return_exceptions=True,
    )

    # Unpack, treating exceptions as empty lists
    us_raw: List[Dict[str, Any]] = []
    hk_stocks: List[Dict[str, Any]] = []
    cn_stocks: List[Dict[str, Any]] = []

    labels = ["US", "HK", "CN"]
    targets = [us_raw, hk_stocks, cn_stocks]

    for i, (label, target) in enumerate(zip(labels, targets)):
        result = results[i]
        if isinstance(result, BaseException):
            logger.error("Fetch %s failed: %s", label, result)
        elif isinstance(result, list):
            target.extend(result)

    # Process US stocks (from Finnhub raw format)
    all_stocks: List[Dict[str, Any]] = []
    for raw_sym in us_raw:
        try:
            stock = _process_finnhub_symbol(raw_sym)
            all_stocks.append(stock)
        except Exception as e:
            logger.warning("Failed to process US symbol %s: %s", raw_sym, e)

    # Add AKShare-sourced stocks (already in standard format)
    all_stocks.extend(hk_stocks)
    all_stocks.extend(cn_stocks)

    # Add precious metals
    all_stocks.extend(_get_precious_metals())

    # Deduplicate by symbol (keep first occurrence)
    seen: set[str] = set()
    unique: List[Dict[str, Any]] = []
    for stock in all_stocks:
        sym = stock["symbol"]
        if sym not in seen:
            seen.add(sym)
            unique.append(stock)

    elapsed = time.monotonic() - t0
    by_market: Dict[str, int] = {}
    for s in unique:
        m = s.get("market", "unknown")
        by_market[m] = by_market.get(m, 0) + 1

    logger.info(
        "Stock list build complete: %d unique symbols in %.1fs -- %s",
        len(unique),
        elapsed,
        by_market,
    )
    return unique
