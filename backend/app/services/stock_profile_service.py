"""Stock profile data collection service for knowledge base construction.

Collects enriched stock profiles via Yahoo quoteSummary API (single request
per symbol, modules=assetProfile,quoteType).  1/3 the HTTP overhead of
yfinance ``Ticker.info``.

- A-shares (CN): Yahoo quoteSummary + AKShare concept boards (enrichment)
- US stocks: Yahoo quoteSummary
- HK stocks: Yahoo quoteSummary

Two endpoint modes:
- **Monolithic** (legacy): ``collect_cn/us/hk_profiles()`` -- full market collection
  in one call.  Kept for backward compat but may timeout for large markets.
- **Granular** (new): ``collect_concept_mapping()`` + ``fetch_*_batch()`` -- small
  batches (<=50 symbols) that complete in <60s, orchestrated by the backend
  Celery worker.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from app.core.executor import run_in_profile_executor as run_in_executor

logger = logging.getLogger(__name__)

# Maximum consecutive 429 errors before aborting the current market collection
_MAX_429_STREAK = 10

_QUOTE_SUMMARY_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
_QUOTE_SUMMARY_PARAMS = {
    "modules": "assetProfile,quoteType",
    "formatted": "false",
}


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check if an exception signals HTTP 429 Too Many Requests."""
    msg = str(exc).lower()
    if "429" in msg or "too many requests" in msg or "rate limit" in msg:
        return True
    status = getattr(exc, "status_code", None)
    if status == 429:
        return True
    return False


def _fetch_yahoo_profile_sync(symbol: str) -> Optional[Dict[str, Any]]:
    """Single-request Yahoo quoteSummary fetch (assetProfile + quoteType only)."""
    import yfinance as yf

    ticker = yf.Ticker(symbol)
    yd = ticker._data
    url = _QUOTE_SUMMARY_URL.format(symbol=symbol)
    params = {**_QUOTE_SUMMARY_PARAMS, "symbol": symbol}
    resp = yd.get(url=url, params=params)
    if resp is None:
        return None
    data = resp.json()
    results = data.get("quoteSummary", {}).get("result")
    if not results:
        return None
    result = results[0]
    ap = result.get("assetProfile", {})
    qt = result.get("quoteType", {})
    name = qt.get("shortName") or qt.get("longName", "")
    if not name:
        return None
    return {
        "name": name,
        "sector": ap.get("sector", ""),
        "industry": ap.get("industry", ""),
        "description": (ap.get("longBusinessSummary") or "")[:500],
        "website": ap.get("website", ""),
    }


def _cn_code_to_symbol(code: str) -> Tuple[str, str]:
    """Convert a 6-digit A-share code to (symbol, market).

    Returns e.g. ("600519.SS", "sh") or ("000001.SZ", "sz").
    """
    if code.startswith(("6", "9")):
        return f"{code}.SS", "sh"
    elif code.startswith(("0", "2", "3")):
        return f"{code}.SZ", "sz"
    return f"{code}.SS", "sh"


# ---------------------------------------------------------------------------
# CN concept board mapping (shared by monolithic + granular endpoints)
# ---------------------------------------------------------------------------

async def collect_concept_mapping() -> Tuple[
    Dict[str, List[str]], Dict[str, str]
]:
    """Collect A-share concept board -> stock mapping (inversion only).

    Fetches all ~400 concept board names, then for each board fetches its
    constituent stocks to build an inverted mapping: stock code -> list of
    concept board names.

    Returns:
        Tuple of (concepts_dict, names_dict) where:
        - concepts_dict: {6-digit code: [sorted concept names]}
        - names_dict: {6-digit code: name_zh}
    """
    import akshare as ak

    logger.info("[StockProfile] Starting concept board mapping")
    t0 = time.monotonic()

    # Step 1: Fetch concept board list
    try:
        boards_df = await run_in_executor(
            ak.stock_board_concept_name_em, timeout=60.0
        )
    except Exception as e:
        logger.error("[StockProfile] Failed to fetch concept boards: %s", e)
        return {}, {}

    if boards_df is None or boards_df.empty:
        logger.warning("[StockProfile] Empty concept board list")
        return {}, {}

    board_names = boards_df["\u677f\u5757\u540d\u79f0"].tolist()
    logger.info("[StockProfile] Found %d concept boards", len(board_names))

    # Step 2: Invert concept boards -> stock->concepts mapping
    stock_concepts: Dict[str, Set[str]] = {}  # code -> set of concept names
    stock_names: Dict[str, str] = {}  # code -> name_zh
    sem = asyncio.Semaphore(3)
    errors = 0
    rate_limit_streak = 0
    abort_event = asyncio.Event()

    async def fetch_board_stocks(board_name: str) -> None:
        nonlocal errors, rate_limit_streak
        if abort_event.is_set():
            return
        async with sem:
            if abort_event.is_set():
                return
            for attempt in range(3):
                try:
                    df = await run_in_executor(
                        lambda bn=board_name: ak.stock_board_concept_cons_em(symbol=bn),
                        timeout=30.0,
                    )
                    rate_limit_streak = 0
                    if df is not None and not df.empty:
                        for _, row in df.iterrows():
                            code = str(row.get("\u4ee3\u7801", "")).strip()
                            name = str(row.get("\u540d\u79f0", "")).strip()
                            if code and len(code) == 6:
                                stock_concepts.setdefault(code, set()).add(board_name)
                                if name and code not in stock_names:
                                    stock_names[code] = name
                    break
                except Exception as e:
                    if _is_rate_limit_error(e):
                        rate_limit_streak += 1
                        wait = 30 * (attempt + 1)
                        logger.warning(
                            "[StockProfile] 429 on board '%s' (streak=%d), "
                            "waiting %ds...",
                            board_name, rate_limit_streak, wait,
                        )
                        if rate_limit_streak >= _MAX_429_STREAK:
                            logger.error(
                                "[StockProfile] Too many 429s, aborting CN boards"
                            )
                            abort_event.set()
                            return
                        await asyncio.sleep(wait)
                        continue
                    errors += 1
                    if errors <= 5:
                        logger.warning(
                            "[StockProfile] Error fetching board '%s': %s",
                            board_name, e,
                        )
                    break
            await asyncio.sleep(1.0)

    # Process boards in batches of 20
    batch_size = 20
    for i in range(0, len(board_names), batch_size):
        batch = board_names[i : i + batch_size]
        await asyncio.gather(
            *[fetch_board_stocks(b) for b in batch],
            return_exceptions=True,
        )
        if i % 100 == 0 and i > 0:
            logger.info(
                "[StockProfile] Processed %d/%d boards, %d stocks so far",
                i, len(board_names), len(stock_concepts),
            )

    # Convert sets to sorted lists
    concepts_dict = {
        code: sorted(concepts)
        for code, concepts in stock_concepts.items()
    }

    elapsed = time.monotonic() - t0
    logger.info(
        "[StockProfile] Concept board mapping complete: %d stocks, %d errors in %.0fs",
        len(concepts_dict), errors, elapsed,
    )
    return concepts_dict, dict(stock_names)


# ---------------------------------------------------------------------------
# CN profiles: concept boards + individual stock info (monolithic, legacy)
# ---------------------------------------------------------------------------

async def collect_cn_profiles() -> List[Dict[str, Any]]:
    """Collect A-share profiles via Yahoo quoteSummary, with optional concept enrichment."""
    logger.info("[StockProfile] Starting CN profile collection")
    t0 = time.monotonic()

    try:
        from app.core.database import get_db_pool
        pool = get_db_pool()
        rows = await pool.fetch(
            "SELECT symbol, name_zh FROM stock_symbols "
            "WHERE market IN ('sh','sz') ORDER BY symbol"
        )
    except Exception as e:
        logger.error("[StockProfile] Failed to load CN symbols from DB: %s", e)
        return []

    if not rows:
        logger.warning("[StockProfile] No CN symbols in DB")
        return []

    symbols = [(r["symbol"], r["name_zh"] or "") for r in rows]
    logger.info("[StockProfile] Loaded %d CN symbols from DB", len(symbols))

    concepts_dict: Dict[str, List[str]] = {}
    try:
        cm, _ = await collect_concept_mapping()
        if cm:
            concepts_dict = cm
            logger.info("[StockProfile] Concept mapping loaded: %d stocks", len(cm))
    except Exception as e:
        logger.warning("[StockProfile] Concept mapping failed (continuing without): %s", e)

    profiles: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(10)
    errors = 0
    streak_429 = 0
    abort_event = asyncio.Event()

    async def fetch_profile(symbol: str, name_zh: str) -> None:
        nonlocal errors, streak_429
        if abort_event.is_set():
            return
        async with sem:
            if abort_event.is_set():
                return
            code = symbol.split(".")[0]
            for attempt in range(3):
                try:
                    info = await run_in_executor(
                        lambda s=symbol: _fetch_yahoo_profile_sync(s),
                        timeout=30.0,
                    )
                    streak_429 = 0
                    if info:
                        _, market = _cn_code_to_symbol(code)
                        profiles.append({
                            "symbol": symbol,
                            "market": market,
                            "name": info["name"],
                            "name_zh": name_zh or info["name"],
                            "sector": info["sector"],
                            "industry": info["industry"],
                            "concepts": concepts_dict.get(code, []),
                            "main_business": info["description"],
                            "description": info["description"],
                        })
                    break
                except Exception as e:
                    if _is_rate_limit_error(e):
                        streak_429 += 1
                        if streak_429 >= _MAX_429_STREAK:
                            logger.error("[StockProfile] Too many 429s, aborting CN")
                            abort_event.set()
                            return
                        await asyncio.sleep(30 * (attempt + 1))
                        continue
                    errors += 1
                    if errors <= 10:
                        logger.warning("[StockProfile] CN error for %s: %s", symbol, e)
                    break
            await asyncio.sleep(0.3)

    batch_size = 20
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        await asyncio.gather(
            *[fetch_profile(sym, nzh) for sym, nzh in batch],
            return_exceptions=True,
        )
        if i % 500 == 0 and i > 0:
            logger.info(
                "[StockProfile] Fetched %d/%d CN profiles (%d errors)",
                len(profiles), i, errors,
            )

    elapsed = time.monotonic() - t0
    logger.info(
        "[StockProfile] CN collection complete: %d profiles in %.0fs (errors=%d)",
        len(profiles), elapsed, errors,
    )
    return profiles


# ---------------------------------------------------------------------------
# US profiles: Yahoo quoteSummary (assetProfile + quoteType)
# ---------------------------------------------------------------------------

async def collect_us_profiles(symbols: List[str]) -> List[Dict[str, Any]]:
    """Collect US stock profiles via Yahoo quoteSummary (1 HTTP req per symbol)."""
    logger.info("[StockProfile] Starting US profile collection: %d symbols", len(symbols))
    t0 = time.monotonic()

    profiles: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(10)
    errors = 0
    streak_429 = 0
    abort_event = asyncio.Event()

    async def fetch_profile(symbol: str) -> None:
        nonlocal errors, streak_429
        if abort_event.is_set():
            return
        async with sem:
            if abort_event.is_set():
                return
            for attempt in range(3):
                try:
                    info = await run_in_executor(
                        lambda s=symbol: _fetch_yahoo_profile_sync(s),
                        timeout=30.0,
                    )
                    streak_429 = 0
                    if info:
                        profiles.append({
                            "symbol": symbol,
                            "market": "us",
                            "name": info["name"],
                            "name_zh": "",
                            "sector": info["sector"],
                            "industry": info["industry"],
                            "concepts": [],
                            "main_business": info["description"],
                            "description": info["description"],
                        })
                    break
                except Exception as e:
                    if _is_rate_limit_error(e):
                        streak_429 += 1
                        if streak_429 >= _MAX_429_STREAK:
                            logger.error("[StockProfile] Too many 429s, aborting US")
                            abort_event.set()
                            return
                        await asyncio.sleep(30 * (attempt + 1))
                        continue
                    errors += 1
                    if errors <= 10:
                        logger.warning("[StockProfile] US error for %s: %s", symbol, e)
                    break
            await asyncio.sleep(0.3)

    batch_size = 20
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        await asyncio.gather(
            *[fetch_profile(s) for s in batch],
            return_exceptions=True,
        )
        if i % 500 == 0 and i > 0:
            logger.info(
                "[StockProfile] Fetched %d/%d US profiles (%d errors)",
                len(profiles), i, errors,
            )

    elapsed = time.monotonic() - t0
    logger.info(
        "[StockProfile] US collection complete: %d profiles in %.0fs (errors=%d)",
        len(profiles), elapsed, errors,
    )
    return profiles


# ---------------------------------------------------------------------------
# HK profiles: Yahoo quoteSummary (assetProfile + quoteType)
# ---------------------------------------------------------------------------

async def collect_hk_profiles(symbols: List[str]) -> List[Dict[str, Any]]:
    """Collect HK stock profiles via Yahoo quoteSummary (1 HTTP req per symbol)."""
    logger.info("[StockProfile] Starting HK profile collection for %d symbols", len(symbols))
    t0 = time.monotonic()

    # yfinance uses 4-digit HK codes (0700.HK), stock list uses 5-digit (00700.HK)
    yf_to_canonical: Dict[str, str] = {}
    yf_symbols: List[str] = []
    for s in symbols:
        code, _, suffix = s.partition(".")
        yf_code = code.lstrip("0").zfill(4)
        yf_sym = f"{yf_code}.{suffix}" if suffix else yf_code
        yf_to_canonical[yf_sym] = s
        yf_symbols.append(yf_sym)

    profiles: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(5)
    errors = 0
    streak_429 = 0
    abort_event = asyncio.Event()

    async def fetch_profile(yf_symbol: str) -> None:
        nonlocal errors, streak_429
        if abort_event.is_set():
            return
        async with sem:
            if abort_event.is_set():
                return
            for attempt in range(3):
                try:
                    info = await run_in_executor(
                        lambda s=yf_symbol: _fetch_yahoo_profile_sync(s),
                        timeout=30.0,
                    )
                    streak_429 = 0
                    if info:
                        canonical = yf_to_canonical.get(yf_symbol, yf_symbol)
                        profiles.append({
                            "symbol": canonical,
                            "market": "hk",
                            "name": info["name"],
                            "name_zh": "",
                            "sector": info["sector"],
                            "industry": info["industry"],
                            "concepts": [],
                            "main_business": info["description"],
                            "description": info["description"],
                        })
                    break
                except Exception as e:
                    if _is_rate_limit_error(e):
                        streak_429 += 1
                        if streak_429 >= _MAX_429_STREAK:
                            logger.error("[StockProfile] Too many 429s, aborting HK")
                            abort_event.set()
                            return
                        await asyncio.sleep(30 * (attempt + 1))
                        continue
                    errors += 1
                    if errors <= 10:
                        logger.warning("[StockProfile] HK error for %s: %s", yf_symbol, e)
                    break
            await asyncio.sleep(0.5)

    batch_size = 10
    for i in range(0, len(yf_symbols), batch_size):
        batch = yf_symbols[i : i + batch_size]
        await asyncio.gather(
            *[fetch_profile(s) for s in batch],
            return_exceptions=True,
        )
        if i % 100 == 0 and i > 0:
            logger.info(
                "[StockProfile] Fetched %d/%d HK profiles (%d errors)",
                len(profiles), i, errors,
            )

    elapsed = time.monotonic() - t0
    logger.info(
        "[StockProfile] HK collection complete: %d profiles in %.0fs (errors=%d)",
        len(profiles), elapsed, errors,
    )
    return profiles


# ===========================================================================
# Granular batch functions (new) -- max 50 symbols per call, <60s each
# ===========================================================================


async def fetch_cn_stock_info_batch(
    codes: List[str],
) -> List[Dict[str, Any]]:
    """Fetch individual stock info for a small batch of A-share codes.

    Unlike ``collect_cn_profiles()``, this does NOT perform concept board
    mapping. The caller is expected to merge concept data separately.

    Args:
        codes: List of 6-digit A-share codes (max 50).

    Returns:
        List of profile dicts (without concepts -- caller adds them).
    """
    import akshare as ak

    codes = codes[:50]
    logger.info("[StockProfile] Fetching CN stock info batch: %d codes", len(codes))
    t0 = time.monotonic()

    profiles: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(3)
    errors = 0

    async def fetch_one(code: str) -> None:
        nonlocal errors
        async with sem:
            symbol, market = _cn_code_to_symbol(code)
            profile: Dict[str, Any] = {
                "symbol": symbol,
                "market": market,
                "name": "",
                "name_zh": "",
                "sector": "",
                "industry": "",
                "concepts": [],
                "main_business": "",
                "description": "",
            }

            for attempt in range(2):
                try:
                    df = await run_in_executor(
                        lambda c=code: ak.stock_individual_info_em(symbol=c),
                        timeout=30.0,
                    )
                    if df is not None and not df.empty:
                        info: Dict[str, str] = {}
                        for _, row in df.iterrows():
                            info[row["item"]] = row["value"]
                        biz_scope = str(info.get("\u7ecf\u8425\u8303\u56f4", ""))[:500]
                        profile["main_business"] = biz_scope
                        profile["description"] = biz_scope
                        profile["industry"] = str(info.get("\u884c\u4e1a", ""))
                        profile["sector"] = str(info.get("\u884c\u4e1a", ""))
                        profile["name"] = str(info.get("\u80a1\u7968\u7b80\u79f0", ""))
                        profile["name_zh"] = profile["name"]
                    break
                except Exception as e:
                    if _is_rate_limit_error(e) and attempt < 1:
                        await asyncio.sleep(30)
                        continue
                    errors += 1
                    if errors <= 5:
                        logger.warning(
                            "[StockProfile] Error fetching info for %s: %s",
                            code, e,
                        )
                    break

            profiles.append(profile)
            await asyncio.sleep(1.0)

    await asyncio.gather(
        *[fetch_one(c) for c in codes],
        return_exceptions=True,
    )

    elapsed = time.monotonic() - t0
    logger.info(
        "[StockProfile] CN batch info done: %d profiles in %.1fs (errors=%d)",
        len(profiles), elapsed, errors,
    )
    return profiles


async def fetch_us_profiles_batch(
    symbols: List[str],
) -> List[Dict[str, Any]]:
    """Fetch US stock profiles for a small batch via yfinance.

    Args:
        symbols: List of US stock symbols (max 50).

    Returns:
        List of profile dicts.
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("[StockProfile] yfinance not installed")
        return []

    symbols = symbols[:50]
    logger.info("[StockProfile] Fetching US profiles batch: %d symbols", len(symbols))
    t0 = time.monotonic()

    profiles: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(5)
    errors = 0

    async def fetch_one(symbol: str) -> None:
        nonlocal errors
        async with sem:
            for attempt in range(2):
                try:
                    def _get_info(s: str = symbol) -> Optional[Dict[str, Any]]:
                        ticker = yf.Ticker(s)
                        return ticker.info

                    info = await run_in_executor(_get_info, timeout=30.0)
                    if info and isinstance(info, dict):
                        name = info.get("shortName") or info.get("longName", "")
                        if name:
                            biz_summary = info.get("longBusinessSummary", "")[:500]
                            profiles.append({
                                "symbol": symbol,
                                "market": "us",
                                "name": name,
                                "name_zh": "",
                                "sector": info.get("sector", ""),
                                "industry": info.get("industry", ""),
                                "concepts": [],
                                "main_business": biz_summary,
                                "description": biz_summary,
                            })
                    break
                except Exception as e:
                    if _is_rate_limit_error(e) and attempt < 1:
                        await asyncio.sleep(30)
                        continue
                    errors += 1
                    if errors <= 5:
                        logger.warning(
                            "[StockProfile] yfinance error for %s: %s",
                            symbol, e,
                        )
                    break
            await asyncio.sleep(0.5)

    await asyncio.gather(
        *[fetch_one(s) for s in symbols],
        return_exceptions=True,
    )

    elapsed = time.monotonic() - t0
    logger.info(
        "[StockProfile] US batch done: %d profiles in %.1fs (errors=%d)",
        len(profiles), elapsed, errors,
    )
    return profiles


async def fetch_hk_profiles_batch(
    symbols: List[str],
) -> List[Dict[str, Any]]:
    """Fetch HK stock profiles for a small batch via yfinance.

    Args:
        symbols: List of HK stock symbols in canonical format e.g. ["00700.HK"]
                 (max 50).

    Returns:
        List of profile dicts (with canonical 5-digit symbols).
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("[StockProfile] yfinance not installed")
        return []

    symbols = symbols[:50]
    logger.info("[StockProfile] Fetching HK profiles batch: %d symbols", len(symbols))
    t0 = time.monotonic()

    # yfinance uses 4-digit HK codes (0700.HK), stock list uses 5-digit (00700.HK)
    yf_to_canonical: Dict[str, str] = {}
    yf_symbols: List[str] = []
    for s in symbols:
        code, _, suffix = s.partition(".")
        yf_code = code.lstrip("0").zfill(4)
        yf_sym = f"{yf_code}.{suffix}" if suffix else yf_code
        yf_to_canonical[yf_sym] = s
        yf_symbols.append(yf_sym)

    profiles: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(5)
    errors = 0

    async def fetch_one(yf_symbol: str) -> None:
        nonlocal errors
        async with sem:
            for attempt in range(2):
                try:
                    def _get_info(s: str = yf_symbol) -> Optional[Dict[str, Any]]:
                        ticker = yf.Ticker(s)
                        return ticker.info

                    info = await run_in_executor(_get_info, timeout=30.0)
                    if info and isinstance(info, dict):
                        name = info.get("shortName") or info.get("longName", "")
                        if name:
                            canonical = yf_to_canonical.get(yf_symbol, yf_symbol)
                            biz_summary = info.get("longBusinessSummary", "")[:500]
                            profiles.append({
                                "symbol": canonical,
                                "market": "hk",
                                "name": name,
                                "name_zh": "",
                                "sector": info.get("sector", ""),
                                "industry": info.get("industry", ""),
                                "concepts": [],
                                "main_business": biz_summary,
                                "description": biz_summary,
                            })
                    break
                except Exception as e:
                    if _is_rate_limit_error(e) and attempt < 1:
                        await asyncio.sleep(30)
                        continue
                    errors += 1
                    if errors <= 5:
                        logger.warning(
                            "[StockProfile] yfinance error for %s: %s",
                            yf_symbol, e,
                        )
                    break
            await asyncio.sleep(0.5)

    await asyncio.gather(
        *[fetch_one(s) for s in yf_symbols],
        return_exceptions=True,
    )

    elapsed = time.monotonic() - t0
    logger.info(
        "[StockProfile] HK batch done: %d profiles in %.1fs (errors=%d)",
        len(profiles), elapsed, errors,
    )
    return profiles
