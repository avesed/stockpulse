"""Fetch daily OHLCV bars from external providers (yfinance, akshare).

CN market uses akshare (ak.stock_zh_a_hist) with per-symbol concurrent fetches.
US/HK/Metal markets use yfinance batch download (yf.download), grouped by
start_date since all symbols in a single call must share the same start.

This service is stateless -- it only fetches data from external APIs and returns
it.  Persistence is handled by the backend's DailyBarService.
"""
from __future__ import annotations

import asyncio
import logging
import threading
from collections import defaultdict
from datetime import date
from typing import Any, Optional

from app.core.executor import run_in_background_executor as run_in_executor
from app.providers.constants import detect_market, normalize_symbol

logger = logging.getLogger(__name__)

# CN: conservative concurrency to avoid Eastmoney rate limiting
_CN_MAX_CONCURRENT = 12
_CN_FETCH_TIMEOUT = 60.0  # seconds per symbol

# yfinance batch download timeout (50 symbols can be slow on first fetch)
_YF_BATCH_TIMEOUT = 120.0  # seconds per batch

# yfinance.shared._DFS is a global dict that is not thread-safe for concurrent
# downloads.  Serialize all yf.download() calls with this lock.
_yf_download_lock = threading.Lock()


class DailyBarFetcher:
    """Fetches daily OHLCV bars from external providers."""

    async def fetch_batch(
        self,
        symbols_with_dates: list[dict],
        market: str,
    ) -> tuple[dict[str, dict], dict[str, str]]:
        """Fetch daily bars for a batch of symbols.

        Args:
            symbols_with_dates: List of dicts, each with keys:
                - "symbol": str (e.g. "AAPL", "600519.SS", "0700.HK", "GC=F")
                - "start_date": Optional[str] in YYYY-MM-DD format.  None = full history.
            market: One of "us", "hk", "cn", "metal".

        Returns:
            Tuple of (results, errors) where:
            - results: {symbol: {"bars": [{"date","open","high","low","close","volume"}, ...], "source": str}}
            - errors: {symbol: "error message"}
        """
        if not symbols_with_dates:
            return {}, {}

        logger.info(
            "fetch_batch: market=%s, symbols=%d",
            market, len(symbols_with_dates),
        )

        if market == "cn":
            return await self._fetch_cn_batch(symbols_with_dates)
        else:
            return await self._fetch_yf_batch(symbols_with_dates)

    # ------------------------------------------------------------------
    # CN path (akshare)
    # ------------------------------------------------------------------

    async def _fetch_cn_batch(
        self,
        symbols_with_dates: list[dict],
    ) -> tuple[dict[str, dict], dict[str, str]]:
        """Fetch CN daily bars using akshare (one call per symbol, concurrent).

        akshare does not support multi-symbol downloads, so we fetch each
        symbol individually with a semaphore to bound concurrency.
        """
        results: dict[str, dict] = {}
        errors: dict[str, str] = {}
        semaphore = asyncio.Semaphore(_CN_MAX_CONCURRENT)

        async def _fetch_one(entry: dict) -> None:
            symbol = entry["symbol"]
            start_date = entry.get("start_date")  # YYYY-MM-DD or None
            async with semaphore:
                try:
                    bars = await self._fetch_cn_symbol(symbol, start_date)
                    if bars is not None:
                        results[symbol] = {"bars": bars, "source": "akshare"}
                    else:
                        errors[symbol] = "No data returned from akshare"
                except asyncio.TimeoutError:
                    msg = f"Timeout after {_CN_FETCH_TIMEOUT}s"
                    logger.warning("CN fetch timeout: %s", symbol)
                    errors[symbol] = msg
                except Exception as exc:
                    logger.warning("CN fetch error for %s: %s", symbol, exc)
                    errors[symbol] = str(exc)

        await asyncio.gather(*[_fetch_one(e) for e in symbols_with_dates])

        logger.info(
            "CN batch complete: %d results, %d errors",
            len(results), len(errors),
        )
        return results, errors

    async def _fetch_cn_symbol(
        self,
        symbol: str,
        start_date: Optional[str],
    ) -> Optional[list[dict]]:
        """Fetch daily bars for a single CN symbol via akshare.

        Args:
            symbol: Full symbol with suffix (e.g. "600519.SS", "000001.SZ").
            start_date: YYYY-MM-DD string or None for full history.

        Returns:
            List of bar dicts, or None on failure.
        """
        detected_market = detect_market(symbol)
        code = normalize_symbol(symbol, detected_market)
        today = date.today()

        if start_date is None:
            start_str = "19900101"
        else:
            # start_date is already the first date to fetch (inclusive).
            # The backend handles the +1 day offset from last_date.
            try:
                parsed = date.fromisoformat(start_date)
            except ValueError:
                logger.warning("Invalid start_date '%s' for %s, using full history", start_date, symbol)
                parsed = None

            if parsed is not None:
                if parsed >= today:
                    logger.info("Symbol %s already up to date (start_date=%s)", symbol, start_date)
                    return []
                start_str = parsed.strftime("%Y%m%d")
            else:
                start_str = "19900101"

        end_str = today.strftime("%Y%m%d")

        def _do_fetch() -> Any:
            import akshare as ak

            return ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_str,
                end_date=end_str,
                adjust="qfq",
            )

        df = await run_in_executor(_do_fetch, timeout=_CN_FETCH_TIMEOUT)

        if df is None or df.empty:
            logger.info("Empty akshare response for %s (code=%s)", symbol, code)
            return []

        bars: list[dict] = []
        for _, row in df.iterrows():
            date_val = row.get("\u65e5\u671f")  # 日期
            if date_val is None:
                continue
            bars.append({
                "date": str(date_val)[:10],
                "open": float(row.get("\u5f00\u76d8") or 0),   # 开盘
                "high": float(row.get("\u6700\u9ad8") or 0),   # 最高
                "low": float(row.get("\u6700\u4f4e") or 0),    # 最低
                "close": float(row.get("\u6536\u76d8") or 0),  # 收盘
                "volume": int(row.get("\u6210\u4ea4\u91cf") or 0),  # 成交量
            })

        # Safety dedup: drop bars before start_date (start_date is inclusive)
        if start_date is not None:
            bars = [b for b in bars if b["date"] >= start_date]

        logger.info("Fetched %d bars for %s (code=%s)", len(bars), symbol, code)
        return bars

    # ------------------------------------------------------------------
    # US / HK / Metal path (yfinance)
    # ------------------------------------------------------------------

    async def _fetch_yf_batch(
        self,
        symbols_with_dates: list[dict],
    ) -> tuple[dict[str, dict], dict[str, str]]:
        """Fetch daily bars using yfinance batch download.

        yfinance.download() requires all symbols in a single call to share the
        same start date.  We group symbols by their start_date, then issue one
        yf.download() per group.
        """
        results: dict[str, dict] = {}
        errors: dict[str, str] = {}
        today = date.today()

        # Group by start_date.  Key is the start string for yf.download().
        # Each value is a list of (symbol, original_start_date_str) tuples.
        date_groups: dict[str, list[tuple[str, Optional[str]]]] = defaultdict(list)
        up_to_date: list[str] = []

        for entry in symbols_with_dates:
            symbol = entry["symbol"]
            start_date = entry.get("start_date")  # YYYY-MM-DD or None

            if start_date is None:
                # Full history -- use a far-past date
                date_groups["1970-01-01"].append((symbol, None))
            else:
                # start_date is already the first date to fetch (inclusive).
                # The backend handles the +1 day offset from last_date.
                try:
                    parsed = date.fromisoformat(start_date)
                except ValueError:
                    logger.warning("Invalid start_date '%s' for %s, using full history", start_date, symbol)
                    date_groups["1970-01-01"].append((symbol, None))
                    continue

                if parsed >= today:
                    up_to_date.append(symbol)
                    results[symbol] = {"bars": [], "source": "yfinance"}
                    continue
                date_groups[start_date].append((symbol, start_date))

        if up_to_date:
            logger.info("Skipped %d already-up-to-date symbols", len(up_to_date))

        logger.info(
            "yfinance: %d date groups, %d symbols to fetch",
            len(date_groups),
            sum(len(g) for g in date_groups.values()),
        )

        # Download each group
        for start_str, group in date_groups.items():
            group_symbols = [sym for sym, _ in group]
            start_dates_map = {sym: sd for sym, sd in group}

            try:
                group_results, group_errors = await self._yf_download_group(
                    group_symbols, start_str, start_dates_map,
                )
                results.update(group_results)
                errors.update(group_errors)
            except Exception as exc:
                logger.error(
                    "yfinance group download failed (start=%s, size=%d): %s",
                    start_str, len(group_symbols), exc,
                )
                for sym in group_symbols:
                    errors[sym] = f"Batch download error: {exc}"

        logger.info(
            "yfinance batch complete: %d results, %d errors",
            len(results), len(errors),
        )
        return results, errors

    async def _yf_download_group(
        self,
        symbols: list[str],
        start_str: str,
        start_dates_map: dict[str, Optional[str]],
    ) -> tuple[dict[str, dict], dict[str, str]]:
        """Download a group of symbols sharing the same start_date via yfinance.

        Args:
            symbols: List of symbols to download.
            start_str: YYYY-MM-DD start date for yf.download().
            start_dates_map: {symbol: original_start_date_str} for dedup filtering.

        Returns:
            Tuple of (results, errors).
        """
        import pandas as pd

        def _do_download() -> Any:
            import yfinance as yf

            with _yf_download_lock:
                return yf.download(
                    symbols,
                    start=start_str,
                    auto_adjust=True,
                    progress=False,
                )

        logger.info(
            "yfinance download: %d symbols, start=%s",
            len(symbols), start_str,
        )

        df = await run_in_executor(_do_download, timeout=_YF_BATCH_TIMEOUT)

        if df is None or df.empty:
            logger.warning(
                "Empty yfinance response: %d symbols, start=%s",
                len(symbols), start_str,
            )
            return {}, {sym: "Empty response from yfinance" for sym in symbols}

        is_multi = isinstance(df.columns, pd.MultiIndex)
        results: dict[str, dict] = {}
        errors: dict[str, str] = {}

        for sym in symbols:
            try:
                if is_multi:
                    sym_df = df[
                        [("Open", sym), ("High", sym), ("Low", sym),
                         ("Close", sym), ("Volume", sym)]
                    ].copy()
                    sym_df.columns = ["open", "high", "low", "close", "volume"]
                else:
                    # Single-symbol download produces flat columns
                    sym_df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
                    sym_df.columns = ["open", "high", "low", "close", "volume"]

                # Drop rows with missing or zero close price
                sym_df = sym_df.dropna(subset=["close"])
                sym_df = sym_df[sym_df["close"] > 0]

                # Dedup: only keep bars on or after the symbol's start_date (inclusive)
                original_start = start_dates_map.get(sym)
                if original_start is not None:
                    try:
                        cutoff = date.fromisoformat(original_start)
                        sym_df = sym_df[sym_df.index.date >= cutoff]
                    except (ValueError, AttributeError):
                        pass

                bars = [
                    {
                        "date": str(idx.date()),
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": int(row["volume"]),
                    }
                    for idx, row in sym_df.iterrows()
                ]

                results[sym] = {"bars": bars, "source": "yfinance"}

                if bars:
                    logger.info("Fetched %d bars for %s", len(bars), sym)

            except KeyError:
                # Symbol not present in batch response (delisted / invalid)
                logger.debug("Symbol %s absent from yfinance batch response", sym)
                errors[sym] = "Symbol not found in yfinance response"
            except Exception as exc:
                logger.warning("Failed to extract bars for %s: %s", sym, exc)
                errors[sym] = str(exc)

        return results, errors
