"""AKShare data provider for A-shares, HK stocks, and institutional data.

Migrated from backend/app/services/providers/akshare.py.
Uses the shared executor and cache helpers instead of per-provider ThreadPool/Redis.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from app.core.redis import cache_get, cache_set, jittered_ttl
from app.core.executor import run_in_executor
from app.providers.base import DataProvider
from app.providers.constants import HK, SH, SZ, normalize_symbol

logger = logging.getLogger(__name__)

# Cache TTL configurations (base_seconds, jitter_seconds)
CACHE_TTL = {
    "fund_holdings": (86400, 3600),  # 24h + rand(1h)
    "northbound_holding": (3600, 600),  # 1h + rand(10min)
    "northbound_flow": (3600, 600),  # 1h + rand(10min)
    "industry_sector_list": (300, 60),  # 5min + rand(1min)
    "stock_industry_cn": (86400, 3600),  # 24h + rand(1h)
    "sector_history": (300, 60),  # 5min + rand(1min)
    "hk_history": (300, 60),  # 5min + rand(1min)
}

# Intraday interval strings
_INTRADAY_INTERVALS = {"1m", "2m", "5m", "15m", "30m", "1h"}

# AKShare intraday period mapping (interval string -> AKShare period string)
_INTRADAY_MAP = {
    "1m": "1",
    "2m": "1",  # AKShare doesn't support 2m; fallback to 1m
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
}

# Period string -> approximate days
_PERIOD_DAYS = {
    "1d": 1,
    "5d": 5,
    "1mo": 30,
    "3mo": 90,
    "6mo": 180,
    "1y": 365,
    "2y": 730,
    "5y": 1825,
    "max": 3650,
}


def _ttl(data_type: str) -> int:
    """Get jittered TTL for a data type."""
    base, jitter = CACHE_TTL.get(data_type, (3600, 300))
    return jittered_ttl(base, jitter)


class AKShareProvider(DataProvider):
    """AKShare data provider for A-shares and HK stocks.

    Primary provider for:
    - A-shares (Shanghai, Shenzhen)
    - HK stocks

    Also provides institutional data:
    - Fund holdings (A-shares)
    - Northbound capital flow
    - Industry sector data
    """

    @property
    def name(self) -> str:
        return "akshare"

    @property
    def supported_markets(self) -> Set[str]:
        return {SH, SZ, HK}

    # ------------------------------------------------------------------
    # Helper: cached fetch
    # ------------------------------------------------------------------
    async def _cached_or_fetch(
        self,
        data_type: str,
        identifier: str,
        fetch_func,
    ) -> Optional[Dict[str, Any]]:
        """Get data from cache or fetch from source."""
        cache_key = f"akshare:{data_type}:{identifier}"
        cached = await cache_get(cache_key)
        if cached is not None:
            logger.debug("Cache hit: %s", cache_key)
            return cached

        try:
            data = await fetch_func()
            if data:
                await cache_set(cache_key, data, ttl=_ttl(data_type))
                logger.debug("Cached: %s", cache_key)
            return data
        except Exception as e:
            logger.error("Fetch error for %s/%s: %s", data_type, identifier, e)
            return None

    # === Core Methods ===

    async def get_quote(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get real-time quote from akshare."""
        if market == HK:
            return await self._get_quote_hk(symbol)
        elif market in (SH, SZ):
            return await self._get_quote_cn(symbol, market)
        return None

    async def _get_quote_cn(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get real-time quote for A-shares."""
        try:
            import akshare as ak

            code = normalize_symbol(symbol, market)

            def fetch():
                df = ak.stock_zh_a_spot_em()
                row = df[df["\u4ee3\u7801"] == code]
                if row.empty:
                    return None
                return row.iloc[0].to_dict()

            data = await run_in_executor(fetch)
            if not data:
                return None

            price = float(data.get("\u6700\u65b0\u4ef7", 0))
            change = float(data.get("\u6da8\u8dcc\u989d", 0))
            change_pct = float(data.get("\u6da8\u8dcc\u5e45", 0))

            return {
                "symbol": symbol,
                "name": data.get("\u540d\u79f0"),
                "price": price,
                "change": round(change, 4),
                "change_percent": round(change_pct, 2),
                "volume": int(data.get("\u6210\u4ea4\u91cf", 0)),
                "market_cap": (
                    float(data.get("\u603b\u5e02\u503c", 0))
                    if data.get("\u603b\u5e02\u503c")
                    else None
                ),
                "high": (
                    float(data.get("\u6700\u9ad8", 0))
                    if data.get("\u6700\u9ad8")
                    else None
                ),
                "low": (
                    float(data.get("\u6700\u4f4e", 0))
                    if data.get("\u6700\u4f4e")
                    else None
                ),
                "open": (
                    float(data.get("\u4eca\u5f00", 0))
                    if data.get("\u4eca\u5f00")
                    else None
                ),
                "prev_close": (
                    float(data.get("\u6628\u6536", 0))
                    if data.get("\u6628\u6536")
                    else None
                ),
                "timestamp": datetime.utcnow().isoformat(),
                "market": market,
                "currency": "CNY",
                "source": "akshare",
            }
        except Exception as e:
            logger.error("AKShare CN quote error for %s: %s", symbol, e)
            return None

    async def _get_quote_hk(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get real-time quote for HK stocks.

        Uses stock_individual_spot_xq (Xueqiu) for fast per-symbol lookup
        instead of stock_hk_spot_em which downloads the entire HK market.
        """
        try:
            import akshare as ak

            code = normalize_symbol(symbol, HK)

            def fetch():
                df = ak.stock_individual_spot_xq(symbol=code)
                if df is None or df.empty:
                    return None
                return dict(zip(df["item"], df["value"]))

            data = await run_in_executor(fetch)
            if not data:
                return None

            price = float(data.get("\u73b0\u4ef7", 0))
            change = float(data.get("\u6da8\u8dcc", 0))
            change_pct = float(data.get("\u6da8\u5e45", 0))

            return {
                "symbol": symbol,
                "name": data.get("\u540d\u79f0"),
                "price": price,
                "change": round(change, 4),
                "change_percent": round(change_pct, 2),
                "volume": int(data.get("\u6210\u4ea4\u91cf", 0)),
                "market_cap": (
                    float(data.get("\u8d44\u4ea7\u51c0\u503c/\u603b\u5e02\u503c", 0))
                    if data.get("\u8d44\u4ea7\u51c0\u503c/\u603b\u5e02\u503c")
                    else None
                ),
                "high": (
                    float(data.get("\u6700\u9ad8", 0))
                    if data.get("\u6700\u9ad8")
                    else None
                ),
                "low": (
                    float(data.get("\u6700\u4f4e", 0))
                    if data.get("\u6700\u4f4e")
                    else None
                ),
                "open": (
                    float(data.get("\u4eca\u5f00", 0))
                    if data.get("\u4eca\u5f00")
                    else None
                ),
                "prev_close": (
                    float(data.get("\u6628\u6536", 0))
                    if data.get("\u6628\u6536")
                    else None
                ),
                "timestamp": datetime.utcnow().isoformat(),
                "market": HK,
                "currency": "HKD",
                "source": "akshare",
            }
        except Exception as e:
            logger.error("AKShare HK quote error for %s: %s", symbol, e)
            return None

    async def get_history(
        self,
        symbol: str,
        market: str,
        period: str,
        interval: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get historical data from akshare."""
        if market == HK:
            return await self._get_history_hk(
                symbol, period, interval, start=start, end=end
            )
        elif market in (SH, SZ):
            return await self._get_history_cn(
                symbol, market, period, interval, start=start, end=end
            )
        return None

    async def _get_history_cn(
        self,
        symbol: str,
        market: str,
        period: str,
        interval: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get historical data for A-shares.

        Uses stock_zh_a_hist_min_em (Eastmoney) for intraday intervals
        and stock_zh_a_hist for daily/weekly/monthly.
        """
        try:
            import akshare as ak

            code = normalize_symbol(symbol, market)

            # Intraday intervals
            intraday_period = _INTRADAY_MAP.get(interval)
            if interval == "2m":
                logger.info(
                    "AKShare CN: 2m interval not supported, falling back to 1m for %s",
                    symbol,
                )

            if intraday_period is not None:
                def fetch_minute():
                    kwargs = {
                        "symbol": code,
                        "period": intraday_period,
                        "adjust": "qfq",
                    }
                    if start and end:
                        kwargs["start_date"] = start.replace("T", " ")[:19]
                        kwargs["end_date"] = end.replace("T", " ")[:19]
                        logger.info(
                            "AKShare CN intraday for %s: start=%s, end=%s, period=%s",
                            symbol, start, end, intraday_period,
                        )
                    return ak.stock_zh_a_hist_min_em(**kwargs)

                df = await run_in_executor(fetch_minute)
                if df is None or df.empty:
                    logger.info(
                        "AKShare CN intraday returned no data for %s (period=%s)",
                        symbol, intraday_period,
                    )
                    return None

                # Column names from stock_zh_a_hist_min_em are Chinese
                ohlc_cols = [
                    "\u5f00\u76d8", "\u6700\u9ad8",
                    "\u6700\u4f4e", "\u6536\u76d8",
                ]
                available_ohlc = [c for c in ohlc_cols if c in df.columns]
                if available_ohlc:
                    df = df.dropna(subset=available_ohlc)

                bars = []
                for _, row in df.iterrows():
                    date_val = row["\u65f6\u95f4"]
                    if isinstance(date_val, str):
                        date_val = datetime.strptime(
                            date_val, "%Y-%m-%d %H:%M:%S"
                        )
                    bars.append({
                        "date": date_val.isoformat(),
                        "open": round(float(row["\u5f00\u76d8"]), 4),
                        "high": round(float(row["\u6700\u9ad8"]), 4),
                        "low": round(float(row["\u6700\u4f4e"]), 4),
                        "close": round(float(row["\u6536\u76d8"]), 4),
                        "volume": int(row["\u6210\u4ea4\u91cf"]),
                    })

                logger.info(
                    "AKShare CN intraday for %s: %d bars returned",
                    symbol, len(bars),
                )

                # When no start/end provided, trim by period
                if bars and not (start and end):
                    period_days_map = {"1d": 1, "5d": 5}
                    max_days = period_days_map.get(period, 5)
                    cutoff = datetime.now() - timedelta(days=max_days + 1)
                    cutoff_iso = cutoff.isoformat()
                    bars = [b for b in bars if b["date"] >= cutoff_iso]

                if not bars:
                    return None

                return {
                    "symbol": symbol,
                    "interval": interval,
                    "bars": bars,
                    "market": market,
                    "source": "akshare",
                }

            # Daily/weekly/monthly: use ak.stock_zh_a_hist()
            ak_period = {
                "1d": "daily",
                "1wk": "weekly",
                "1mo": "monthly",
            }.get(interval, "daily")

            # Determine date range
            if start and end:
                fmt_start = start.replace("-", "")[:8]
                fmt_end = end.replace("-", "")[:8]
                logger.info(
                    "AKShare CN daily for %s: start=%s, end=%s",
                    symbol, fmt_start, fmt_end,
                )
            else:
                end_date = datetime.now()
                days = _PERIOD_DAYS.get(period, 365)
                start_date = end_date - timedelta(days=days)
                fmt_start = start_date.strftime("%Y%m%d")
                fmt_end = end_date.strftime("%Y%m%d")

            def fetch():
                df = ak.stock_zh_a_hist(
                    symbol=code,
                    period=ak_period,
                    start_date=fmt_start,
                    end_date=fmt_end,
                    adjust="qfq",
                )
                return df

            df = await run_in_executor(fetch)
            if df is None or df.empty:
                logger.info("AKShare CN daily returned no data for %s", symbol)
                return None

            bars = []
            for _, row in df.iterrows():
                date_val = row["\u65e5\u671f"]
                if isinstance(date_val, str):
                    date_val = datetime.strptime(date_val, "%Y-%m-%d")
                bars.append({
                    "date": date_val.isoformat(),
                    "open": round(float(row["\u5f00\u76d8"]), 4),
                    "high": round(float(row["\u6700\u9ad8"]), 4),
                    "low": round(float(row["\u6700\u4f4e"]), 4),
                    "close": round(float(row["\u6536\u76d8"]), 4),
                    "volume": int(row["\u6210\u4ea4\u91cf"]),
                })

            return {
                "symbol": symbol,
                "interval": interval,
                "bars": bars,
                "market": market,
                "source": "akshare",
            }
        except Exception as e:
            logger.error("AKShare CN history error for %s: %s", symbol, e)
            return None

    async def _get_history_hk(
        self,
        symbol: str,
        period: str,
        interval: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get historical data for HK stocks (daily only).

        HK via AKShare only supports daily intervals; skip for intraday
        so the router can try yfinance as fallback.
        """
        if interval in _INTRADAY_INTERVALS:
            logger.info(
                "AKShare HK does not support intraday interval %s for %s, skipping",
                interval, symbol,
            )
            return None

        try:
            import akshare as ak

            code = normalize_symbol(symbol, HK)

            def fetch():
                df = ak.stock_hk_hist(
                    symbol=code, period="daily", adjust="qfq"
                )
                return df

            df = await run_in_executor(fetch)
            if df is None or df.empty:
                return None

            # Determine cutoff dates
            if start and end:
                cutoff_str = start[:10]
                cutoff = datetime.strptime(cutoff_str, "%Y-%m-%d")
                logger.info(
                    "AKShare HK history for %s: filtering start=%s, end=%s",
                    symbol, start, end,
                )
            else:
                period_days = {
                    "1mo": 30, "3mo": 90, "6mo": 180, "1y": 365,
                    "2y": 730, "5y": 1825, "max": 9999,
                }
                cutoff = datetime.now() - timedelta(
                    days=period_days.get(period, 365)
                )

            end_cutoff = None
            if start and end:
                end_cutoff = datetime.strptime(end[:10], "%Y-%m-%d") + timedelta(
                    days=1
                )

            bars = []
            for _, row in df.iterrows():
                date_val = row["\u65e5\u671f"]
                if isinstance(date_val, str):
                    date_val = datetime.strptime(date_val, "%Y-%m-%d")
                if date_val < cutoff:
                    continue
                if end_cutoff and date_val >= end_cutoff:
                    continue

                bars.append({
                    "date": date_val.isoformat(),
                    "open": round(float(row["\u5f00\u76d8"]), 4),
                    "high": round(float(row["\u6700\u9ad8"]), 4),
                    "low": round(float(row["\u6700\u4f4e"]), 4),
                    "close": round(float(row["\u6536\u76d8"]), 4),
                    "volume": int(row["\u6210\u4ea4\u91cf"]),
                })

            # Resample for weekly/monthly if needed
            if interval != "1d" and bars:
                bars = self._resample_bars(bars, interval)

            return {
                "symbol": symbol,
                "interval": interval,
                "bars": bars,
                "market": HK,
                "source": "akshare",
            }
        except Exception as e:
            logger.error("AKShare HK history error for %s: %s", symbol, e)
            return None

    def _resample_bars(
        self, bars: List[Dict[str, Any]], interval: str
    ) -> List[Dict[str, Any]]:
        """Resample daily bars to weekly or monthly."""
        if not bars:
            return bars

        data = {
            "date": [datetime.fromisoformat(b["date"]) for b in bars],
            "open": [b["open"] for b in bars],
            "high": [b["high"] for b in bars],
            "low": [b["low"] for b in bars],
            "close": [b["close"] for b in bars],
            "volume": [b["volume"] for b in bars],
        }
        df = pd.DataFrame(data)
        df.set_index("date", inplace=True)

        freq = "W" if interval == "1wk" else "ME"
        resampled = (
            df.resample(freq)
            .agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            })
            .dropna()
        )

        result = []
        for idx, row in resampled.iterrows():
            result.append({
                "date": idx.to_pydatetime().isoformat(),
                "open": round(row["open"], 4),
                "high": round(row["high"], 4),
                "low": round(row["low"], 4),
                "close": round(row["close"], 4),
                "volume": int(row["volume"]),
            })
        return result

    async def search(
        self, query: str, markets: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        """Search stocks using akshare."""
        results: List[Dict[str, Any]] = []

        if markets is None:
            markets = {SH, SZ, HK}

        if SH in markets or SZ in markets:
            cn_results = await self._search_cn(query)
            results.extend(cn_results)

        if HK in markets:
            hk_results = await self._search_hk(query)
            results.extend(hk_results)

        return results

    async def _search_cn(self, query: str) -> List[Dict[str, Any]]:
        """Search A-share stocks."""
        try:
            import akshare as ak

            def fetch():
                df = ak.stock_zh_a_spot_em()
                mask = df["\u540d\u79f0"].str.contains(
                    query, na=False
                ) | df["\u4ee3\u7801"].str.contains(query, na=False)
                return df[mask].head(20).to_dict("records")

            results = await run_in_executor(fetch)

            return [
                {
                    "symbol": (
                        r["\u4ee3\u7801"]
                        + ("." + ("SS" if r["\u4ee3\u7801"].startswith("6") else "SZ"))
                    ),
                    "name": r["\u540d\u79f0"],
                    "exchange": (
                        "SSE"
                        if r["\u4ee3\u7801"].startswith("6")
                        else "SZSE"
                    ),
                    "market": (
                        SH if r["\u4ee3\u7801"].startswith("6") else SZ
                    ),
                }
                for r in results
            ]
        except Exception as e:
            logger.error("AKShare CN search error for %s: %s", query, e)
            return []

    async def _search_hk(self, query: str) -> List[Dict[str, Any]]:
        """Search HK stocks."""
        try:
            import akshare as ak

            def fetch():
                df = ak.stock_hk_spot_em()
                mask = df["\u540d\u79f0"].str.contains(
                    query, na=False
                ) | df["\u4ee3\u7801"].str.contains(query, na=False)
                return df[mask].head(20).to_dict("records")

            results = await run_in_executor(fetch)

            return [
                {
                    "symbol": r["\u4ee3\u7801"] + ".HK",
                    "name": r["\u540d\u79f0"],
                    "exchange": "HKEX",
                    "market": HK,
                }
                for r in results
            ]
        except Exception as e:
            logger.error("AKShare HK search error for %s: %s", query, e)
            return []

    # === Optional Methods ===

    async def get_info(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get company info for A-shares."""
        if market not in (SH, SZ):
            return None

        try:
            import akshare as ak

            code = normalize_symbol(symbol, market)

            def fetch():
                df = ak.stock_individual_info_em(symbol=code)
                if df is None or df.empty:
                    return None
                info = {}
                for _, row in df.iterrows():
                    info[row["item"]] = row["value"]
                return info

            info = await run_in_executor(fetch)
            if not info:
                return None

            return {
                "symbol": symbol,
                "name": info.get("\u80a1\u7968\u7b80\u79f0", ""),
                "description": info.get("\u7ecf\u8425\u8303\u56f4"),
                "sector": info.get("\u884c\u4e1a"),
                "industry": info.get("\u884c\u4e1a"),
                "website": info.get("\u516c\u53f8\u7f51\u5740"),
                "employees": (
                    int(info.get("\u5458\u5de5\u4eba\u6570", 0))
                    if info.get("\u5458\u5de5\u4eba\u6570")
                    else None
                ),
                "market_cap": (
                    float(info.get("\u603b\u5e02\u503c", 0))
                    if info.get("\u603b\u5e02\u503c")
                    else None
                ),
                "currency": "CNY",
                "exchange": "SSE" if market == SH else "SZSE",
                "market": market,
                "source": "akshare",
            }
        except Exception as e:
            logger.error("AKShare CN info error for %s: %s", symbol, e)
            return None

    async def get_financials(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get financial data for A-shares via quarterly report indicators.

        Uses stock_financial_analysis_indicator() which provides profitability,
        growth, and balance sheet ratios from the latest quarterly report.
        Estimation/valuation fields (pe, eps, etc.) are not available from
        this API -- those are typically served from the DB-first path
        (data-processor collection via Xueqiu).
        """
        if market not in (SH, SZ):
            return None

        try:
            import akshare as ak

            code = normalize_symbol(symbol, market)
            start_year = str(datetime.now().year - 1)

            def fetch():
                df = ak.stock_financial_analysis_indicator(
                    symbol=code, start_year=start_year,
                )
                if df is None or df.empty:
                    return None
                return df.iloc[-1].to_dict()

            data = await run_in_executor(fetch)
            if not data:
                return None

            def _pct(key: str) -> Optional[float]:
                """Extract percentage field and convert to ratio."""
                v = data.get(key)
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                try:
                    return float(v) / 100.0
                except (ValueError, TypeError):
                    return None

            def _raw(key: str) -> Optional[float]:
                """Extract non-percentage field as-is."""
                v = data.get(key)
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                try:
                    return float(v)
                except (ValueError, TypeError):
                    return None

            # Compute gross_margin from cost ratio.
            gross_margin = None
            cost_ratio = _raw("\u4e3b\u8425\u4e1a\u52a1\u6210\u672c\u7387(%)")
            if cost_ratio is not None:
                gross_margin = 1.0 - cost_ratio / 100.0

            return {
                "symbol": symbol,
                "pe_ratio": None,
                "forward_pe": None,
                "eps": None,
                "dividend_yield": None,
                "dividend_rate": None,
                "book_value": None,
                "price_to_book": None,
                "revenue": None,
                "revenue_growth": _pct("\u4e3b\u8425\u4e1a\u52a1\u6536\u5165\u589e\u957f\u7387(%)"),
                "net_income": None,
                "profit_margin": _pct("\u9500\u552e\u51c0\u5229\u7387(%)"),
                "gross_margin": gross_margin,
                "operating_margin": _pct("\u8425\u4e1a\u5229\u6da6\u7387(%)"),
                "roe": _pct("\u51c0\u8d44\u4ea7\u6536\u76ca\u7387(%)"),
                "roa": _pct("\u603b\u8d44\u4ea7\u5229\u6da6\u7387(%)"),
                "debt_to_equity": None,
                "current_ratio": _raw("\u6d41\u52a8\u6bd4\u7387"),
                "eps_growth": _pct("\u51c0\u5229\u6da6\u589e\u957f\u7387(%)"),
                "payout_ratio": None,
                "market": market,
                "source": "akshare",
            }
        except Exception as e:
            logger.error("AKShare CN financials error for %s: %s", symbol, e)
            return None

    # === Extended Methods (Institutional Data) ===

    async def get_fund_holdings_cn(
        self, symbol: str, quarter: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get fund holdings for A-share stock."""
        code = symbol.replace(".SS", "").replace(".SZ", "")

        quarters_to_try: List[str] = []
        if quarter is None:
            now = datetime.now()
            year = now.year
            month = now.month
            for i in range(8):
                total_q = (year * 4 + ((month - 1) // 3)) - i - 1
                q_year = total_q // 4
                q_num = (total_q % 4) + 1
                quarters_to_try.append(f"{q_year}{q_num}")
        else:
            quarters_to_try = [quarter]

        async def fetch():
            import akshare as ak

            def _fetch_sync():
                for q in quarters_to_try:
                    try:
                        df = ak.stock_institute_hold(symbol=q)
                        if df is None or df.empty:
                            continue

                        row = df[
                            df["\u8bc1\u5238\u4ee3\u7801"] == code
                        ]
                        if row.empty:
                            continue

                        r = row.iloc[0]
                        holdings = {
                            "stock_code": r.get("\u8bc1\u5238\u4ee3\u7801"),
                            "stock_name": r.get("\u8bc1\u5238\u7b80\u79f0"),
                            "institution_count": int(
                                r.get("\u673a\u6784\u6570", 0)
                            ),
                            "institution_count_change": (
                                int(r.get("\u673a\u6784\u6570\u53d8\u5316", 0))
                                if pd.notna(
                                    r.get("\u673a\u6784\u6570\u53d8\u5316")
                                )
                                else None
                            ),
                            "holding_pct": (
                                float(r.get("\u6301\u80a1\u6bd4\u4f8b", 0))
                                if pd.notna(
                                    r.get("\u6301\u80a1\u6bd4\u4f8b")
                                )
                                else None
                            ),
                            "holding_pct_change": (
                                float(
                                    r.get(
                                        "\u6301\u80a1\u6bd4\u4f8b\u589e\u5e45",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    r.get(
                                        "\u6301\u80a1\u6bd4\u4f8b\u589e\u5e45"
                                    )
                                )
                                else None
                            ),
                            "float_pct": (
                                float(
                                    r.get(
                                        "\u5360\u6d41\u901a\u80a1\u6bd4\u4f8b",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    r.get(
                                        "\u5360\u6d41\u901a\u80a1\u6bd4\u4f8b"
                                    )
                                )
                                else None
                            ),
                            "float_pct_change": (
                                float(
                                    r.get(
                                        "\u5360\u6d41\u901a\u80a1\u6bd4\u4f8b\u589e\u5e45",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    r.get(
                                        "\u5360\u6d41\u901a\u80a1\u6bd4\u4f8b\u589e\u5e45"
                                    )
                                )
                                else None
                            ),
                        }

                        return {
                            "symbol": symbol,
                            "quarter": q,
                            "holdings": holdings,
                            "source": "akshare",
                        }
                    except Exception as e:
                        logger.warning(
                            "AKShare fund holdings error for %s: %s", q, e
                        )
                        continue

                return {
                    "symbol": symbol,
                    "quarter": (
                        quarters_to_try[0] if quarters_to_try else None
                    ),
                    "holdings": None,
                    "source": "akshare",
                    "note": f"\u80a1\u7968 {code} \u672a\u88ab\u57fa\u91d1\u6301\u4ed3\u6216\u65e0\u6570\u636e",
                }

            return await run_in_executor(_fetch_sync)

        cache_key = (
            f"{code}:{quarters_to_try[0] if quarters_to_try else 'default'}"
        )
        return await self._cached_or_fetch("fund_holdings", cache_key, fetch)

    async def get_northbound_holding(
        self, symbol: str, days: int = 30
    ) -> Optional[Dict[str, Any]]:
        """Get northbound holding for a specific A-share stock."""
        code = symbol.replace(".SS", "").replace(".SZ", "")

        async def fetch():
            import akshare as ak

            def _fetch_sync():
                try:
                    df = ak.stock_hsgt_individual_em(symbol=code)

                    if df is None or df.empty:
                        return {
                            "symbol": symbol,
                            "holdings": [],
                            "latest_holding": None,
                            "data_cutoff_notice": "\u65e0\u5317\u5411\u6301\u4ed3\u6570\u636e",
                            "source": "akshare",
                        }

                    df = df.tail(days)

                    holdings = []
                    for _, row in df.iterrows():
                        holding = {
                            "holding_date": str(
                                row.get("\u6301\u80a1\u65e5\u671f", "")
                            )[:10],
                            "close_price": (
                                float(
                                    row.get(
                                        "\u5f53\u65e5\u6536\u76d8\u4ef7", 0
                                    )
                                )
                                if pd.notna(
                                    row.get("\u5f53\u65e5\u6536\u76d8\u4ef7")
                                )
                                else None
                            ),
                            "change_pct": (
                                float(
                                    row.get(
                                        "\u5f53\u65e5\u6da8\u8dcc\u5e45", 0
                                    )
                                )
                                if pd.notna(
                                    row.get("\u5f53\u65e5\u6da8\u8dcc\u5e45")
                                )
                                else None
                            ),
                            "holding_shares": int(
                                row.get("\u6301\u80a1\u6570\u91cf", 0)
                            ),
                            "holding_value": float(
                                row.get("\u6301\u80a1\u5e02\u503c", 0)
                            ),
                            "holding_pct": (
                                float(
                                    row.get(
                                        "\u6301\u80a1\u6570\u91cf\u5360A\u80a1\u767e\u5206\u6bd4",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    row.get(
                                        "\u6301\u80a1\u6570\u91cf\u5360A\u80a1\u767e\u5206\u6bd4"
                                    )
                                )
                                else None
                            ),
                            "change_shares": (
                                float(
                                    row.get(
                                        "\u4eca\u65e5\u589e\u6301\u80a1\u6570",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    row.get(
                                        "\u4eca\u65e5\u589e\u6301\u80a1\u6570"
                                    )
                                )
                                else None
                            ),
                            "change_value": (
                                float(
                                    row.get(
                                        "\u4eca\u65e5\u589e\u6301\u8d44\u91d1",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    row.get(
                                        "\u4eca\u65e5\u589e\u6301\u8d44\u91d1"
                                    )
                                )
                                else None
                            ),
                            "value_change": (
                                float(
                                    row.get(
                                        "\u4eca\u65e5\u6301\u80a1\u5e02\u503c\u53d8\u5316",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    row.get(
                                        "\u4eca\u65e5\u6301\u80a1\u5e02\u503c\u53d8\u5316"
                                    )
                                )
                                else None
                            ),
                        }
                        holdings.append(holding)

                    latest = holdings[-1] if holdings else None

                    return {
                        "symbol": symbol,
                        "holdings": holdings,
                        "latest_holding": latest,
                        "data_cutoff_notice": "\u6570\u636e\u53ef\u80fd\u5728 2024-08-16 \u540e\u4e0d\u66f4\u65b0\uff0c\u8bf7\u4ee5\u4ea4\u6613\u6240\u516c\u544a\u4e3a\u51c6",
                        "source": "akshare",
                    }
                except Exception as e:
                    logger.debug(
                        "AKShare northbound holding skipped %s: %s", code, e
                    )
                    return None

            return await run_in_executor(_fetch_sync)

        return await self._cached_or_fetch(
            "northbound_holding", code, fetch
        )

    async def get_northbound_flow(
        self,
        direction: str = "\u5317\u5411\u8d44\u91d1",
        days: int = 30,
    ) -> Optional[Dict[str, Any]]:
        """Get northbound capital flow history."""

        async def fetch():
            import akshare as ak

            def _fetch_sync():
                try:
                    df = ak.stock_hsgt_hist_em(symbol=direction)

                    if df is None or df.empty:
                        return None

                    valid_df = df.dropna(
                        subset=["\u5f53\u65e5\u6210\u4ea4\u51c0\u4e70\u989d"]
                    )
                    latest_valid_date = None
                    if not valid_df.empty:
                        latest_valid_date = str(
                            valid_df["\u65e5\u671f"].max()
                        )[:10]

                    df = df.tail(days)

                    flows = []
                    for _, row in df.iterrows():
                        flow = {
                            "date": str(
                                row.get("\u65e5\u671f", "")
                            )[:10],
                            "net_buy": (
                                float(
                                    row.get(
                                        "\u5f53\u65e5\u6210\u4ea4\u51c0\u4e70\u989d",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    row.get(
                                        "\u5f53\u65e5\u6210\u4ea4\u51c0\u4e70\u989d"
                                    )
                                )
                                else None
                            ),
                            "buy_amount": (
                                float(
                                    row.get(
                                        "\u4e70\u5165\u6210\u4ea4\u989d", 0
                                    )
                                )
                                if pd.notna(
                                    row.get(
                                        "\u4e70\u5165\u6210\u4ea4\u989d"
                                    )
                                )
                                else None
                            ),
                            "sell_amount": (
                                float(
                                    row.get(
                                        "\u5356\u51fa\u6210\u4ea4\u989d", 0
                                    )
                                )
                                if pd.notna(
                                    row.get(
                                        "\u5356\u51fa\u6210\u4ea4\u989d"
                                    )
                                )
                                else None
                            ),
                            "cumulative_net_buy": (
                                float(
                                    row.get(
                                        "\u5386\u53f2\u7d2f\u8ba1\u51c0\u4e70\u989d",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    row.get(
                                        "\u5386\u53f2\u7d2f\u8ba1\u51c0\u4e70\u989d"
                                    )
                                )
                                else None
                            ),
                            "inflow": (
                                float(
                                    row.get(
                                        "\u5f53\u65e5\u8d44\u91d1\u6d41\u5165",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    row.get(
                                        "\u5f53\u65e5\u8d44\u91d1\u6d41\u5165"
                                    )
                                )
                                else None
                            ),
                            "remaining_quota": (
                                float(
                                    row.get(
                                        "\u5f53\u65e5\u4f59\u989d", 0
                                    )
                                )
                                if pd.notna(
                                    row.get("\u5f53\u65e5\u4f59\u989d")
                                )
                                else None
                            ),
                            "holding_value": (
                                float(
                                    row.get(
                                        "\u6301\u80a1\u5e02\u503c", 0
                                    )
                                )
                                if pd.notna(
                                    row.get("\u6301\u80a1\u5e02\u503c")
                                )
                                else None
                            ),
                        }
                        flows.append(flow)

                    return {
                        "direction": direction,
                        "flows": flows,
                        "latest_valid_date": latest_valid_date,
                        "data_cutoff_notice": "\u6570\u636e\u53ef\u80fd\u5728 2024-08-19 \u540e\u4e0d\u5b8c\u6574\uff0c\u8bf7\u4ee5\u4ea4\u6613\u6240\u516c\u544a\u4e3a\u51c6",
                        "source": "akshare",
                    }
                except Exception as e:
                    logger.error(
                        "AKShare northbound flow error: %s", e
                    )
                    return None

            return await run_in_executor(_fetch_sync)

        cache_key = f"{direction}:{days}"
        return await self._cached_or_fetch(
            "northbound_flow", cache_key, fetch
        )

    async def get_industry_sector_list(
        self,
    ) -> Optional[Dict[str, Any]]:
        """Get list of all industry sectors with real-time data."""

        async def fetch():
            import akshare as ak

            def _fetch_sync():
                try:
                    df = ak.stock_board_industry_name_em()

                    if df is None or df.empty:
                        return None

                    sectors = []
                    for _, row in df.iterrows():
                        sector = {
                            "rank": int(row.get("\u6392\u540d", 0)),
                            "sector_name": row.get(
                                "\u677f\u5757\u540d\u79f0"
                            ),
                            "sector_code": row.get(
                                "\u677f\u5757\u4ee3\u7801"
                            ),
                            "latest_price": (
                                float(row.get("\u6700\u65b0\u4ef7", 0))
                                if pd.notna(row.get("\u6700\u65b0\u4ef7"))
                                else None
                            ),
                            "change": (
                                float(row.get("\u6da8\u8dcc\u989d", 0))
                                if pd.notna(row.get("\u6da8\u8dcc\u989d"))
                                else None
                            ),
                            "change_pct": (
                                float(row.get("\u6da8\u8dcc\u5e45", 0))
                                if pd.notna(row.get("\u6da8\u8dcc\u5e45"))
                                else None
                            ),
                            "total_market_cap": (
                                float(row.get("\u603b\u5e02\u503c", 0))
                                if pd.notna(row.get("\u603b\u5e02\u503c"))
                                else None
                            ),
                            "turnover_rate": (
                                float(row.get("\u6362\u624b\u7387", 0))
                                if pd.notna(row.get("\u6362\u624b\u7387"))
                                else None
                            ),
                            "up_count": (
                                int(row.get("\u4e0a\u6da8\u5bb6\u6570", 0))
                                if pd.notna(
                                    row.get("\u4e0a\u6da8\u5bb6\u6570")
                                )
                                else None
                            ),
                            "down_count": (
                                int(row.get("\u4e0b\u8dcc\u5bb6\u6570", 0))
                                if pd.notna(
                                    row.get("\u4e0b\u8dcc\u5bb6\u6570")
                                )
                                else None
                            ),
                            "leading_stock": row.get(
                                "\u9886\u6da8\u80a1\u7968"
                            ),
                            "leading_stock_change": (
                                float(
                                    row.get(
                                        "\u9886\u6da8\u80a1\u7968-\u6da8\u8dcc\u5e45",
                                        0,
                                    )
                                )
                                if pd.notna(
                                    row.get(
                                        "\u9886\u6da8\u80a1\u7968-\u6da8\u8dcc\u5e45"
                                    )
                                )
                                else None
                            ),
                        }
                        sectors.append(sector)

                    return {
                        "sectors": sectors,
                        "update_time": datetime.now().isoformat(),
                        "source": "akshare",
                    }
                except Exception as e:
                    logger.error("AKShare sector list error: %s", e)
                    return None

            return await run_in_executor(_fetch_sync)

        return await self._cached_or_fetch(
            "industry_sector_list", "all", fetch
        )

    async def get_stock_industry_cn(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get industry information for A-share stock."""
        code = symbol.replace(".SS", "").replace(".SZ", "")

        async def fetch():
            import akshare as ak

            def _fetch_sync():
                try:
                    df = ak.stock_individual_info_em(symbol=code)

                    if df is None or df.empty:
                        return None

                    info = {}
                    for _, row in df.iterrows():
                        info[row["item"]] = row["value"]

                    return {
                        "symbol": symbol,
                        "stock_code": info.get(
                            "\u80a1\u7968\u4ee3\u7801"
                        ),
                        "stock_name": info.get(
                            "\u80a1\u7968\u7b80\u79f0"
                        ),
                        "industry": info.get("\u884c\u4e1a"),
                        "total_market_cap": info.get(
                            "\u603b\u5e02\u503c"
                        ),
                        "float_market_cap": info.get(
                            "\u6d41\u901a\u5e02\u503c"
                        ),
                        "source": "akshare",
                    }
                except Exception as e:
                    logger.error("AKShare stock info error: %s", e)
                    return None

            return await run_in_executor(_fetch_sync)

        return await self._cached_or_fetch(
            "stock_industry_cn", code, fetch
        )

    async def get_sector_history(
        self,
        sector_name: str,
        period: str = "\u65e5k",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get historical data for an industry sector."""
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        if start_date is None:
            start_date = (
                datetime.now() - timedelta(days=180)
            ).strftime("%Y%m%d")

        async def fetch():
            import akshare as ak

            def _fetch_sync():
                try:
                    df = ak.stock_board_industry_hist_em(
                        symbol=sector_name,
                        period=period,
                        start_date=start_date,
                        end_date=end_date,
                        adjust="",
                    )

                    if df is None or df.empty:
                        return None

                    bars = []
                    for _, row in df.iterrows():
                        bar = {
                            "date": str(
                                row.get("\u65e5\u671f", "")
                            )[:10],
                            "open": float(
                                row.get("\u5f00\u76d8", 0)
                            ),
                            "close": float(
                                row.get("\u6536\u76d8", 0)
                            ),
                            "high": float(
                                row.get("\u6700\u9ad8", 0)
                            ),
                            "low": float(
                                row.get("\u6700\u4f4e", 0)
                            ),
                            "change_pct": (
                                float(
                                    row.get("\u6da8\u8dcc\u5e45", 0)
                                )
                                if pd.notna(
                                    row.get("\u6da8\u8dcc\u5e45")
                                )
                                else None
                            ),
                            "change": (
                                float(
                                    row.get("\u6da8\u8dcc\u989d", 0)
                                )
                                if pd.notna(
                                    row.get("\u6da8\u8dcc\u989d")
                                )
                                else None
                            ),
                            "volume": (
                                int(row.get("\u6210\u4ea4\u91cf", 0))
                                if pd.notna(
                                    row.get("\u6210\u4ea4\u91cf")
                                )
                                else None
                            ),
                            "amount": (
                                float(
                                    row.get("\u6210\u4ea4\u989d", 0)
                                )
                                if pd.notna(
                                    row.get("\u6210\u4ea4\u989d")
                                )
                                else None
                            ),
                            "amplitude": (
                                float(row.get("\u632f\u5e45", 0))
                                if pd.notna(row.get("\u632f\u5e45"))
                                else None
                            ),
                            "turnover_rate": (
                                float(
                                    row.get("\u6362\u624b\u7387", 0)
                                )
                                if pd.notna(
                                    row.get("\u6362\u624b\u7387")
                                )
                                else None
                            ),
                        }
                        bars.append(bar)

                    return {
                        "sector_name": sector_name,
                        "period": period,
                        "bars": bars,
                        "source": "akshare",
                    }
                except Exception as e:
                    logger.error(
                        "AKShare sector history error: %s", e
                    )
                    return None

            return await run_in_executor(_fetch_sync)

        cache_key = f"{sector_name}:{period}:{start_date}:{end_date}"
        return await self._cached_or_fetch(
            "sector_history", cache_key, fetch
        )

    async def get_hk_stock_history(
        self, symbol: str, days: int = 30
    ) -> Optional[Dict[str, Any]]:
        """Get Hong Kong stock historical data with yfinance fallback."""
        code = symbol.replace(".HK", "").lstrip("0").zfill(5)

        async def fetch():
            import akshare as ak

            def _fetch_sync():
                try:
                    df = ak.stock_hk_hist(
                        symbol=code, period="daily", adjust="qfq"
                    )

                    if df is None or df.empty:
                        return None

                    df = df.tail(days)

                    bars = []
                    for _, row in df.iterrows():
                        bars.append({
                            "date": str(
                                row.get("\u65e5\u671f", "")
                            )[:10],
                            "open": float(
                                row.get("\u5f00\u76d8", 0)
                            ),
                            "high": float(
                                row.get("\u6700\u9ad8", 0)
                            ),
                            "low": float(
                                row.get("\u6700\u4f4e", 0)
                            ),
                            "close": float(
                                row.get("\u6536\u76d8", 0)
                            ),
                            "volume": int(
                                row.get("\u6210\u4ea4\u91cf", 0)
                            ),
                            "amount": float(
                                row.get("\u6210\u4ea4\u989d", 0)
                            ),
                            "change_pct": (
                                float(
                                    row.get("\u6da8\u8dcc\u5e45", 0)
                                )
                                if pd.notna(
                                    row.get("\u6da8\u8dcc\u5e45")
                                )
                                else None
                            ),
                        })

                    return {
                        "symbol": symbol,
                        "bars": bars,
                        "source": "akshare",
                    }
                except Exception as e:
                    logger.warning(
                        "AKShare HK history error: %s, trying yfinance", e
                    )
                    return None

            result = await run_in_executor(_fetch_sync)

            # Fallback to yfinance
            if result is None:
                import yfinance as yf

                def _fetch_yf():
                    ticker = yf.Ticker(f"{code}.HK")
                    df = ticker.history(period=f"{days}d")

                    if df is None or df.empty:
                        return None

                    bars = []
                    for idx, row in df.iterrows():
                        bars.append({
                            "date": idx.strftime("%Y-%m-%d"),
                            "open": round(float(row["Open"]), 2),
                            "high": round(float(row["High"]), 2),
                            "low": round(float(row["Low"]), 2),
                            "close": round(float(row["Close"]), 2),
                            "volume": int(row["Volume"]),
                        })

                    return {
                        "symbol": symbol,
                        "bars": bars,
                        "source": "yfinance",
                    }

                result = await run_in_executor(_fetch_yf)

            return result

        return await self._cached_or_fetch(
            "hk_history", f"{code}:{days}", fetch
        )

    # ------------------------------------------------------------------
    # News
    # ------------------------------------------------------------------
    async def get_news(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """News from akshare.

        - Per-symbol (A-share): ``stock_news_em(symbol=...)``
        - Global (A-share market-wide): ``stock_zh_a_alerts_cls()`` —
          财联社电报, returned when ``symbol`` is None.

        HK / US not supported via akshare news endpoints.
        """
        try:
            import akshare as ak
            from datetime import datetime as _dt

            since_ts: Optional[float] = None
            if since:
                try:
                    since_ts = _dt.fromisoformat(
                        since.replace("Z", "+00:00")
                    ).timestamp()
                except Exception:
                    since_ts = None

            if symbol:
                # Strip exchange suffix; ak.stock_news_em expects bare 6-digit code
                code = normalize_symbol(symbol, market or SH)

                def _fetch_sym():
                    try:
                        return ak.stock_news_em(symbol=code)
                    except Exception as exc:
                        logger.warning(
                            "akshare stock_news_em(%s) failed: %s", code, exc,
                        )
                        return None

                df = await run_in_executor(_fetch_sym)
                if df is None or df.empty:
                    return []

                out: List[Dict[str, Any]] = []
                for _, row in df.head(limit).iterrows():
                    pub = row.get("发布时间")  # 发布时间
                    pub_iso = None
                    if pd.notna(pub):
                        try:
                            pub_iso = pd.to_datetime(pub).isoformat()
                        except Exception:
                            pub_iso = str(pub)

                    if since_ts and pub_iso:
                        try:
                            if pd.to_datetime(pub_iso).timestamp() < since_ts:
                                continue
                        except Exception:
                            pass

                    raw = {k: (None if pd.isna(v) else v) for k, v in row.items()}
                    out.append({
                        "source": "akshare",
                        "id": None,
                        "title": str(row.get("新闻标题") or ""),
                        "summary": (
                            str(row.get("新闻内容"))
                            if pd.notna(row.get("新闻内容"))
                            else None
                        ),
                        "url": (
                            str(row.get("新闻链接"))
                            if pd.notna(row.get("新闻链接"))
                            else None
                        ),
                        "publisher": (
                            str(row.get("文章来源"))
                            if pd.notna(row.get("文章来源"))
                            else "东方财富"
                        ),
                        "published_at": pub_iso,
                        "symbols": [symbol],
                        "image_url": None,
                        "language": "zh",
                        "raw": raw,
                    })
                return out

            # Global: 财联社电报
            def _fetch_global():
                try:
                    return ak.stock_info_global_cls()
                except Exception as exc:
                    logger.warning(
                        "akshare stock_info_global_cls failed: %s", exc,
                    )
                    return None

            df = await run_in_executor(_fetch_global)
            if df is None or df.empty:
                return []

            out2: List[Dict[str, Any]] = []
            for _, row in df.head(limit).iterrows():
                date_v = row.get("发布日期")  # 发布日期
                time_v = row.get("发布时间")  # 发布时间
                pub_iso = None
                try:
                    if pd.notna(date_v) and pd.notna(time_v):
                        pub_iso = pd.to_datetime(
                            f"{date_v} {time_v}"
                        ).isoformat()
                except Exception:
                    pub_iso = None

                if since_ts and pub_iso:
                    try:
                        if pd.to_datetime(pub_iso).timestamp() < since_ts:
                            continue
                    except Exception:
                        pass

                raw = {k: (None if pd.isna(v) else v) for k, v in row.items()}
                out2.append({
                    "source": "akshare",
                    "id": None,
                    "title": str(row.get("标题") or ""),
                    "summary": (
                        str(row.get("内容"))
                        if pd.notna(row.get("内容"))
                        else None
                    ),
                    "url": None,
                    "publisher": "财联社",
                    "published_at": pub_iso,
                    "symbols": [],
                    "image_url": None,
                    "language": "zh",
                    "raw": raw,
                })
            return out2
        except Exception as e:
            logger.warning("akshare news error: %s", e)
            return []

    # === Market-wide Sentiment / Flow Methods ===

    async def get_margin_trading(
        self, start_date: str, end_date: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Get market-wide margin trading data from SSE."""
        try:
            import akshare as ak

            def fetch():
                return ak.stock_margin_sse(
                    start_date=start_date, end_date=end_date
                )

            df = await run_in_executor(fetch, timeout=60)
            if df is None or df.empty:
                return None

            results = []
            for _, row in df.iterrows():
                results.append({
                    "date": str(row.get("信用交易日期", ""))[:10],
                    "margin_balance": (
                        float(row["融资余额"])
                        if pd.notna(row.get("融资余额")) else None
                    ),
                    "margin_buy": (
                        float(row["融资买入额"])
                        if pd.notna(row.get("融资买入额")) else None
                    ),
                    "short_sell_qty": (
                        float(row["融券余量"])
                        if pd.notna(row.get("融券余量")) else None
                    ),
                    "short_sell_value": (
                        float(row["融券余量金额"])
                        if pd.notna(row.get("融券余量金额")) else None
                    ),
                    "short_sell_volume": (
                        float(row["融券卖出量"])
                        if pd.notna(row.get("融券卖出量")) else None
                    ),
                    "total_balance": (
                        float(row["融资融券余额"])
                        if pd.notna(row.get("融资融券余额")) else None
                    ),
                })
            return results
        except Exception as e:
            logger.warning("AKShare margin trading error: %s", e)
            return None

    async def get_stock_connect_flow(
        self, direction: str, days: int = 30
    ) -> Optional[List[Dict[str, Any]]]:
        """Get daily Stock Connect capital flow (沪股通/深股通)."""
        try:
            import akshare as ak

            def fetch():
                return ak.stock_hsgt_hist_em(symbol=direction)

            df = await run_in_executor(fetch, timeout=60)
            if df is None or df.empty:
                return None

            df = df.tail(days)

            results = []
            for _, row in df.iterrows():
                results.append({
                    "date": str(row.get("日期", ""))[:10],
                    "net_buy": (
                        float(row["当日成交净买额"])
                        if pd.notna(row.get("当日成交净买额")) else None
                    ),
                    "buy_amount": (
                        float(row["买入成交额"])
                        if pd.notna(row.get("买入成交额")) else None
                    ),
                    "sell_amount": (
                        float(row["卖出成交额"])
                        if pd.notna(row.get("卖出成交额")) else None
                    ),
                    "cumulative_net_buy": (
                        float(row["历史累计净买额"])
                        if pd.notna(row.get("历史累计净买额")) else None
                    ),
                    "holding_value": (
                        float(row["持股市值"])
                        if pd.notna(row.get("持股市值")) else None
                    ),
                })
            return results
        except Exception as e:
            logger.warning("AKShare stock connect flow error: %s", e)
            return None

    async def get_shareholder_count(
        self, quarter_date: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Get shareholder concentration data for all A-shares."""
        try:
            import akshare as ak

            def fetch():
                return ak.stock_zh_a_gdhs(symbol=quarter_date)

            df = await run_in_executor(fetch, timeout=60)
            if df is None or df.empty:
                return None

            results = []
            for _, row in df.iterrows():
                code = str(row.get("代码", ""))
                if code.startswith("6"):
                    sym = code + ".SS"
                else:
                    sym = code + ".SZ"

                results.append({
                    "symbol": sym,
                    "name": row.get("名称"),
                    "current_count": (
                        int(row["股东户数-本次"])
                        if pd.notna(row.get("股东户数-本次")) else None
                    ),
                    "prior_count": (
                        int(row["股东户数-上次"])
                        if pd.notna(row.get("股东户数-上次")) else None
                    ),
                    "change": (
                        int(row["股东户数-增减"])
                        if pd.notna(row.get("股东户数-增减")) else None
                    ),
                    "change_pct": (
                        float(row["股东户数-增减比例"])
                        if pd.notna(row.get("股东户数-增减比例")) else None
                    ),
                    "avg_holding_value": (
                        float(row["户均持股市值"])
                        if pd.notna(row.get("户均持股市值")) else None
                    ),
                    "avg_holding_shares": (
                        float(row["户均持股数量"])
                        if pd.notna(row.get("户均持股数量")) else None
                    ),
                    "report_date": (
                        str(row["公告日期"])[:10]
                        if pd.notna(row.get("公告日期")) else None
                    ),
                })
            return results
        except Exception as e:
            logger.warning("AKShare shareholder count error: %s", e)
            return None

    async def get_dragon_tiger(
        self, start_date: str, end_date: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Get dragon-tiger board (top trader activity) data."""
        try:
            import akshare as ak

            def fetch():
                return ak.stock_lhb_detail_em(
                    start_date=start_date, end_date=end_date
                )

            df = await run_in_executor(fetch, timeout=60)
            if df is None or df.empty:
                return None

            results = []
            for _, row in df.iterrows():
                results.append({
                    "symbol": str(row.get("代码", "")),
                    "name": row.get("名称"),
                    "date": str(row.get("上榜日", ""))[:10],
                    "close": (
                        float(row["收盘价"])
                        if pd.notna(row.get("收盘价")) else None
                    ),
                    "change_pct": (
                        float(row["涨跌幅"])
                        if pd.notna(row.get("涨跌幅")) else None
                    ),
                    "net_buy": (
                        float(row["龙虎榜净买额"])
                        if pd.notna(row.get("龙虎榜净买额")) else None
                    ),
                })
            return results
        except Exception as e:
            logger.warning("AKShare dragon tiger error: %s", e)
            return None

    async def get_block_trades(
        self, start_date: str, end_date: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Get large block trade data."""
        try:
            import akshare as ak

            def fetch():
                return ak.stock_dzjy_mrtj(
                    start_date=start_date, end_date=end_date
                )

            df = await run_in_executor(fetch, timeout=60)
            if df is None or df.empty:
                return None

            results = []
            for _, row in df.iterrows():
                results.append({
                    "date": str(row.get("交易日期", ""))[:10],
                    "symbol": str(row.get("证券代码", "")),
                    "name": row.get("证券简称"),
                    "change_pct": (
                        float(row["涨跌幅"])
                        if pd.notna(row.get("涨跌幅")) else None
                    ),
                    "close": (
                        float(row["收盘价"])
                        if pd.notna(row.get("收盘价")) else None
                    ),
                    "trade_price": (
                        float(row["成交价"])
                        if pd.notna(row.get("成交价")) else None
                    ),
                    "premium_rate": (
                        float(row["折溢率"])
                        if pd.notna(row.get("折溢率")) else None
                    ),
                })
            return results
        except Exception as e:
            logger.warning("AKShare block trades error: %s", e)
            return None
