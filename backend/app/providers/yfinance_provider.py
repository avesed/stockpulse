"""YFinance data provider for US stocks, HK, and precious metals.

Migrated from backend/app/services/providers/yfinance.py.
Uses the shared executor and cache helpers instead of per-provider ThreadPool/Redis.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from app.core.redis import cache_get, cache_set, jittered_ttl
from app.core.executor import run_in_executor
from app.providers.base import DataProvider
from app.providers.constants import (
    HK,
    METAL,
    PRECIOUS_METALS,
    SH,
    SZ,
    US,
)

logger = logging.getLogger(__name__)

# Cache TTL configurations (base_seconds, jitter_seconds)
CACHE_TTL = {
    "institutional_holders": (86400, 3600),  # 24h + rand(1h)
    "market_index": (300, 60),  # 5min + rand(1min)
    "analyst_ratings": (86400, 3600),  # 24h + rand(1h)
    "technical_info": (3600, 600),  # 1h + rand(10min)
}

# Market index symbol mapping
MARKET_INDICES = {
    "sp500": ("^GSPC", "S&P 500"),
    "hang_seng": ("^HSI", "\u6052\u751f\u6307\u6570"),
    "shanghai": ("000001.SS", "\u4e0a\u8bc1\u7efc\u6307"),
    "shenzhen": ("399001.SZ", "\u6df1\u8bc1\u6210\u6307"),
}


def _ttl(data_type: str) -> int:
    """Get jittered TTL for a data type."""
    base, jitter = CACHE_TTL.get(data_type, (3600, 300))
    return jittered_ttl(base, jitter)


class YFinanceProvider(DataProvider):
    """YFinance data provider for US stocks, HK stocks, and precious metals.

    Primary provider for:
    - US stocks (NYSE, NASDAQ)
    - Precious metals (COMEX/NYMEX futures)

    Fallback provider for:
    - HK stocks (when AKShare fails)
    - A-shares (when AKShare and Tushare fail)
    """

    @property
    def name(self) -> str:
        return "yfinance"

    @property
    def supported_markets(self) -> Set[str]:
        return {US, HK, METAL, SH, SZ}

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
        cache_key = f"yfinance:{data_type}:{identifier}"
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
        """Get real-time quote from yfinance."""
        try:
            import yfinance as yf

            def fetch():
                ticker = yf.Ticker(symbol)
                info = ticker.info
                if not info or info.get("regularMarketPrice") is None:
                    return None
                return info

            info = await run_in_executor(fetch)
            if not info:
                return None

            price = info.get("regularMarketPrice", 0)
            prev_close = info.get("previousClose", price)
            change = price - prev_close if prev_close else 0
            change_pct = (change / prev_close * 100) if prev_close else 0

            return {
                "symbol": symbol,
                "name": info.get("shortName") or info.get("longName"),
                "price": price,
                "change": round(change, 4),
                "change_percent": round(change_pct, 2),
                "volume": info.get("regularMarketVolume", 0),
                "market_cap": info.get("marketCap"),
                "high": info.get("dayHigh"),
                "low": info.get("dayLow"),
                "open": info.get("open"),
                "prev_close": prev_close,
                "timestamp": datetime.utcnow().isoformat(),
                "market": market,
                "currency": info.get("currency"),
                "source": "yfinance",
            }
        except Exception as e:
            logger.error("YFinance quote error for %s: %s", symbol, e)
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
        """Get historical data from yfinance."""
        try:
            import yfinance as yf

            def fetch():
                ticker = yf.Ticker(symbol)
                if start and end:
                    # yfinance only accepts YYYY-MM-DD for start/end (not datetime strings).
                    yf_start = start[:10] if len(start) > 10 else start
                    yf_end = end[:10] if len(end) > 10 else end
                    # yfinance end is exclusive -- if same date, bump end by 1 day
                    if yf_start == yf_end:
                        from datetime import datetime as _dt, timedelta as _td
                        yf_end = (
                            _dt.strptime(yf_end, "%Y-%m-%d") + _td(days=1)
                        ).strftime("%Y-%m-%d")
                    logger.info(
                        "YFinance history for %s: start=%s, end=%s, interval=%s",
                        symbol, yf_start, yf_end, interval,
                    )
                    df = ticker.history(
                        start=yf_start, end=yf_end, interval=interval
                    )
                else:
                    df = ticker.history(period=period, interval=interval)
                return df

            df = await run_in_executor(fetch)
            if df is None or df.empty:
                return None

            # Drop rows with NaN OHLC values (common in 1m data)
            df = df.dropna(subset=["Open", "High", "Low", "Close"])

            bars = []
            for idx, row in df.iterrows():
                bars.append({
                    "date": idx.to_pydatetime().isoformat(),
                    "open": round(row["Open"], 4),
                    "high": round(row["High"], 4),
                    "low": round(row["Low"], 4),
                    "close": round(row["Close"], 4),
                    "volume": int(row["Volume"]),
                })

            return {
                "symbol": symbol,
                "interval": interval,
                "bars": bars,
                "market": market,
                "source": "yfinance",
            }
        except Exception as e:
            logger.error("YFinance history error for %s: %s", symbol, e)
            return None

    async def search(
        self, query: str, markets: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        """Search stocks using yfinance (limited -- direct ticker lookup only)."""
        try:
            import yfinance as yf

            def fetch():
                ticker = yf.Ticker(query.upper())
                info = ticker.info
                if info and info.get("shortName"):
                    return [{
                        "symbol": query.upper(),
                        "name": info.get("shortName", ""),
                        "exchange": info.get("exchange", ""),
                    }]
                return []

            results = await run_in_executor(fetch)
            return [
                {
                    "symbol": r["symbol"],
                    "name": r["name"],
                    "exchange": r["exchange"],
                    "market": US,
                }
                for r in results
            ]
        except Exception as e:
            logger.error("YFinance search error for %s: %s", query, e)
            return []

    # === Optional Methods ===

    async def get_info(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get company/asset info from yfinance."""
        # Handle precious metals specially
        if market == METAL and symbol in PRECIOUS_METALS:
            metal_info = PRECIOUS_METALS[symbol]
            return {
                "symbol": symbol,
                "name": metal_info["name"],
                "description": metal_info["name_zh"] + " (" + metal_info["name"] + ")",
                "sector": "Commodities",
                "industry": "Precious Metals",
                "website": None,
                "employees": None,
                "market_cap": None,
                "currency": metal_info["currency"],
                "exchange": metal_info["exchange"],
                "market": market,
                "source": "yfinance",
            }

        try:
            import yfinance as yf

            def fetch():
                ticker = yf.Ticker(symbol)
                return ticker.info

            info = await run_in_executor(fetch)
            if not info or not info.get("shortName"):
                return None

            return {
                "symbol": symbol,
                "name": info.get("shortName") or info.get("longName", ""),
                "description": info.get("longBusinessSummary"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "website": info.get("website"),
                "employees": info.get("fullTimeEmployees"),
                "market_cap": info.get("marketCap"),
                "currency": info.get("currency", "USD"),
                "exchange": info.get("exchange", ""),
                "market": market,
                "source": "yfinance",
            }
        except Exception as e:
            logger.error("YFinance info error for %s: %s", symbol, e)
            return None

    async def get_financials(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get financial data from yfinance."""
        # Precious metals don't have financials
        if market == METAL:
            return None

        try:
            import yfinance as yf

            def fetch():
                ticker = yf.Ticker(symbol)
                return ticker.info

            info = await run_in_executor(fetch)
            if not info:
                return None

            # Normalize dividend yield
            dividend_yield = info.get("dividendYield")
            if dividend_yield is not None:
                dividend_yield = dividend_yield / 100  # Convert 0.37 -> 0.0037
            elif info.get("payoutRatio") == 0:
                dividend_yield = 0.0

            dividend_rate = info.get("dividendRate")
            if dividend_rate is None and info.get("payoutRatio") == 0:
                dividend_rate = 0.0

            return {
                "symbol": symbol,
                "pe_ratio": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "eps": info.get("trailingEps"),
                "dividend_yield": dividend_yield,
                "dividend_rate": dividend_rate,
                "book_value": info.get("bookValue"),
                "price_to_book": info.get("priceToBook"),
                "revenue": info.get("totalRevenue"),
                "revenue_growth": info.get("revenueGrowth"),
                "net_income": info.get("netIncomeToCommon"),
                "profit_margin": info.get("profitMargins"),
                "gross_margin": info.get("grossMargins"),
                "operating_margin": info.get("operatingMargins"),
                "roe": info.get("returnOnEquity"),
                "roa": info.get("returnOnAssets"),
                "debt_to_equity": info.get("debtToEquity"),
                "current_ratio": info.get("currentRatio"),
                "eps_growth": info.get("earningsQuarterlyGrowth"),
                "payout_ratio": info.get("payoutRatio"),
                "market": market,
                "source": "yfinance",
            }
        except Exception as e:
            logger.error("YFinance financials error for %s: %s", symbol, e)
            return None

    async def get_analyst_ratings(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get analyst ratings and price targets."""

        async def fetch():
            import yfinance as yf

            def _fetch_sync():
                ticker = yf.Ticker(symbol)
                info = ticker.info

                if not info:
                    return None

                recommendation = info.get("recommendationKey")
                target_mean = info.get("targetMeanPrice")
                current_price = info.get("currentPrice") or info.get(
                    "regularMarketPrice"
                )

                if not recommendation and not target_mean:
                    return None

                upside_pct = None
                if target_mean and current_price and current_price > 0:
                    upside_pct = (
                        (target_mean - current_price) / current_price
                    ) * 100

                return {
                    "symbol": symbol,
                    "recommendation": recommendation,
                    "recommendation_mean": info.get("recommendationMean"),
                    "target_mean_price": target_mean,
                    "target_high_price": info.get("targetHighPrice"),
                    "target_low_price": info.get("targetLowPrice"),
                    "target_median_price": info.get("targetMedianPrice"),
                    "number_of_analysts": info.get("numberOfAnalystOpinions"),
                    "current_price": current_price,
                    "upside_pct": (
                        round(upside_pct, 2) if upside_pct else None
                    ),
                    "source": "yfinance",
                }

            return await run_in_executor(_fetch_sync)

        return await self._cached_or_fetch("analyst_ratings", symbol, fetch)

    async def get_fundamentals_bundle(
        self, symbol: str, market: str,
    ) -> dict[str, Any]:
        """Fetch financials + analyst ratings in a single yf.Ticker().info call.

        Returns ``{"financials": {...}|None, "analyst": {...}|None}``.
        Used by the fundamentals collection service to avoid duplicate HTTP
        requests (both get_financials and get_analyst_ratings call ticker.info).
        """
        if market == METAL:
            return {"financials": None, "analyst": None}

        try:
            import yfinance as yf

            def _fetch():
                return yf.Ticker(symbol).info

            info = await run_in_executor(_fetch)
            if not info:
                return {"financials": None, "analyst": None}

            # --- financials ---
            dividend_yield = info.get("dividendYield")
            if dividend_yield is not None:
                dividend_yield = dividend_yield / 100
            elif info.get("payoutRatio") == 0:
                dividend_yield = 0.0

            dividend_rate = info.get("dividendRate")
            if dividend_rate is None and info.get("payoutRatio") == 0:
                dividend_rate = 0.0

            financials = {
                "symbol": symbol,
                "pe_ratio": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "eps": info.get("trailingEps"),
                "dividend_yield": dividend_yield,
                "dividend_rate": dividend_rate,
                "book_value": info.get("bookValue"),
                "price_to_book": info.get("priceToBook"),
                "revenue": info.get("totalRevenue"),
                "revenue_growth": info.get("revenueGrowth"),
                "net_income": info.get("netIncomeToCommon"),
                "profit_margin": info.get("profitMargins"),
                "gross_margin": info.get("grossMargins"),
                "operating_margin": info.get("operatingMargins"),
                "roe": info.get("returnOnEquity"),
                "roa": info.get("returnOnAssets"),
                "debt_to_equity": info.get("debtToEquity"),
                "current_ratio": info.get("currentRatio"),
                "eps_growth": info.get("earningsQuarterlyGrowth"),
                "payout_ratio": info.get("payoutRatio"),
                "market": market,
                "source": "yfinance",
            }

            # --- analyst ratings ---
            recommendation = info.get("recommendationKey")
            target_mean = info.get("targetMeanPrice")
            current_price = info.get("currentPrice") or info.get("regularMarketPrice")

            analyst = None
            if recommendation or target_mean:
                upside_pct = None
                if target_mean and current_price and current_price > 0:
                    upside_pct = round(((target_mean - current_price) / current_price) * 100, 2)
                analyst = {
                    "symbol": symbol,
                    "recommendation": recommendation,
                    "recommendation_mean": info.get("recommendationMean"),
                    "target_mean_price": target_mean,
                    "target_high_price": info.get("targetHighPrice"),
                    "target_low_price": info.get("targetLowPrice"),
                    "target_median_price": info.get("targetMedianPrice"),
                    "number_of_analysts": info.get("numberOfAnalystOpinions"),
                    "current_price": current_price,
                    "upside_pct": upside_pct,
                    "source": "yfinance",
                }

            return {"financials": financials, "analyst": analyst}
        except Exception as e:
            logger.error("YFinance bundle error for %s: %s", symbol, e)
            return {"financials": None, "analyst": None}

    async def get_technical_info(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get pre-calculated technical data from yfinance."""

        async def fetch():
            import yfinance as yf

            def _fetch_sync():
                ticker = yf.Ticker(symbol)
                info = ticker.info

                if not info:
                    return None

                return {
                    "symbol": symbol,
                    "fiftyDayAverage": info.get("fiftyDayAverage"),
                    "twoHundredDayAverage": info.get("twoHundredDayAverage"),
                    "averageVolume": info.get("averageVolume"),
                    "averageVolume10days": info.get("averageVolume10days"),
                    "beta": info.get("beta"),
                    "fiftyTwoWeekHigh": info.get("fiftyTwoWeekHigh"),
                    "fiftyTwoWeekLow": info.get("fiftyTwoWeekLow"),
                    "currentPrice": info.get("currentPrice")
                    or info.get("regularMarketPrice"),
                    "source": "yfinance",
                }

            return await run_in_executor(_fetch_sync)

        return await self._cached_or_fetch("technical_info", symbol, fetch)

    # === Extended Methods (Institutional Data) ===

    async def get_institutional_holders(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get institutional holders for a stock (US/HK)."""

        async def fetch():
            import yfinance as yf

            def _fetch_sync():
                ticker = yf.Ticker(symbol)
                holders_df = ticker.institutional_holders

                if holders_df is None or holders_df.empty:
                    return None

                holders = []
                for _, row in holders_df.iterrows():
                    holder = {
                        "date_reported": (
                            str(row.get("Date Reported"))[:10]
                            if pd.notna(row.get("Date Reported"))
                            else None
                        ),
                        "holder": row.get("Holder", ""),
                        "pct_held": (
                            float(row.get("pctHeld", 0))
                            if pd.notna(row.get("pctHeld"))
                            else None
                        ),
                        "shares": (
                            int(row.get("Shares", 0))
                            if pd.notna(row.get("Shares"))
                            else None
                        ),
                        "value": (
                            int(row.get("Value", 0))
                            if pd.notna(row.get("Value"))
                            else None
                        ),
                        "pct_change": (
                            float(row.get("pctChange", 0))
                            if pd.notna(row.get("pctChange"))
                            else None
                        ),
                    }
                    holders.append(holder)

                total_pct = sum(
                    h["pct_held"]
                    for h in holders
                    if h["pct_held"] is not None
                )

                latest_date = None
                if holders and holders[0].get("date_reported"):
                    latest_date = holders[0]["date_reported"]

                return {
                    "symbol": symbol,
                    "holders": holders,
                    "total_institutional_pct": total_pct,
                    "data_as_of": latest_date,
                    "source": "yfinance",
                }

            return await run_in_executor(_fetch_sync)

        return await self._cached_or_fetch(
            "institutional_holders", symbol, fetch
        )

    # === Market Index Methods ===

    async def get_market_index(
        self, index_symbol: str, period: str = "5d"
    ) -> Optional[Dict[str, Any]]:
        """Get market index data."""

        async def fetch():
            import yfinance as yf

            def _fetch_sync():
                ticker = yf.Ticker(index_symbol)
                df = ticker.history(period=period)

                if df is None or df.empty:
                    return None

                info = ticker.info
                name = info.get("shortName", index_symbol)

                bars = []
                for idx, row in df.iterrows():
                    bars.append({
                        "date": idx.isoformat(),
                        "open": round(float(row["Open"]), 2),
                        "high": round(float(row["High"]), 2),
                        "low": round(float(row["Low"]), 2),
                        "close": round(float(row["Close"]), 2),
                        "volume": int(row["Volume"]),
                    })

                latest_close = bars[-1]["close"] if bars else None
                prev_close = bars[-2]["close"] if len(bars) >= 2 else None
                change_pct = None
                if latest_close and prev_close:
                    change_pct = round(
                        (latest_close - prev_close) / prev_close * 100, 2
                    )

                return {
                    "symbol": index_symbol,
                    "name": name,
                    "bars": bars,
                    "latest_close": latest_close,
                    "change_pct": change_pct,
                    "source": "yfinance",
                }

            return await run_in_executor(_fetch_sync)

        cache_key = f"{index_symbol}:{period}"
        return await self._cached_or_fetch("market_index", cache_key, fetch)

    async def get_all_market_indices(
        self, period: str = "5d"
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Get all major market indices in parallel."""
        import asyncio

        tasks = {
            name: self.get_market_index(sym, period)
            for name, (sym, _) in MARKET_INDICES.items()
        }

        results = await asyncio.gather(
            *tasks.values(), return_exceptions=True
        )

        return {
            name: result if not isinstance(result, Exception) else None
            for name, result in zip(tasks.keys(), results)
        }

    async def get_news(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Per-symbol news from yfinance. No global feed.

        yfinance returns items in two possible shapes (legacy + 0.2.x).
        We map both into the common schema and pass the original under
        ``raw``.
        """
        if not symbol:
            return []

        try:
            import yfinance as yf
            from datetime import datetime as _dt

            def _fetch():
                return yf.Ticker(symbol).news or []

            items = await run_in_executor(_fetch)
            if not items:
                return []

            since_ts: Optional[float] = None
            if since:
                try:
                    since_ts = _dt.fromisoformat(
                        since.replace("Z", "+00:00")
                    ).timestamp()
                except Exception:
                    since_ts = None

            out: List[Dict[str, Any]] = []
            for it in items[:limit]:
                content = it.get("content") if isinstance(it, dict) else None

                if isinstance(content, dict):
                    # Modern shape (yfinance 0.2.x)
                    canonical = content.get("canonicalUrl") or {}
                    click = content.get("clickThroughUrl") or {}
                    thumb = content.get("thumbnail") or {}
                    resolutions = thumb.get("resolutions") or []
                    image_url = resolutions[0].get("url") if resolutions else None
                    provider = (content.get("provider") or {}).get("displayName")
                    pub = content.get("pubDate") or content.get("displayTime")

                    title = content.get("title") or ""
                    summary = content.get("summary") or content.get("description")
                    url = canonical.get("url") or click.get("url")
                    nid = content.get("id") or it.get("id")
                else:
                    # Legacy shape
                    title = it.get("title", "")
                    summary = None
                    url = it.get("link")
                    provider = it.get("publisher")
                    ts = it.get("providerPublishTime")
                    pub = (
                        _dt.utcfromtimestamp(ts).isoformat() + "Z"
                        if isinstance(ts, (int, float))
                        else None
                    )
                    image_url = None
                    nid = it.get("uuid")

                # since filter
                if since_ts and pub:
                    try:
                        item_ts = _dt.fromisoformat(
                            str(pub).replace("Z", "+00:00")
                        ).timestamp()
                        if item_ts < since_ts:
                            continue
                    except Exception:
                        pass

                related = (
                    content.get("relatedTickers")
                    if isinstance(content, dict)
                    else it.get("relatedTickers")
                ) or []

                out.append({
                    "source": "yfinance",
                    "id": nid,
                    "title": title,
                    "summary": summary,
                    "url": url,
                    "publisher": provider,
                    "published_at": pub,
                    "symbols": [s for s in related if isinstance(s, str)] or [symbol],
                    "image_url": image_url,
                    "language": "en",
                    "raw": it if isinstance(it, dict) else None,
                })
            return out
        except Exception as e:
            logger.warning("YFinance news error for %s: %s", symbol, e)
            return []

    async def get_sector_industry(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get sector and industry classification for a stock."""

        async def fetch():
            import yfinance as yf

            def _fetch_sync():
                ticker = yf.Ticker(symbol)
                info = ticker.info

                if not info:
                    return None

                sector = info.get("sector")
                industry = info.get("industry")

                if not sector and not industry:
                    return None

                return {
                    "symbol": symbol,
                    "sector": sector,
                    "industry": industry,
                    "source": "yfinance",
                }

            return await run_in_executor(_fetch_sync)

        # No caching for this simple call as it's part of other cached operations
        return await fetch()
