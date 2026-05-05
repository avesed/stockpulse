"""YFinance data provider for US stocks, HK, and precious metals.

All yfinance calls run in a persistent multiprocessing pool
(``core.yf_process_pool``) for memory leak isolation. The ``yf_call``
function is a synchronous blocking call submitted through the provider
queue's ThreadPool — the thread just waits on IPC while the subprocess
does the actual yfinance work.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from app.core.redis import cache_get, cache_set, jittered_ttl
from app.core.executor import ExecutorPool
from app.core.provider_queue import Priority, submit
from app.core.yf_process_pool import yf_call
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
    "hang_seng": ("^HSI", "恒生指数"),
    "shanghai": ("000001.SS", "上证综指"),
    "shenzhen": ("399001.SZ", "深证成指"),
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
            info = await submit(
                "yfinance", yf_call, "ticker_info", {"symbol": symbol},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
            if not info or info.get("regularMarketPrice") is None:
                return None

            price = info.get("regularMarketPrice", 0)
            prev_close = info.get("previousClose", price)
            change = price - prev_close if prev_close else 0
            change_pct = (change / prev_close * 100) if prev_close else 0

            rmt = info.get("regularMarketTime")
            regular_market_time = (
                datetime.utcfromtimestamp(rmt).isoformat() if rmt else None
            )

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
                # Extended quote fields
                "bid": info.get("bid"),
                "bid_size": info.get("bidSize"),
                "ask": info.get("ask"),
                "ask_size": info.get("askSize"),
                "market_state": info.get("marketState"),
                "pre_market_price": info.get("preMarketPrice"),
                "pre_market_change": info.get("preMarketChange"),
                "pre_market_change_percent": info.get("preMarketChangePercent"),
                "post_market_price": info.get("postMarketPrice"),
                "post_market_change": info.get("postMarketChange"),
                "post_market_change_percent": info.get("postMarketChangePercent"),
                "regular_market_time": regular_market_time,
                "average_volume": info.get("averageVolume"),
                "average_volume_10day": info.get("averageDailyVolume10Day"),
                "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
                "fifty_two_week_low": info.get("fiftyTwoWeekLow"),
                "fifty_day_average": info.get("fiftyDayAverage"),
                "two_hundred_day_average": info.get("twoHundredDayAverage"),
                "shares_outstanding": info.get("sharesOutstanding"),
                "float_shares": info.get("floatShares"),
                "shares_short": info.get("sharesShort"),
                "short_percent_of_float": info.get("shortPercentOfFloat"),
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
            bars = await submit(
                "yfinance", yf_call, "ticker_history",
                {"symbol": symbol, "period": period, "interval": interval,
                 "start": start, "end": end},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
            if not bars:
                return None

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
            info = await submit(
                "yfinance", yf_call, "ticker_info", {"symbol": query.upper()},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
            if info and info.get("shortName"):
                return [{
                    "symbol": query.upper(),
                    "name": info.get("shortName", ""),
                    "exchange": info.get("exchange", ""),
                    "market": US,
                }]
            return []
        except Exception as e:
            logger.error("YFinance search error for %s: %s", query, e)
            return []

    # === Optional Methods ===

    async def get_info(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get company/asset info from yfinance."""
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
            info = await submit(
                "yfinance", yf_call, "ticker_info", {"symbol": symbol},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
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
        if market == METAL:
            return None

        try:
            info = await submit(
                "yfinance", yf_call, "ticker_info", {"symbol": symbol},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
            if not info:
                return None

            dividend_yield = info.get("dividendYield")
            if dividend_yield is not None:
                dividend_yield = dividend_yield / 100
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
            info = await submit(
                "yfinance", yf_call, "ticker_info", {"symbol": symbol},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
            if not info:
                return None

            recommendation = info.get("recommendationKey")
            target_mean = info.get("targetMeanPrice")
            current_price = info.get("currentPrice") or info.get("regularMarketPrice")

            if not recommendation and not target_mean:
                return None

            upside_pct = None
            if target_mean and current_price and current_price > 0:
                upside_pct = ((target_mean - current_price) / current_price) * 100

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
                "upside_pct": round(upside_pct, 2) if upside_pct else None,
                "source": "yfinance",
            }

        return await self._cached_or_fetch("analyst_ratings", symbol, fetch)

    async def get_fundamentals_bundle(
        self, symbol: str, market: str,
    ) -> dict[str, Any]:
        """Fetch financials + analyst ratings in a single ticker.info call."""
        if market == METAL:
            return {"financials": None, "analyst": None}

        try:
            info = await submit(
                "yfinance", yf_call, "ticker_info", {"symbol": symbol},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
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
            info = await submit(
                "yfinance", yf_call, "ticker_info", {"symbol": symbol},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
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
                "currentPrice": info.get("currentPrice") or info.get("regularMarketPrice"),
                "source": "yfinance",
            }

        return await self._cached_or_fetch("technical_info", symbol, fetch)

    # === Extended Methods (Institutional Data) ===

    async def get_institutional_holders(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get institutional holders for a stock (US/HK)."""

        async def fetch():
            records = await submit(
                "yfinance", yf_call, "ticker_institutional", {"symbol": symbol},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
            if not records:
                return None

            holders = []
            for row in records:
                holder = {
                    "date_reported": (
                        str(row.get("Date Reported"))[:10]
                        if row.get("Date Reported") is not None
                        else None
                    ),
                    "holder": row.get("Holder", ""),
                    "pct_held": (
                        float(row["pctHeld"])
                        if row.get("pctHeld") is not None
                        else None
                    ),
                    "shares": (
                        int(row["Shares"])
                        if row.get("Shares") is not None
                        else None
                    ),
                    "value": (
                        int(row["Value"])
                        if row.get("Value") is not None
                        else None
                    ),
                    "pct_change": (
                        float(row["pctChange"])
                        if row.get("pctChange") is not None
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

        return await self._cached_or_fetch(
            "institutional_holders", symbol, fetch
        )

    # === Market Index Methods ===

    async def get_market_index(
        self, index_symbol: str, period: str = "5d"
    ) -> Optional[Dict[str, Any]]:
        """Get market index data (history + info in one subprocess call)."""

        async def fetch():
            return await submit(
                "yfinance", yf_call, "ticker_market_index",
                {"symbol": index_symbol, "period": period},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )

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
        """Per-symbol news from yfinance."""
        if not symbol:
            return []

        try:
            from datetime import datetime as _dt

            items = await submit(
                "yfinance", yf_call, "ticker_news", {"symbol": symbol},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
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
        try:
            info = await submit(
                "yfinance", yf_call, "ticker_info", {"symbol": symbol},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
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
        except Exception as e:
            logger.warning("YFinance sector_industry error for %s: %s", symbol, e)
            return None

    # === ML Data Methods ===

    async def get_insider_transactions(
        self, symbol: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Get insider transactions (buys/sells) for ML feature extraction."""
        try:
            records = await submit(
                "yfinance", yf_call, "ticker_insider_tx", {"symbol": symbol},
                priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND,
            )
            if not records:
                return None

            results = []
            for row in records:
                text = row.get("Text", "") or ""
                tx_type = text.split(" at ")[0].split(" -")[0].strip() if text else None

                results.append({
                    "date": (
                        str(row["Start Date"])[:10]
                        if row.get("Start Date") is not None
                        else None
                    ),
                    "insider_name": row.get("Insider"),
                    "title": row.get("Position"),
                    "transaction_type": tx_type,
                    "shares": (
                        int(row["Shares"])
                        if row.get("Shares") is not None
                        else None
                    ),
                    "value": (
                        float(row["Value"])
                        if row.get("Value") is not None
                        else None
                    ),
                })
            return results or None
        except Exception as e:
            logger.warning("YFinance insider_transactions error for %s: %s", symbol, e)
            return None

    async def get_insider_purchases(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get aggregated insider purchase/sale summary for ML features."""
        try:
            records = await submit(
                "yfinance", yf_call, "ticker_insider_purchases", {"symbol": symbol},
                priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND,
            )
            if not records:
                return None

            lookup: Dict[str, Any] = {}
            for row in records:
                label = str(row.get(row.get("_first_col_key", ""), "")).strip() if row else ""
                # insider_purchases DataFrame: first col is label, second is value
                cols = [k for k in row if k != "_index"]
                if len(cols) >= 2:
                    label = str(row[cols[0]]).strip() if row.get(cols[0]) is not None else ""
                    val = row.get(cols[1])
                    if val is not None:
                        lookup[label] = val

            return {
                "purchases": lookup.get("Purchases"),
                "sales": lookup.get("Sales"),
                "net_shares": lookup.get("Net Shares Purchased (Sold)"),
                "total_held": lookup.get("Total Insider Shares Held"),
                "buy_pct": lookup.get("% Net Shares Purchased (Sold)"),
                "sell_pct": lookup.get("% Buy Shares"),
            }
        except Exception as e:
            logger.warning("YFinance insider_purchases error for %s: %s", symbol, e)
            return None

    async def get_options_sentiment(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get put/call volume and OI ratios from nearest-expiry options chain."""
        try:
            return await submit(
                "yfinance", yf_call, "ticker_options_chain", {"symbol": symbol},
                priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND,
            )
        except Exception as e:
            logger.warning("YFinance options_sentiment error for %s: %s", symbol, e)
            return None

    async def get_options_chain(
        self, symbol: str, expiry: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get full options chain for a symbol (US only)."""
        try:
            return await submit(
                "yfinance", yf_call, "ticker_options_detail",
                {"symbol": symbol, "expiry": expiry},
                priority=Priority.FRONTEND, pool=ExecutorPool.FRONTEND,
            )
        except Exception as e:
            logger.error("YFinance options_chain error for %s: %s", symbol, e)
            return None

    async def get_upgrades_downgrades(
        self, symbol: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Get analyst upgrade/downgrade history (most recent 200)."""
        try:
            records = await submit(
                "yfinance", yf_call, "ticker_upgrades", {"symbol": symbol},
                priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND,
            )
            if not records:
                return None

            results = []
            for row in records:
                results.append({
                    "date": row.get("date"),
                    "firm": row.get("Firm"),
                    "to_grade": row.get("ToGrade"),
                    "from_grade": row.get("FromGrade"),
                    "action": row.get("Action"),
                })
            return results or None
        except Exception as e:
            logger.warning("YFinance upgrades_downgrades error for %s: %s", symbol, e)
            return None

    async def get_valuation_measures(
        self, symbol: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Get quarterly valuation measures (P/E, P/B, EV/EBITDA, etc.)."""
        try:
            return await submit(
                "yfinance", yf_call, "ticker_valuation", {"symbol": symbol},
                priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND,
            )
        except Exception as e:
            logger.warning("YFinance valuation_measures error for %s: %s", symbol, e)
            return None

    async def get_short_interest(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get short interest data from ticker.info fields."""
        try:
            info = await submit(
                "yfinance", yf_call, "ticker_info", {"symbol": symbol},
                priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND,
            )
            if not info:
                return None

            fields = {
                "short_percent_of_float": info.get("shortPercentOfFloat"),
                "short_ratio": info.get("shortRatio"),
                "shares_short": info.get("sharesShort"),
                "shares_short_prior_month": info.get("sharesShortPriorMonth"),
                "short_percent_of_shares_outstanding": info.get(
                    "shortPercentOfSharesOutstanding"
                ),
                "date_short_interest": info.get("dateShortInterest"),
            }

            if all(v is None for v in fields.values()):
                return None

            dsi = fields["date_short_interest"]
            if isinstance(dsi, (int, float)):
                from datetime import datetime as _dt
                fields["date_short_interest"] = (
                    _dt.utcfromtimestamp(dsi).strftime("%Y-%m-%d")
                )

            return fields
        except Exception as e:
            logger.warning("YFinance short_interest error for %s: %s", symbol, e)
            return None
