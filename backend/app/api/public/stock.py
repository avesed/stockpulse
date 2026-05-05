"""Stock data API routes -- quotes, history, info, financials, search, batch.

All routes are protected by API key and return ApiResponse envelopes.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.core.auth import verify_api_key
from app.schemas.base import ApiResponse
from app.schemas.stock import (
    AnalystRatingsData,
    BatchDailyBarsData,
    BatchDailyBarsRequest,
    FinancialsData,
    FundHoldingsData,
    HistoryData,
    InfoData,
    InstitutionalHoldersData,
    InstitutionalHolderEntry,
    NorthboundHoldingsData,
    NorthboundHoldingEntry,
    OHLCVBar,
    OptionContract,
    OptionsChainData,
    OptionsExpiry,
    PeersData,
    PeerStock,
    QuoteData,
    SearchItem,
    SymbolBarsResult,
)
from app.services.stock_router import get_stock_router

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/v1/data",
    tags=["stock"],
    dependencies=[Depends(verify_api_key)],
)


# --- Request models ---

class BatchQuotesRequest(BaseModel):
    symbols: list[str]


# --- Routes ---


@router.get("/quote/{symbol}", response_model=ApiResponse[QuoteData])
async def get_quote(
    symbol: str,
    market: str = Query("us"),
):
    """Get real-time quote for a symbol."""
    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_quote(symbol, market=market)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No quote data for {symbol}",
            elapsed_ms=elapsed,
        )

    source = data.get("source", "unknown")
    quote = QuoteData(
        symbol=data.get("symbol", symbol),
        name=data.get("name"),
        price=data.get("price", 0),
        change=data.get("change", 0),
        change_percent=data.get("change_percent", 0),
        volume=data.get("volume"),
        market_cap=data.get("market_cap"),
        day_high=data.get("high") or data.get("day_high"),
        day_low=data.get("low") or data.get("day_low"),
        open=data.get("open"),
        previous_close=data.get("prev_close") or data.get("previous_close"),
        timestamp=data.get("timestamp"),
        market=data.get("market", market),
        currency=data.get("currency"),
        source=source,
        bid=data.get("bid"),
        bid_size=data.get("bid_size"),
        ask=data.get("ask"),
        ask_size=data.get("ask_size"),
        market_state=data.get("market_state"),
        pre_market_price=data.get("pre_market_price"),
        pre_market_change=data.get("pre_market_change"),
        pre_market_change_percent=data.get("pre_market_change_percent"),
        post_market_price=data.get("post_market_price"),
        post_market_change=data.get("post_market_change"),
        post_market_change_percent=data.get("post_market_change_percent"),
        regular_market_time=data.get("regular_market_time"),
        average_volume=data.get("average_volume"),
        average_volume_10day=data.get("average_volume_10day"),
        fifty_two_week_high=data.get("fifty_two_week_high"),
        fifty_two_week_low=data.get("fifty_two_week_low"),
        fifty_day_average=data.get("fifty_day_average"),
        two_hundred_day_average=data.get("two_hundred_day_average"),
        shares_outstanding=data.get("shares_outstanding"),
        float_shares=data.get("float_shares"),
        shares_short=data.get("shares_short"),
        short_percent_of_float=data.get("short_percent_of_float"),
    )

    return ApiResponse(
        data=quote,
        source=source,
        elapsed_ms=elapsed,
    )


@router.get("/history/{symbol}", response_model=ApiResponse[HistoryData])
async def get_history(
    symbol: str,
    period: str = Query("1y"),
    interval: str = Query("1d"),
    market: str = Query("us"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Get historical OHLCV data for a symbol.

    Uses the 3-layer cache (disk + DB) when the interval is supported.
    Falls back to direct provider fetch on cache miss or error.
    """
    from app.services.history_cache_service import (
        PERIOD_DAYS,
        RESAMPLE_MAP,
        get_history_cache_service,
    )
    from app.providers.constants import detect_market as _detect_market

    t0 = time.monotonic()

    # --- Try the cache service for supported intervals ---
    if interval in RESAMPLE_MAP:
        try:
            # Resolve period to day count
            period_days = PERIOD_DAYS.get(period, 365)

            # Detect market from symbol (more reliable than query param for
            # symbols like 600519.SS where the caller might pass "us")
            detected_market = _detect_market(symbol)

            cache_svc = await get_history_cache_service()
            cached_bars = await cache_svc.get_history(
                symbol=symbol,
                interval=interval,
                period_days=period_days,
                market=detected_market,
                start=start,
                end=end,
            )

            if cached_bars:
                elapsed = int((time.monotonic() - t0) * 1000)
                bars = [
                    OHLCVBar(
                        date=b.get("date", ""),
                        open=b.get("open", 0),
                        high=b.get("high", 0),
                        low=b.get("low", 0),
                        close=b.get("close", 0),
                        volume=b.get("volume"),
                    )
                    for b in cached_bars
                ]
                history = HistoryData(
                    symbol=symbol,
                    bars=bars,
                    interval=interval,
                    market=detected_market,
                )
                return ApiResponse(
                    data=history,
                    source="cache",
                    elapsed_ms=elapsed,
                )

            # cached_bars is None -- fall through to direct provider fetch
            logger.debug(
                "Cache returned None for %s/%s, falling back to provider",
                symbol, interval,
            )

        except Exception as exc:
            logger.warning(
                "Cache service error for %s/%s, falling back to provider: %s",
                symbol, interval, exc,
            )

    # --- Fallback: direct provider fetch (original behavior) ---
    sr = await get_stock_router()
    data = await sr.get_history(
        symbol, period=period, interval=interval,
        market=market, start=start, end=end,
    )
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No history data for {symbol}",
            elapsed_ms=elapsed,
        )

    bars = [
        OHLCVBar(
            date=b.get("date", ""),
            open=b.get("open", 0),
            high=b.get("high", 0),
            low=b.get("low", 0),
            close=b.get("close", 0),
            volume=b.get("volume"),
        )
        for b in data.get("bars", [])
    ]

    history = HistoryData(
        symbol=data.get("symbol", symbol),
        bars=bars,
        interval=data.get("interval", interval),
        market=data.get("market", market),
    )

    return ApiResponse(
        data=history,
        source=data.get("source"),
        elapsed_ms=elapsed,
    )


@router.get("/info/{symbol}", response_model=ApiResponse[InfoData])
async def get_info(
    symbol: str,
    market: str = Query("us"),
):
    """Get company / instrument information."""
    t0 = time.monotonic()
    sr = await get_stock_router()
    data = await sr.get_info(symbol, market=market)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No info data for {symbol}",
            elapsed_ms=elapsed,
        )

    source = data.get("source", "unknown")
    info = InfoData(
        symbol=data.get("symbol", symbol),
        name=data.get("name", ""),
        description=data.get("description"),
        sector=data.get("sector"),
        industry=data.get("industry"),
        website=data.get("website"),
        employees=data.get("employees"),
        market_cap=data.get("market_cap"),
        currency=data.get("currency"),
        exchange=data.get("exchange"),
        market=data.get("market", market),
        source=source,
    )

    return ApiResponse(
        data=info,
        source=source,
        elapsed_ms=elapsed,
    )


@router.get(
    "/financials/{symbol}",
    response_model=ApiResponse[FinancialsData],
)
async def get_financials(
    symbol: str,
    market: str = Query("us"),
):
    """Get key financial metrics and ratios.

    DB-first strategy: queries stock_fundamentals table for fresh data
    (<48h). Falls back to live provider API on cache miss.
    """
    from app.services.fundamentals_db_service import get_financials_from_db

    t0 = time.monotonic()

    # --- DB-first path ---
    try:
        db_data = await get_financials_from_db(symbol)
    except Exception:
        logger.warning("Unexpected error in DB financials for %s, falling back", symbol, exc_info=True)
        db_data = None

    if db_data is not None:
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.debug("Financials for %s served from DB in %dms", symbol, elapsed)
        financials = FinancialsData(
            symbol=db_data.get("symbol", symbol),
            pe_ratio=db_data.get("pe_ratio"),
            forward_pe=db_data.get("forward_pe"),
            eps=db_data.get("eps"),
            dividend_yield=db_data.get("dividend_yield"),
            dividend_rate=db_data.get("dividend_rate"),
            book_value=db_data.get("book_value"),
            price_to_book=db_data.get("price_to_book"),
            revenue=db_data.get("revenue"),
            revenue_growth=db_data.get("revenue_growth"),
            net_income=db_data.get("net_income"),
            profit_margin=db_data.get("profit_margin"),
            gross_margin=db_data.get("gross_margin"),
            operating_margin=db_data.get("operating_margin"),
            roe=db_data.get("roe"),
            roa=db_data.get("roa"),
            debt_to_equity=db_data.get("debt_to_equity"),
            current_ratio=db_data.get("current_ratio"),
            eps_growth=db_data.get("eps_growth"),
            payout_ratio=db_data.get("payout_ratio"),
            market=db_data.get("market", market),
            source="db",
        )
        return ApiResponse(
            data=financials,
            source="db",
            elapsed_ms=elapsed,
        )

    # --- Fallback: live provider API ---
    logger.debug("Financials for %s: DB miss, falling back to live API", symbol)
    sr = await get_stock_router()
    data = await sr.get_financials(symbol, market=market)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No financials data for {symbol}",
            elapsed_ms=elapsed,
        )

    source = data.get("source", "unknown")
    financials = FinancialsData(
        symbol=data.get("symbol", symbol),
        pe_ratio=data.get("pe_ratio"),
        forward_pe=data.get("forward_pe"),
        eps=data.get("eps"),
        dividend_yield=data.get("dividend_yield"),
        dividend_rate=data.get("dividend_rate"),
        book_value=data.get("book_value"),
        price_to_book=data.get("price_to_book"),
        revenue=data.get("revenue"),
        revenue_growth=data.get("revenue_growth"),
        net_income=data.get("net_income"),
        profit_margin=data.get("profit_margin"),
        gross_margin=data.get("gross_margin"),
        operating_margin=data.get("operating_margin"),
        roe=data.get("roe"),
        roa=data.get("roa"),
        debt_to_equity=data.get("debt_to_equity"),
        current_ratio=data.get("current_ratio"),
        eps_growth=data.get("eps_growth"),
        payout_ratio=data.get("payout_ratio"),
        market=data.get("market", market),
        source=source,
    )

    return ApiResponse(
        data=financials,
        source=source,
        elapsed_ms=elapsed,
    )


@router.get(
    "/analyst-ratings/{symbol}",
    response_model=ApiResponse[AnalystRatingsData],
)
async def get_analyst_ratings(
    symbol: str,
    market: str = Query("us"),
):
    """Get analyst ratings and price targets. DB-first with live fallback."""
    t0 = time.monotonic()

    # DB-first
    from app.services.fundamentals_db_service import get_analyst_ratings_from_db
    db_data = await get_analyst_ratings_from_db(symbol)

    if db_data is not None:
        elapsed = int((time.monotonic() - t0) * 1000)
        return ApiResponse(
            data=AnalystRatingsData(**db_data),
            source="db",
            elapsed_ms=elapsed,
        )

    # Live fallback
    sr = await get_stock_router()
    provider = sr.get_provider_by_name("yfinance")
    if provider is None:
        elapsed = int((time.monotonic() - t0) * 1000)
        return ApiResponse(data=None, source="none", elapsed_ms=elapsed)

    data = await provider.get_analyst_ratings(symbol)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(data=None, source="none", elapsed_ms=elapsed)

    return ApiResponse(
        data=AnalystRatingsData(
            symbol=data.get("symbol", symbol),
            recommendation=data.get("recommendation"),
            recommendation_mean=data.get("recommendation_mean"),
            target_mean_price=data.get("target_mean_price"),
            target_high_price=data.get("target_high_price"),
            target_low_price=data.get("target_low_price"),
            target_median_price=data.get("target_median_price"),
            number_of_analysts=data.get("number_of_analysts"),
            current_price=data.get("current_price"),
            upside_pct=data.get("upside_pct"),
            market=market,
            source=data.get("source", "yfinance"),
        ),
        source=data.get("source", "yfinance"),
        elapsed_ms=elapsed,
    )


@router.get(
    "/northbound/{symbol}",
    response_model=ApiResponse[NorthboundHoldingsData],
)
async def get_northbound_holdings(
    symbol: str,
    days: int = Query(30, ge=1, le=365),
):
    """Get northbound capital flow data for CN stocks. DB-first with live fallback."""
    t0 = time.monotonic()

    # DB-first
    from app.services.fundamentals_db_service import get_northbound_holdings_from_db
    db_data = await get_northbound_holdings_from_db(symbol, days)

    if db_data is not None:
        elapsed = int((time.monotonic() - t0) * 1000)
        holdings = [NorthboundHoldingEntry(**h) for h in db_data]
        return ApiResponse(
            data=NorthboundHoldingsData(symbol=symbol, holdings=holdings, source="db"),
            source="db",
            elapsed_ms=elapsed,
        )

    # Live fallback
    from app.providers.akshare_provider import AKShareProvider
    provider = AKShareProvider()
    data = await provider.get_northbound_holding(symbol, days=days)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None or not data.get("holdings"):
        return ApiResponse(
            data=NorthboundHoldingsData(symbol=symbol, holdings=[], source="none"),
            source="none",
            elapsed_ms=elapsed,
        )

    holdings = []
    for h in data["holdings"]:
        holdings.append(NorthboundHoldingEntry(
            date=str(h.get("holding_date", h.get("date", ""))),
            close_price=h.get("close_price"),
            holding_shares=h.get("holding_shares"),
            holding_value=h.get("holding_value"),
            holding_pct=h.get("holding_pct"),
            change_shares=h.get("change_shares"),
        ))

    return ApiResponse(
        data=NorthboundHoldingsData(symbol=symbol, holdings=holdings, source="akshare"),
        source="akshare",
        elapsed_ms=elapsed,
    )


@router.get(
    "/institutional-holders/{symbol}",
    response_model=ApiResponse[InstitutionalHoldersData],
)
async def get_institutional_holders(
    symbol: str,
    market: str = Query("us"),
):
    """Get institutional holders. DB-first with live fallback."""
    t0 = time.monotonic()

    from app.services.fundamentals_db_service import get_institutional_holders_from_db
    db_data = await get_institutional_holders_from_db(symbol)

    if db_data is not None:
        elapsed = int((time.monotonic() - t0) * 1000)
        holders = [InstitutionalHolderEntry(**h) for h in db_data["holders"]]
        return ApiResponse(
            data=InstitutionalHoldersData(
                symbol=symbol, holders=holders,
                total_institutional_pct=db_data.get("total_institutional_pct"),
                source="db",
            ),
            source="db", elapsed_ms=elapsed,
        )

    # Live fallback
    from app.providers.yfinance_provider import YFinanceProvider
    provider = YFinanceProvider()
    data = await provider.get_institutional_holders(symbol)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None or not data.get("holders"):
        return ApiResponse(
            data=InstitutionalHoldersData(symbol=symbol),
            source="none", elapsed_ms=elapsed,
        )

    holders = [InstitutionalHolderEntry(**h) for h in data["holders"]]
    return ApiResponse(
        data=InstitutionalHoldersData(
            symbol=symbol, holders=holders,
            total_institutional_pct=data.get("total_institutional_pct"),
            source="yfinance",
        ),
        source="yfinance", elapsed_ms=elapsed,
    )


@router.get(
    "/fund-holdings/{symbol}",
    response_model=ApiResponse[FundHoldingsData],
)
async def get_fund_holdings(symbol: str):
    """Get fund holdings for CN stocks. DB-first with live fallback."""
    t0 = time.monotonic()

    from app.services.fundamentals_db_service import get_fund_holdings_from_db
    db_data = await get_fund_holdings_from_db(symbol)

    if db_data is not None:
        elapsed = int((time.monotonic() - t0) * 1000)
        return ApiResponse(data=FundHoldingsData(**db_data), source="db", elapsed_ms=elapsed)

    # Live fallback
    from app.providers.akshare_provider import AKShareProvider
    provider = AKShareProvider()
    data = await provider.get_fund_holdings_cn(symbol)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None or not data.get("holdings") or not isinstance(data["holdings"], dict):
        return ApiResponse(data=FundHoldingsData(symbol=symbol), source="none", elapsed_ms=elapsed)

    h = data["holdings"]
    return ApiResponse(
        data=FundHoldingsData(
            symbol=symbol, quarter=data.get("quarter"),
            institution_count=h.get("institution_count"),
            institution_count_change=h.get("institution_count_change"),
            holding_pct=h.get("holding_pct"), holding_pct_change=h.get("holding_pct_change"),
            float_pct=h.get("float_pct"), float_pct_change=h.get("float_pct_change"),
            source="akshare",
        ),
        source="akshare", elapsed_ms=elapsed,
    )


@router.get(
    "/peers/{symbol}",
    response_model=ApiResponse[PeersData],
)
async def get_peers(
    symbol: str,
    limit: int = Query(20, ge=1, le=50),
):
    """Get peer stocks by industry matching from stock_profiles."""
    t0 = time.monotonic()

    from app.services.fundamentals_db_service import get_peers_from_db
    data = await get_peers_from_db(symbol, limit)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(data=PeersData(symbol=symbol), source="none", elapsed_ms=elapsed)

    peers = [PeerStock(**p) for p in data["peers"]]
    return ApiResponse(
        data=PeersData(
            symbol=symbol, industry=data.get("industry"),
            sector=data.get("sector"), peers=peers, source="db",
        ),
        source="db", elapsed_ms=elapsed,
    )


@router.get(
    "/search",
    response_model=ApiResponse[list[SearchItem]],
)
async def search_stocks(
    q: str = Query(..., min_length=1),
    markets: str = Query("us,hk,sh,sz,metal"),
):
    """Search for stocks across markets."""
    t0 = time.monotonic()
    market_list = [m.strip().lower() for m in markets.split(",") if m.strip()]
    sr = await get_stock_router()
    results = await sr.search(q, markets=market_list)
    elapsed = int((time.monotonic() - t0) * 1000)

    items = [
        SearchItem(
            symbol=r.get("symbol", ""),
            name=r.get("name", ""),
            exchange=r.get("exchange"),
            market=r.get("market", "us"),
        )
        for r in results
    ]

    return ApiResponse(
        data=items,
        source="mixed",
        elapsed_ms=elapsed,
    )


@router.post(
    "/batch/quotes",
    response_model=ApiResponse[dict],
)
async def batch_quotes(body: BatchQuotesRequest):
    """Get quotes for multiple symbols in parallel."""
    t0 = time.monotonic()
    sr = await get_stock_router()

    async def _fetch_one(sym: str):
        try:
            return sym, await sr.get_quote(sym)
        except Exception as e:
            logger.warning("Batch quote error for %s: %s", sym, e)
            return sym, None

    tasks = [_fetch_one(s) for s in body.symbols[:50]]  # cap at 50
    results = await asyncio.gather(*tasks)
    elapsed = int((time.monotonic() - t0) * 1000)

    quotes = {}
    for sym, data in results:
        if data is not None:
            quotes[sym] = data

    return ApiResponse(
        data={"quotes": quotes, "count": len(quotes)},
        source="mixed",
        elapsed_ms=elapsed,
    )


@router.post(
    "/batch/daily-bars",
    response_model=ApiResponse[BatchDailyBarsData],
)
async def batch_daily_bars(body: BatchDailyBarsRequest):
    """Fetch daily OHLCV bars for up to 50 symbols.

    Used by the backend's daily bar collection tasks to fetch bars
    from yfinance (US/HK/Metal) or akshare (CN).
    """
    from app.services.daily_bar_service import DailyBarFetcher

    t0 = time.monotonic()

    # Cap at 50 symbols
    entries = [
        {"symbol": s.symbol, "start_date": s.start_date}
        for s in body.symbols[:50]
    ]

    fetcher = DailyBarFetcher()
    raw_results, raw_errors = await fetcher.fetch_batch(entries, market=body.market)

    # Convert raw dicts to Pydantic models
    results: dict[str, SymbolBarsResult] = {}
    for symbol, data in raw_results.items():
        bars = [
            OHLCVBar(
                date=b["date"],
                open=b["open"],
                high=b["high"],
                low=b["low"],
                close=b["close"],
                volume=b.get("volume"),
            )
            for b in data.get("bars", [])
        ]
        results[symbol] = SymbolBarsResult(
            bars=bars,
            source=data.get("source", "unknown"),
        )

    elapsed = int((time.monotonic() - t0) * 1000)
    logger.info(
        "batch_daily_bars: market=%s, requested=%d, returned=%d, errors=%d, elapsed=%dms",
        body.market, len(entries), len(results), len(raw_errors), elapsed,
    )

    response_data = BatchDailyBarsData(
        results=results,
        errors=raw_errors,
    )

    return ApiResponse(
        data=response_data,
        source="mixed",
        elapsed_ms=elapsed,
    )


@router.get(
    "/options/{symbol}",
    response_model=ApiResponse[OptionsChainData],
)
async def get_options_chain(
    symbol: str,
    expiry: Optional[str] = Query(None, description="Expiry date YYYY-MM-DD; defaults to nearest"),
):
    """Get options chain for a US stock symbol."""
    from app.providers.yfinance_provider import YFinanceProvider

    t0 = time.monotonic()
    provider = YFinanceProvider()
    data = await provider.get_options_chain(symbol, expiry=expiry)
    elapsed = int((time.monotonic() - t0) * 1000)

    if data is None:
        return ApiResponse(
            success=False,
            error=f"No options data for {symbol}",
            elapsed_ms=elapsed,
        )

    chain = None
    if data.get("chain"):
        c = data["chain"]
        chain = OptionsExpiry(
            expiry=c["expiry"],
            calls=[OptionContract(**contract) for contract in c.get("calls", [])],
            puts=[OptionContract(**contract) for contract in c.get("puts", [])],
        )

    options_data = OptionsChainData(
        symbol=data.get("symbol", symbol),
        expiries=data.get("expiries", []),
        chain=chain,
        source="yfinance",
    )

    return ApiResponse(
        data=options_data,
        source="yfinance",
        elapsed_ms=elapsed,
    )
