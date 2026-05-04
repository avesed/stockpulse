"""ML data collection service — 14 collection jobs for valuation, insider,
earnings, sentiment, macro, and alternative data.

Each job follows the same pattern as fundamentals_collection_service:
- Redis lock: ``sp:ml:{job_type}:{market}:lock`` with TTL
- Progress: ``sp:ml:{job_type}:{market}:progress`` JSON in Redis
- Audit: collection_runs record per execution
- Upsert: raw asyncpg SQL with ``ON CONFLICT DO UPDATE``

Jobs:
 1. valuation_history      — Finnhub per-symbol
 2. insider_sentiment       — Finnhub per-symbol
 3. insider_transactions    — YFinance per-symbol
 4. earnings_surprises      — Finnhub per-symbol
 5. recommendation_trends   — Finnhub per-symbol
 6. upgrades_downgrades     — YFinance per-symbol
 7. sec_financials           — Finnhub per-symbol
 8. earnings_calendar        — Finnhub global
 9. options_sentiment        — YFinance per-symbol (US only)
10. short_interest           — YFinance per-symbol (US only)
11. economic_indicators      — Finnhub global
12. macro_daily              — YFinance global (batch download)
13. cn_alternative           — AKShare mixed (CN only)
14. backfill_valuation       — Finnhub per-symbol (backfill variant)
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from app.core.database import get_db_pool
from app.core.provider_queue import has_high_priority_pending
from app.core.redis import get_redis
from app.services import collection_run_service
from app.services import symbol_resolver

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LOCK_TTL = 7200  # 2 hours
_PROGRESS_TTL = 3600

_FINNHUB_BATCH = 20
_FINNHUB_DELAY = 0.5
_YF_BATCH = 20
_YF_DELAY = 0.5
_AK_DELAY = 1.0

MACRO_TICKERS = {
    "^VIX": "VIX", "^TNX": "10Y_Treasury", "^TYX": "30Y_Treasury",
    "^FVX": "5Y_Treasury", "^IRX": "3M_Treasury", "DX-Y.NYB": "DXY",
    "GC=F": "Gold", "CL=F": "Oil_WTI", "BZ=F": "Oil_Brent",
    "HG=F": "Copper", "NG=F": "NatGas", "BTC-USD": "Bitcoin",
    "^GSPC": "SP500", "^IXIC": "Nasdaq", "^RUT": "Russell2000",
    "EURUSD=X": "EUR_USD", "CNY=X": "USD_CNY", "JPY=X": "USD_JPY",
}

ECONOMIC_CODES = {
    "MA-USA-656880": "consumer_confidence",
}

_RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""

# Finnhub valuation field name -> DB column name
_VAL_FIELD_MAP = {
    "pe": "pe_ratio",
    "pb": "pb_ratio",
    "ps": "ps_ratio",
    "evEbitda": "ev_to_ebitda",
    "evRevenue": "ev_to_revenue",
    "roe": "roe",
    "roa": "roa",
    "roic": "roic",
    "fcfMargin": "fcf_margin",
    "netMargin": "net_margin",
    "operatingMargin": "operating_margin",
    "grossMargin": "gross_margin",
    "currentRatio": "current_ratio",
    "quickRatio": "quick_ratio",
    "payoutRatio": "payout_ratio",
    "bookValue": "book_value",
    "eps": "eps",
    "ev": "ev",
}


# ---------------------------------------------------------------------------
# Generic job runner
# ---------------------------------------------------------------------------

async def _run_job(
    job_type: str,
    market: str,
    per_symbol_fn,
    provider_name: str,
    triggered_by: str,
    batch_size: int | None = None,
    delay: float | None = None,
) -> dict[str, Any]:
    """Generic collection job backbone with lock / progress / audit."""
    lock_key = f"sp:ml:{job_type}:{market}:lock"
    progress_key = f"sp:ml:{job_type}:{market}:progress"

    owner = await _acquire_lock(lock_key)
    if owner is None:
        return {"symbol_count": 0, "errors": ["Already running"]}

    if batch_size is None:
        batch_size = _FINNHUB_BATCH if provider_name == "finnhub" else _YF_BATCH
    if delay is None:
        delay = _FINNHUB_DELAY if provider_name == "finnhub" else _YF_DELAY

    run_id: int | None = None
    run_finalized = False
    started_at = datetime.now(timezone.utc)
    try:
        run = await collection_run_service.create_run(market, f"ml_{job_type}", triggered_by)
        run_id = run.id
        started_at = run.started_at or started_at
    except Exception:
        logger.warning("Failed to create ml_%s run record for %s", job_type, market)

    try:
        symbols = await symbol_resolver.get_symbols(market)
        if not symbols:
            if run_id:
                await collection_run_service.fail_run(run_id, "No symbols")
            return {"symbol_count": 0, "errors": ["No symbols"]}

        total = len(symbols)
        logger.info("[ml_%s/%s] started: %d symbols", job_type, market, total)

        log_interval = max(total // 10, 100)
        pool = get_db_pool()
        today = datetime.now(timezone.utc).date()
        errors: list[dict] = []
        done = 0
        upserted = 0

        # Lazy provider init
        if provider_name == "finnhub":
            from app.providers.finnhub_provider import FinnhubProvider
            provider = FinnhubProvider()
        elif provider_name == "yfinance":
            from app.providers.yfinance_provider import YFinanceProvider
            provider = YFinanceProvider()
        else:
            from app.providers.akshare_provider import AKShareProvider
            provider = AKShareProvider()

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i: i + batch_size]
            for sym in batch:
                try:
                    count = await per_symbol_fn(pool, sym, provider, today)
                    upserted += count
                except Exception as exc:
                    errors.append({"symbol": sym, "error": str(exc), "category": "fetch"})
                done += 1

            await _update_progress(
                progress_key, done, total, started_at, len(errors),
                upserted=upserted, job_type=f"ml_{job_type}", market=market,
            )

            if done % log_interval < batch_size:
                elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
                eta = round(elapsed * (total - done) / done, 0) if done > 0 else 0
                logger.info(
                    "[ml_%s/%s] %d/%d (%d%%) upserted=%d errors=%d elapsed=%ds ETA=%ds",
                    job_type, market, done, total,
                    int(done * 100 / total), upserted, len(errors),
                    int(elapsed), int(eta),
                )

            # Yield to high-priority requests
            if has_high_priority_pending(provider_name):
                await asyncio.sleep(0.1)

            if i + batch_size < len(symbols):
                await asyncio.sleep(delay)

        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.info(
            "[ml_%s/%s] complete: %d symbols, upserted=%d, errors=%d, %.0fs",
            job_type, market, total, upserted, len(errors), elapsed,
        )

        if run_id:
            try:
                await collection_run_service.complete_run(
                    run_id, len(symbols), done, upserted, errors,
                )
                run_finalized = True
            except Exception:
                pass

        return {"symbol_count": len(symbols), "upserted": upserted, "errors": errors}

    except Exception as exc:
        logger.exception("ml_%s failed for market=%s: %s", job_type, market, exc)
        if run_id:
            try:
                await collection_run_service.fail_run(run_id, str(exc))
                run_finalized = True
            except Exception:
                pass
        return {"symbol_count": 0, "errors": [{"symbol": "", "error": str(exc), "category": "fatal"}]}
    finally:
        if run_id and not run_finalized:
            try:
                await collection_run_service.fail_run(run_id, "interrupted")
            except Exception:
                pass
        await _clear_progress(progress_key)
        await _release_lock(lock_key, owner)


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

async def _upsert_valuation(pool, symbol: str, market: str, records: list[dict]) -> int:
    """Upsert valuation history records."""
    if not records:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in records:
                await conn.execute(
                    """
                    INSERT INTO valuation_history
                        (symbol, market, date, period_type,
                         pe_ratio, pb_ratio, ps_ratio, ev_to_ebitda, ev_to_revenue,
                         roe, roa, roic, fcf_margin, net_margin,
                         operating_margin, gross_margin, current_ratio, quick_ratio,
                         payout_ratio, book_value, eps, ev, data_source)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
                    ON CONFLICT (symbol, date, period_type) DO UPDATE SET
                        market=EXCLUDED.market,
                        pe_ratio=EXCLUDED.pe_ratio, pb_ratio=EXCLUDED.pb_ratio,
                        ps_ratio=EXCLUDED.ps_ratio, ev_to_ebitda=EXCLUDED.ev_to_ebitda,
                        ev_to_revenue=EXCLUDED.ev_to_revenue,
                        roe=EXCLUDED.roe, roa=EXCLUDED.roa, roic=EXCLUDED.roic,
                        fcf_margin=EXCLUDED.fcf_margin, net_margin=EXCLUDED.net_margin,
                        operating_margin=EXCLUDED.operating_margin, gross_margin=EXCLUDED.gross_margin,
                        current_ratio=EXCLUDED.current_ratio, quick_ratio=EXCLUDED.quick_ratio,
                        payout_ratio=EXCLUDED.payout_ratio, book_value=EXCLUDED.book_value,
                        eps=EXCLUDED.eps, ev=EXCLUDED.ev, data_source=EXCLUDED.data_source
                    """,
                    symbol, market, r["date"], r["period_type"],
                    r.get("pe_ratio"), r.get("pb_ratio"), r.get("ps_ratio"),
                    r.get("ev_to_ebitda"), r.get("ev_to_revenue"),
                    r.get("roe"), r.get("roa"), r.get("roic"),
                    r.get("fcf_margin"), r.get("net_margin"),
                    r.get("operating_margin"), r.get("gross_margin"),
                    r.get("current_ratio"), r.get("quick_ratio"),
                    r.get("payout_ratio"), r.get("book_value"),
                    r.get("eps"), r.get("ev"), "finnhub",
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert valuation %s failed: %s", symbol, exc)
    return count


async def _upsert_insider_sentiment(pool, symbol: str, records: list[dict]) -> int:
    """Upsert insider sentiment records."""
    if not records:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in records:
                month_date = date(r["year"], r["month"], 1)
                await conn.execute(
                    """
                    INSERT INTO insider_sentiment (symbol, month, mspr, change, data_source)
                    VALUES ($1,$2,$3,$4,'finnhub')
                    ON CONFLICT (symbol, month) DO UPDATE SET
                        mspr=EXCLUDED.mspr, change=EXCLUDED.change
                    """,
                    symbol, month_date, r.get("mspr"), r.get("change"),
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert insider_sentiment %s failed: %s", symbol, exc)
    return count


async def _upsert_insider_transactions(pool, symbol: str, market: str, records: list[dict]) -> int:
    """Upsert insider transactions."""
    if not records:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in records:
                tx_date = r.get("date")
                if not tx_date:
                    continue
                if isinstance(tx_date, str):
                    tx_date = date.fromisoformat(tx_date[:10])
                await conn.execute(
                    """
                    INSERT INTO insider_transactions
                        (symbol, market, date, insider_name, title, transaction_type, shares, value)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (symbol, date, insider_name, transaction_type) DO UPDATE SET
                        market=EXCLUDED.market, title=EXCLUDED.title,
                        shares=EXCLUDED.shares, value=EXCLUDED.value
                    """,
                    symbol, market, tx_date,
                    r.get("insider_name") or "Unknown",
                    r.get("title"), r.get("transaction_type"),
                    r.get("shares"), r.get("value"),
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert insider_transactions %s failed: %s", symbol, exc)
    return count


async def _upsert_earnings_surprises(pool, symbol: str, records: list[dict]) -> int:
    """Upsert earnings surprise records."""
    if not records:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in records:
                period_str = r.get("period")
                if not period_str:
                    continue
                period_date = date.fromisoformat(period_str) if isinstance(period_str, str) else period_str
                await conn.execute(
                    """
                    INSERT INTO earnings_surprises
                        (symbol, period, actual_eps, estimated_eps, surprise_pct, data_source)
                    VALUES ($1,$2,$3,$4,$5,'finnhub')
                    ON CONFLICT (symbol, period) DO UPDATE SET
                        actual_eps=EXCLUDED.actual_eps, estimated_eps=EXCLUDED.estimated_eps,
                        surprise_pct=EXCLUDED.surprise_pct
                    """,
                    symbol, period_date,
                    r.get("actual"), r.get("estimate"), r.get("surprisePercent"),
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert earnings_surprises %s failed: %s", symbol, exc)
    return count


async def _upsert_recommendation_trends(pool, symbol: str, records: list[dict]) -> int:
    """Upsert recommendation trend records."""
    if not records:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in records:
                period_str = r.get("period")
                if not period_str:
                    continue
                period_date = date.fromisoformat(period_str) if isinstance(period_str, str) else period_str
                await conn.execute(
                    """
                    INSERT INTO recommendation_trends
                        (symbol, period, strong_buy, buy, hold, sell, strong_sell)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (symbol, period) DO UPDATE SET
                        strong_buy=EXCLUDED.strong_buy, buy=EXCLUDED.buy,
                        hold=EXCLUDED.hold, sell=EXCLUDED.sell, strong_sell=EXCLUDED.strong_sell
                    """,
                    symbol, period_date,
                    r.get("strongBuy"), r.get("buy"), r.get("hold"),
                    r.get("sell"), r.get("strongSell"),
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert recommendation_trends %s failed: %s", symbol, exc)
    return count


async def _upsert_upgrades_downgrades(pool, symbol: str, market: str, records: list[dict]) -> int:
    """Upsert upgrades/downgrades."""
    if not records:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in records:
                ud_date = r.get("date")
                if not ud_date:
                    continue
                if isinstance(ud_date, str):
                    ud_date = date.fromisoformat(ud_date[:10])
                firm = r.get("firm") or "Unknown"
                await conn.execute(
                    """
                    INSERT INTO upgrades_downgrades
                        (symbol, market, date, firm, to_grade, from_grade, action)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (symbol, date, firm) DO UPDATE SET
                        market=EXCLUDED.market, to_grade=EXCLUDED.to_grade,
                        from_grade=EXCLUDED.from_grade, action=EXCLUDED.action
                    """,
                    symbol, market, ud_date, firm,
                    r.get("to_grade"), r.get("from_grade"), r.get("action"),
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert upgrades_downgrades %s failed: %s", symbol, exc)
    return count


async def _upsert_sec_financials(pool, symbol: str, records: list[dict]) -> int:
    """Upsert SEC-filed financial statements with JSONB columns."""
    if not records:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in records:
                filed_date_str = r.get("filedDate")
                filed_date = date.fromisoformat(filed_date_str[:10]) if filed_date_str else None
                # Derive period date from filed_date or construct from year/quarter
                year = r.get("year")
                quarter = r.get("quarter")
                if filed_date:
                    period_date = filed_date
                elif year and quarter:
                    # Approximate period end by quarter
                    month = quarter * 3
                    period_date = date(year, month, 28)
                else:
                    continue
                form_type = r.get("form", "10-Q")
                report = r.get("report") or {}
                import json as _json
                await conn.execute(
                    """
                    INSERT INTO sec_financials
                        (symbol, period, form_type, filed_date, year, quarter,
                         balance_sheet, income_statement, cash_flow)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (symbol, period, form_type) DO UPDATE SET
                        filed_date=EXCLUDED.filed_date, year=EXCLUDED.year,
                        quarter=EXCLUDED.quarter,
                        balance_sheet=EXCLUDED.balance_sheet,
                        income_statement=EXCLUDED.income_statement,
                        cash_flow=EXCLUDED.cash_flow
                    """,
                    symbol, period_date, form_type, filed_date, year, quarter,
                    _json.dumps(report.get("bs")) if report.get("bs") else None,
                    _json.dumps(report.get("ic")) if report.get("ic") else None,
                    _json.dumps(report.get("cf")) if report.get("cf") else None,
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert sec_financials %s failed: %s", symbol, exc)
    return count


async def _upsert_earnings_calendar(pool, records: list[dict]) -> int:
    """Upsert earnings calendar entries."""
    if not records:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in records:
                earnings_date_str = r.get("date")
                symbol = r.get("symbol")
                if not earnings_date_str or not symbol:
                    continue
                earnings_date = date.fromisoformat(earnings_date_str) if isinstance(earnings_date_str, str) else earnings_date_str
                await conn.execute(
                    """
                    INSERT INTO earnings_calendar
                        (symbol, earnings_date, eps_estimate, eps_actual,
                         revenue_estimate, revenue_actual, quarter, year)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (symbol, earnings_date) DO UPDATE SET
                        eps_estimate=EXCLUDED.eps_estimate, eps_actual=EXCLUDED.eps_actual,
                        revenue_estimate=EXCLUDED.revenue_estimate,
                        revenue_actual=EXCLUDED.revenue_actual,
                        quarter=EXCLUDED.quarter, year=EXCLUDED.year
                    """,
                    symbol, earnings_date,
                    r.get("epsEstimate"), r.get("epsActual"),
                    r.get("revenueEstimate"), r.get("revenueActual"),
                    str(r["quarter"]) if r.get("quarter") is not None else None,
                    r.get("year"),
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert earnings_calendar failed: %s", exc)
    return count


async def _upsert_options_sentiment(pool, symbol: str, today: date, data: dict) -> int:
    """Upsert options sentiment snapshot."""
    try:
        async with pool.acquire(timeout=5) as conn:
            await conn.execute(
                """
                INSERT INTO options_sentiment
                    (symbol, date, put_volume, call_volume, put_call_ratio,
                     put_oi, call_oi, put_call_oi_ratio)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (symbol, date) DO UPDATE SET
                    put_volume=EXCLUDED.put_volume, call_volume=EXCLUDED.call_volume,
                    put_call_ratio=EXCLUDED.put_call_ratio,
                    put_oi=EXCLUDED.put_oi, call_oi=EXCLUDED.call_oi,
                    put_call_oi_ratio=EXCLUDED.put_call_oi_ratio
                """,
                symbol, today,
                data.get("put_volume"), data.get("call_volume"),
                data.get("put_call_ratio"),
                data.get("put_oi"), data.get("call_oi"),
                data.get("put_call_oi_ratio"),
            )
        return 1
    except Exception as exc:
        logger.warning("Upsert options_sentiment %s failed: %s", symbol, exc)
        return 0


async def _upsert_short_interest(pool, symbol: str, today: date, data: dict) -> int:
    """Upsert short interest snapshot."""
    try:
        async with pool.acquire(timeout=5) as conn:
            await conn.execute(
                """
                INSERT INTO short_interest
                    (symbol, date, short_pct_float, short_ratio, shares_short,
                     shares_short_prior, short_pct_shares_out)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (symbol, date) DO UPDATE SET
                    short_pct_float=EXCLUDED.short_pct_float,
                    short_ratio=EXCLUDED.short_ratio,
                    shares_short=EXCLUDED.shares_short,
                    shares_short_prior=EXCLUDED.shares_short_prior,
                    short_pct_shares_out=EXCLUDED.short_pct_shares_out
                """,
                symbol, today,
                data.get("short_percent_of_float"), data.get("short_ratio"),
                data.get("shares_short"), data.get("shares_short_prior_month"),
                data.get("short_percent_of_shares_outstanding"),
            )
        return 1
    except Exception as exc:
        logger.warning("Upsert short_interest %s failed: %s", symbol, exc)
        return 0


async def _upsert_economic_indicators(pool, code: str, name: str, records: list[dict]) -> int:
    """Upsert economic indicator data points."""
    if not records:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in records:
                d = r.get("date")
                if not d:
                    continue
                if isinstance(d, str):
                    d = date.fromisoformat(d)
                await conn.execute(
                    """
                    INSERT INTO economic_indicators
                        (indicator_code, indicator_name, date, value)
                    VALUES ($1,$2,$3,$4)
                    ON CONFLICT (indicator_code, date) DO UPDATE SET
                        indicator_name=EXCLUDED.indicator_name, value=EXCLUDED.value
                    """,
                    code, name, d, r.get("value"),
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert economic_indicators %s failed: %s", code, exc)
    return count


async def _upsert_macro_daily(pool, ticker: str, indicator_name: str, rows: list[dict]) -> int:
    """Upsert macro daily bar records."""
    if not rows:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in rows:
                await conn.execute(
                    """
                    INSERT INTO macro_daily
                        (ticker, indicator_name, date, open, high, low, close, volume)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (ticker, date) DO UPDATE SET
                        indicator_name=EXCLUDED.indicator_name,
                        open=EXCLUDED.open, high=EXCLUDED.high,
                        low=EXCLUDED.low, close=EXCLUDED.close,
                        volume=EXCLUDED.volume
                    """,
                    ticker, indicator_name, r["date"],
                    r.get("open"), r.get("high"), r.get("low"),
                    r["close"], r.get("volume"),
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert macro_daily %s failed: %s", ticker, exc)
    return count


async def _upsert_cn_alternative(pool, data_type: str, today: date, records: list[dict]) -> int:
    """Upsert CN alternative data records."""
    if not records:
        return 0
    count = 0
    try:
        async with pool.acquire(timeout=5) as conn:
            for r in records:
                symbol = r.pop("symbol", None)
                record_date = r.pop("date", None)
                if record_date and isinstance(record_date, str):
                    try:
                        record_date = date.fromisoformat(record_date[:10])
                    except ValueError:
                        record_date = today
                else:
                    record_date = today
                import json as _json
                await conn.execute(
                    """
                    INSERT INTO cn_alternative_data
                        (symbol, data_type, date, data)
                    VALUES ($1,$2,$3,$4)
                    ON CONFLICT (symbol, data_type, date) DO UPDATE SET
                        data=EXCLUDED.data
                    """,
                    symbol, data_type, record_date, _json.dumps(r),
                )
                count += 1
    except Exception as exc:
        logger.warning("Upsert cn_alternative %s failed: %s", data_type, exc)
    return count


# ---------------------------------------------------------------------------
# Valuation series parser
# ---------------------------------------------------------------------------

def _parse_valuation_series(series: dict, market: str) -> list[dict]:
    """Parse Finnhub valuation series into flat records.

    ``series`` has shape ``{"annual": {"pe": [{"period": "...", "v": ...}], ...},
                            "quarterly": {...}}``.
    Returns a flat list of dicts ready for upsert.
    """
    records: list[dict] = []
    for period_type in ("annual", "quarterly"):
        bucket = series.get(period_type)
        if not bucket or not isinstance(bucket, dict):
            continue
        # Collect all dates across all metrics first
        dates_data: dict[str, dict] = {}  # period_date -> {col: value}
        for fh_field, db_col in _VAL_FIELD_MAP.items():
            items = bucket.get(fh_field)
            if not items or not isinstance(items, list):
                continue
            for item in items:
                period_str = item.get("period")
                if not period_str:
                    continue
                if period_str not in dates_data:
                    dates_data[period_str] = {}
                val = item.get("v")
                if val is not None:
                    dates_data[period_str][db_col] = float(val)

        for period_str, cols in dates_data.items():
            try:
                period_date = date.fromisoformat(period_str)
            except ValueError:
                continue
            record = {"date": period_date, "period_type": period_type}
            record.update(cols)
            records.append(record)

    return records


def _parse_yf_valuation_measures(measures: list[dict], market: str) -> list[dict]:
    """Convert YFinance get_valuation_measures() output to upsert records."""
    _YF_VAL_MAP = {
        "trailing_pe": "pe_ratio",
        "forward_pe": "pe_ratio",  # fallback
        "price_to_book": "pb_ratio",
        "price_to_sales": "ps_ratio",
        "ev_to_ebitda": "ev_to_ebitda",
        "ev_to_revenue": "ev_to_revenue",
        "peg_ratio": "payout_ratio",  # reuse column
        "market_cap": "market_cap",
    }
    records = []
    for m in measures:
        date_str = m.get("date")
        if not date_str:
            continue
        try:
            from datetime import datetime as _dt
            period_date = _dt.strptime(date_str, "%m/%d/%Y").date()
        except ValueError:
            try:
                period_date = date.fromisoformat(date_str)
            except ValueError:
                continue
        record: dict = {"date": period_date, "period_type": "quarterly"}
        if m.get("trailing_pe") is not None:
            record["pe_ratio"] = m["trailing_pe"]
        elif m.get("forward_pe") is not None:
            record["pe_ratio"] = m["forward_pe"]
        for src, dst in (
            ("price_to_book", "pb_ratio"), ("price_to_sales", "ps_ratio"),
            ("ev_to_ebitda", "ev_to_ebitda"), ("ev_to_revenue", "ev_to_revenue"),
            ("market_cap", "market_cap"),
        ):
            if m.get(src) is not None:
                record[dst] = m[src]
        records.append(record)
    return records


# ---------------------------------------------------------------------------
# Public collection functions (14 total)
# ---------------------------------------------------------------------------

# 1. Valuation History — Finnhub (US) / YFinance valuation_measures (CN/HK)
async def collect_valuation_history(
    market: str, *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect valuation time-series.

    US: Finnhub ``company_basic_financials`` series (25 yr, API, safe).
    CN/HK: YFinance ``get_valuation_measures()`` (5-6 quarters, scrapes
    key-statistics HTML — must run at very low concurrency to avoid
    triggering Yahoo IP ban).
    """
    market = market.lower()

    if market == "us":
        async def _per_symbol(pool, sym, provider, today):
            series = await provider.get_valuation_series(sym)
            if not series:
                return 0
            records = _parse_valuation_series(series, market)
            return await _upsert_valuation(pool, sym, market, records)

        return await _run_job("valuation_history", market, _per_symbol, "finnhub", triggered_by)

    # CN / HK — use yfinance get_valuation_measures (key-statistics scrape)
    # Very slow: batch=2, delay=10s to avoid Yahoo global IP ban
    async def _per_symbol_yf(pool, sym, provider, today):
        measures = await provider.get_valuation_measures(sym)
        if not measures:
            return 0
        records = _parse_yf_valuation_measures(measures, market)
        return await _upsert_valuation(pool, sym, market, records)

    return await _run_job(
        "valuation_history", market, _per_symbol_yf, "yfinance", triggered_by,
        batch_size=2, delay=10.0,
    )


# 2. Insider Sentiment — Finnhub per-symbol
async def collect_insider_sentiment(
    market: str, *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect monthly insider sentiment (MSPR) from Finnhub."""
    market = market.lower()
    from_date = (datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%d")
    to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    async def _per_symbol(pool, sym, provider, today):
        items = await provider.get_insider_sentiment(sym, from_date, to_date)
        if not items:
            return 0
        return await _upsert_insider_sentiment(pool, sym, items)

    return await _run_job("insider_sentiment", market, _per_symbol, "finnhub", triggered_by)


# 3. Insider Transactions — YFinance per-symbol
async def collect_insider_transactions(
    market: str, *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect insider buy/sell transactions from YFinance."""
    market = market.lower()

    async def _per_symbol(pool, sym, provider, today):
        items = await provider.get_insider_transactions(sym)
        if not items:
            return 0
        return await _upsert_insider_transactions(pool, sym, market, items)

    return await _run_job("insider_transactions", market, _per_symbol, "yfinance", triggered_by)


# 4. Earnings Surprises — Finnhub per-symbol
async def collect_earnings_surprises(
    market: str, *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect EPS actual vs estimate from Finnhub."""
    market = market.lower()

    async def _per_symbol(pool, sym, provider, today):
        items = await provider.get_earnings_surprises(sym)
        if not items:
            return 0
        return await _upsert_earnings_surprises(pool, sym, items)

    return await _run_job("earnings_surprises", market, _per_symbol, "finnhub", triggered_by)


# 5. Recommendation Trends — Finnhub per-symbol
async def collect_recommendation_trends(
    market: str, *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect analyst recommendation trends from Finnhub."""
    market = market.lower()

    async def _per_symbol(pool, sym, provider, today):
        items = await provider.get_recommendation_trends(sym)
        if not items:
            return 0
        return await _upsert_recommendation_trends(pool, sym, items)

    return await _run_job("recommendation_trends", market, _per_symbol, "finnhub", triggered_by)


# 6. Upgrades / Downgrades — YFinance per-symbol
async def collect_upgrades_downgrades(
    market: str, *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect analyst upgrade/downgrade history from YFinance."""
    market = market.lower()

    async def _per_symbol(pool, sym, provider, today):
        items = await provider.get_upgrades_downgrades(sym)
        if not items:
            return 0
        return await _upsert_upgrades_downgrades(pool, sym, market, items)

    return await _run_job("upgrades_downgrades", market, _per_symbol, "yfinance", triggered_by)


# 7. SEC Financials — Finnhub per-symbol
async def collect_sec_financials(
    market: str, *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect SEC-filed quarterly statements from Finnhub."""
    market = market.lower()

    async def _per_symbol(pool, sym, provider, today):
        items = await provider.get_sec_financials(sym)
        if not items:
            return 0
        return await _upsert_sec_financials(pool, sym, items)

    return await _run_job("sec_financials", market, _per_symbol, "finnhub", triggered_by)


# 8. Earnings Calendar — Finnhub global (NOT per-symbol)
async def collect_earnings_calendar(
    market: str = "us", *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect upcoming earnings calendar from Finnhub (next 30 days)."""
    lock_key = f"sp:ml:earnings_calendar:{market}:lock"
    progress_key = f"sp:ml:earnings_calendar:{market}:progress"

    owner = await _acquire_lock(lock_key)
    if owner is None:
        return {"symbol_count": 0, "errors": ["Already running"]}

    run_id: int | None = None
    run_finalized = False
    started_at = datetime.now(timezone.utc)
    try:
        run = await collection_run_service.create_run(market, "ml_earnings_calendar", triggered_by)
        run_id = run.id
        started_at = run.started_at or started_at
    except Exception:
        logger.warning("Failed to create ml_earnings_calendar run record")

    try:
        from app.providers.finnhub_provider import FinnhubProvider
        provider = FinnhubProvider()

        today = datetime.now(timezone.utc).date()
        from_date = today.isoformat()
        to_date = (today + timedelta(days=30)).isoformat()

        items = await provider.get_earnings_calendar(from_date, to_date)
        pool = get_db_pool()
        upserted = await _upsert_earnings_calendar(pool, items)

        logger.info("[ml_earnings_calendar] upserted %d entries", upserted)

        if run_id:
            try:
                await collection_run_service.complete_run(
                    run_id, len(items), len(items), upserted, [],
                )
                run_finalized = True
            except Exception:
                pass

        return {"symbol_count": len(items), "upserted": upserted, "errors": []}

    except Exception as exc:
        logger.exception("ml_earnings_calendar failed: %s", exc)
        if run_id:
            try:
                await collection_run_service.fail_run(run_id, str(exc))
                run_finalized = True
            except Exception:
                pass
        return {"symbol_count": 0, "errors": [{"symbol": "", "error": str(exc), "category": "fatal"}]}
    finally:
        if run_id and not run_finalized:
            try:
                await collection_run_service.fail_run(run_id, "interrupted")
            except Exception:
                pass
        await _clear_progress(progress_key)
        await _release_lock(lock_key, owner)


# 9. Options Sentiment — YFinance per-symbol (US only)
async def collect_options_sentiment(
    market: str = "us", *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect put/call ratios from YFinance options chain."""
    market = market.lower()
    if market != "us":
        return {"symbol_count": 0, "errors": [], "skipped": "options data is US only"}

    async def _per_symbol(pool, sym, provider, today):
        data = await provider.get_options_sentiment(sym)
        if not data:
            return 0
        return await _upsert_options_sentiment(pool, sym, today, data)

    return await _run_job("options_sentiment", market, _per_symbol, "yfinance", triggered_by)


# 10. Short Interest — YFinance per-symbol (US only)
async def collect_short_interest(
    market: str = "us", *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect short interest metrics from YFinance."""
    market = market.lower()
    if market != "us":
        return {"symbol_count": 0, "errors": [], "skipped": "short interest is US only"}

    async def _per_symbol(pool, sym, provider, today):
        data = await provider.get_short_interest(sym)
        if not data:
            return 0
        return await _upsert_short_interest(pool, sym, today, data)

    return await _run_job("short_interest", market, _per_symbol, "yfinance", triggered_by)


# 11. Economic Indicators — Finnhub global (NOT per-symbol)
async def collect_economic_indicators(
    market: str = "us", *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect economic indicator time-series from Finnhub."""
    lock_key = f"sp:ml:economic_indicators:{market}:lock"
    progress_key = f"sp:ml:economic_indicators:{market}:progress"

    owner = await _acquire_lock(lock_key)
    if owner is None:
        return {"symbol_count": 0, "errors": ["Already running"]}

    run_id: int | None = None
    run_finalized = False
    started_at = datetime.now(timezone.utc)
    try:
        run = await collection_run_service.create_run(market, "ml_economic_indicators", triggered_by)
        run_id = run.id
        started_at = run.started_at or started_at
    except Exception:
        logger.warning("Failed to create ml_economic_indicators run record")

    try:
        from app.providers.finnhub_provider import FinnhubProvider
        provider = FinnhubProvider()

        pool = get_db_pool()
        upserted = 0
        errors: list[dict] = []

        for code, name in ECONOMIC_CODES.items():
            try:
                items = await provider.get_economic_indicator(code)
                count = await _upsert_economic_indicators(pool, code, name, items)
                upserted += count
            except Exception as exc:
                errors.append({"symbol": code, "error": str(exc), "category": "fetch"})

        logger.info("[ml_economic_indicators] upserted %d data points", upserted)

        if run_id:
            try:
                await collection_run_service.complete_run(
                    run_id, len(ECONOMIC_CODES), len(ECONOMIC_CODES), upserted, errors,
                )
                run_finalized = True
            except Exception:
                pass

        return {"symbol_count": len(ECONOMIC_CODES), "upserted": upserted, "errors": errors}

    except Exception as exc:
        logger.exception("ml_economic_indicators failed: %s", exc)
        if run_id:
            try:
                await collection_run_service.fail_run(run_id, str(exc))
                run_finalized = True
            except Exception:
                pass
        return {"symbol_count": 0, "errors": [{"symbol": "", "error": str(exc), "category": "fatal"}]}
    finally:
        if run_id and not run_finalized:
            try:
                await collection_run_service.fail_run(run_id, "interrupted")
            except Exception:
                pass
        await _clear_progress(progress_key)
        await _release_lock(lock_key, owner)


# 12. Macro Daily — YFinance global (batch download)
async def collect_macro_daily(
    market: str = "us", *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Batch-download macro indicators via yf.download() for the last 5 days."""
    lock_key = f"sp:ml:macro_daily:{market}:lock"
    progress_key = f"sp:ml:macro_daily:{market}:progress"

    owner = await _acquire_lock(lock_key)
    if owner is None:
        return {"symbol_count": 0, "errors": ["Already running"]}

    run_id: int | None = None
    run_finalized = False
    started_at = datetime.now(timezone.utc)
    try:
        run = await collection_run_service.create_run(market, "ml_macro_daily", triggered_by)
        run_id = run.id
        started_at = run.started_at or started_at
    except Exception:
        logger.warning("Failed to create ml_macro_daily run record")

    try:
        from app.core.provider_queue import Priority, submit
        from app.core.executor import ExecutorPool

        tickers = list(MACRO_TICKERS.keys())

        def _download_macro():
            import yfinance as yf
            end = datetime.now(timezone.utc).date()
            start = end - timedelta(days=7)  # extra padding for market holidays
            df = yf.download(tickers, start=start.isoformat(), end=end.isoformat(), progress=False)
            return df

        df = await submit("yfinance", _download_macro, priority=Priority.SCHEDULED, pool=ExecutorPool.BACKGROUND)

        pool = get_db_pool()
        upserted = 0
        errors: list[dict] = []

        if df is not None and not df.empty:
            import pandas as pd
            for ticker, indicator_name in MACRO_TICKERS.items():
                try:
                    rows = []
                    # Handle multi-level columns from yf.download
                    if isinstance(df.columns, pd.MultiIndex):
                        if "Close" not in df.columns.get_level_values(0):
                            continue
                        close_series = df["Close"][ticker] if ticker in df["Close"].columns else None
                        open_series = df["Open"][ticker] if "Open" in df.columns.get_level_values(0) and ticker in df["Open"].columns else None
                        high_series = df["High"][ticker] if "High" in df.columns.get_level_values(0) and ticker in df["High"].columns else None
                        low_series = df["Low"][ticker] if "Low" in df.columns.get_level_values(0) and ticker in df["Low"].columns else None
                        vol_series = df["Volume"][ticker] if "Volume" in df.columns.get_level_values(0) and ticker in df["Volume"].columns else None
                    else:
                        # Single ticker fallback
                        close_series = df.get("Close")
                        open_series = df.get("Open")
                        high_series = df.get("High")
                        low_series = df.get("Low")
                        vol_series = df.get("Volume")

                    if close_series is None:
                        continue

                    for idx in close_series.index:
                        close_val = close_series[idx]
                        if pd.isna(close_val):
                            continue
                        bar_date = idx.date() if hasattr(idx, 'date') else idx
                        row = {
                            "date": bar_date,
                            "close": float(close_val),
                            "open": float(open_series[idx]) if open_series is not None and not pd.isna(open_series[idx]) else None,
                            "high": float(high_series[idx]) if high_series is not None and not pd.isna(high_series[idx]) else None,
                            "low": float(low_series[idx]) if low_series is not None and not pd.isna(low_series[idx]) else None,
                            "volume": int(vol_series[idx]) if vol_series is not None and not pd.isna(vol_series[idx]) else None,
                        }
                        rows.append(row)

                    count = await _upsert_macro_daily(pool, ticker, indicator_name, rows)
                    upserted += count
                except Exception as exc:
                    errors.append({"symbol": ticker, "error": str(exc), "category": "parse"})

        logger.info("[ml_macro_daily] upserted %d rows for %d tickers", upserted, len(MACRO_TICKERS))

        if run_id:
            try:
                await collection_run_service.complete_run(
                    run_id, len(MACRO_TICKERS), len(MACRO_TICKERS), upserted, errors,
                )
                run_finalized = True
            except Exception:
                pass

        return {"symbol_count": len(MACRO_TICKERS), "upserted": upserted, "errors": errors}

    except Exception as exc:
        logger.exception("ml_macro_daily failed: %s", exc)
        if run_id:
            try:
                await collection_run_service.fail_run(run_id, str(exc))
                run_finalized = True
            except Exception:
                pass
        return {"symbol_count": 0, "errors": [{"symbol": "", "error": str(exc), "category": "fatal"}]}
    finally:
        if run_id and not run_finalized:
            try:
                await collection_run_service.fail_run(run_id, "interrupted")
            except Exception:
                pass
        await _clear_progress(progress_key)
        await _release_lock(lock_key, owner)


# 13. CN Alternative Data — AKShare mixed (CN only)
async def collect_cn_alternative(
    market: str = "cn", *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Collect CN alternative data: margin, stock connect, shareholder, dragon tiger, block trades."""
    market = market.lower()
    if market != "cn":
        return {"symbol_count": 0, "errors": [], "skipped": "CN alternative data is CN only"}

    lock_key = f"sp:ml:cn_alternative:{market}:lock"
    progress_key = f"sp:ml:cn_alternative:{market}:progress"

    owner = await _acquire_lock(lock_key)
    if owner is None:
        return {"symbol_count": 0, "errors": ["Already running"]}

    run_id: int | None = None
    run_finalized = False
    started_at = datetime.now(timezone.utc)
    try:
        run = await collection_run_service.create_run(market, "ml_cn_alternative", triggered_by)
        run_id = run.id
        started_at = run.started_at or started_at
    except Exception:
        logger.warning("Failed to create ml_cn_alternative run record")

    try:
        from app.providers.akshare_provider import AKShareProvider
        provider = AKShareProvider()

        pool = get_db_pool()
        today = datetime.now(timezone.utc).date()
        upserted = 0
        errors: list[dict] = []
        sources_done = 0
        sources_total = 5

        # 1. Margin trading (last 5 days)
        try:
            end_str = today.strftime("%Y%m%d")
            start_str = (today - timedelta(days=5)).strftime("%Y%m%d")
            items = await provider.get_margin_trading(start_str, end_str)
            if items:
                count = await _upsert_cn_alternative(pool, "margin_trading", today, items)
                upserted += count
        except Exception as exc:
            errors.append({"symbol": "margin_trading", "error": str(exc), "category": "fetch"})
        sources_done += 1
        await _update_progress(progress_key, sources_done, sources_total, started_at, len(errors),
                               upserted=upserted, job_type="ml_cn_alternative", market=market)
        await asyncio.sleep(_AK_DELAY)

        # 2. Stock connect flow (northbound + southbound)
        for direction in ("沪股通", "深股通"):
            try:
                items = await provider.get_stock_connect_flow(direction, days=30)
                if items:
                    data_type = f"stock_connect_{direction}"
                    count = await _upsert_cn_alternative(pool, data_type, today, items)
                    upserted += count
            except Exception as exc:
                errors.append({"symbol": f"stock_connect_{direction}", "error": str(exc), "category": "fetch"})
            await asyncio.sleep(_AK_DELAY)
        sources_done += 1
        await _update_progress(progress_key, sources_done, sources_total, started_at, len(errors),
                               upserted=upserted, job_type="ml_cn_alternative", market=market)

        # 3. Shareholder count (latest quarter)
        try:
            # Use previous quarter end as query date
            month = today.month
            if month <= 3:
                quarter_date = f"{today.year - 1}1231"
            elif month <= 6:
                quarter_date = f"{today.year}0331"
            elif month <= 9:
                quarter_date = f"{today.year}0630"
            else:
                quarter_date = f"{today.year}0930"
            items = await provider.get_shareholder_count(quarter_date)
            if items:
                count = await _upsert_cn_alternative(pool, "shareholder_count", today, items)
                upserted += count
        except Exception as exc:
            errors.append({"symbol": "shareholder_count", "error": str(exc), "category": "fetch"})
        sources_done += 1
        await _update_progress(progress_key, sources_done, sources_total, started_at, len(errors),
                               upserted=upserted, job_type="ml_cn_alternative", market=market)
        await asyncio.sleep(_AK_DELAY)

        # 4. Dragon tiger board (last 5 days)
        try:
            end_str = today.strftime("%Y%m%d")
            start_str = (today - timedelta(days=5)).strftime("%Y%m%d")
            items = await provider.get_dragon_tiger(start_str, end_str)
            if items:
                count = await _upsert_cn_alternative(pool, "dragon_tiger", today, items)
                upserted += count
        except Exception as exc:
            errors.append({"symbol": "dragon_tiger", "error": str(exc), "category": "fetch"})
        sources_done += 1
        await _update_progress(progress_key, sources_done, sources_total, started_at, len(errors),
                               upserted=upserted, job_type="ml_cn_alternative", market=market)
        await asyncio.sleep(_AK_DELAY)

        # 5. Block trades (last 5 days)
        try:
            end_str = today.strftime("%Y%m%d")
            start_str = (today - timedelta(days=5)).strftime("%Y%m%d")
            items = await provider.get_block_trades(start_str, end_str)
            if items:
                count = await _upsert_cn_alternative(pool, "block_trades", today, items)
                upserted += count
        except Exception as exc:
            errors.append({"symbol": "block_trades", "error": str(exc), "category": "fetch"})
        sources_done += 1
        await _update_progress(progress_key, sources_done, sources_total, started_at, len(errors),
                               upserted=upserted, job_type="ml_cn_alternative", market=market)

        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.info("[ml_cn_alternative] complete: upserted=%d, errors=%d, %.0fs",
                    upserted, len(errors), elapsed)

        if run_id:
            try:
                await collection_run_service.complete_run(
                    run_id, sources_total, sources_done, upserted, errors,
                )
                run_finalized = True
            except Exception:
                pass

        return {"symbol_count": sources_total, "upserted": upserted, "errors": errors}

    except Exception as exc:
        logger.exception("ml_cn_alternative failed: %s", exc)
        if run_id:
            try:
                await collection_run_service.fail_run(run_id, str(exc))
                run_finalized = True
            except Exception:
                pass
        return {"symbol_count": 0, "errors": [{"symbol": "", "error": str(exc), "category": "fatal"}]}
    finally:
        if run_id and not run_finalized:
            try:
                await collection_run_service.fail_run(run_id, "interrupted")
            except Exception:
                pass
        await _clear_progress(progress_key)
        await _release_lock(lock_key, owner)


# 14. Backfill Valuation History — Finnhub per-symbol (backfill variant)
async def backfill_valuation_history(
    market: str, *, triggered_by: str = "api",
) -> dict[str, Any]:
    """Backfill valuation history — same as collect but with BACKFILL priority."""
    market = market.lower()

    async def _per_symbol(pool, sym, provider, today):
        series = await provider.get_valuation_series(sym)
        if not series:
            return 0
        records = _parse_valuation_series(series, market)
        return await _upsert_valuation(pool, sym, market, records)

    return await _run_job(
        "backfill_valuation", market, _per_symbol, "finnhub", triggered_by,
        batch_size=3, delay=8.0,
    )


# ---------------------------------------------------------------------------
# Progress / unlock helpers (used by admin API)
# ---------------------------------------------------------------------------

async def get_progress(job_type: str, market: str) -> Optional[dict[str, Any]]:
    """Read ML collection progress from Redis."""
    try:
        r = await get_redis()
        data = await r.get(f"sp:ml:{job_type}:{market}:progress")
        if data is not None:
            return json.loads(data)
    except Exception:
        pass
    return None


async def force_unlock(job_type: str, market: str) -> bool:
    """Force-release the ML collection lock."""
    try:
        r = await get_redis()
        return (await r.delete(f"sp:ml:{job_type}:{market}:lock")) > 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

async def _acquire_lock(lock_key: str) -> Optional[str]:
    try:
        r = await get_redis()
        owner = str(uuid.uuid4())
        acquired = await r.set(lock_key, owner, nx=True, ex=_LOCK_TTL)
        return owner if acquired else None
    except Exception as exc:
        logger.error("Redis lock failed for %s: %s", lock_key, exc)
        return None


async def _release_lock(lock_key: str, owner: str) -> None:
    try:
        r = await get_redis()
        await r.eval(_RELEASE_LOCK_LUA, 1, lock_key, owner)
    except Exception:
        pass


async def _update_progress(
    progress_key: str, done: int, total: int,
    started_at: datetime, error_count: int = 0,
    upserted: int = 0,
    job_type: str = "",
    market: str = "",
) -> None:
    try:
        r = await get_redis()
        now = datetime.now(timezone.utc)
        elapsed = (now - started_at).total_seconds()
        pct = int(done * 100 / total) if total > 0 else 0
        estimated_remaining = None
        if done > 0 and done < total:
            estimated_remaining = round(elapsed * (total - done) / done, 1)

        progress = {
            "symbolsDone": done,
            "symbolsTotal": total,
            "upserted": upserted,
            "percent": pct,
            "elapsedSeconds": round(elapsed, 1),
            "errorsCount": error_count,
            "estimatedRemaining": estimated_remaining,
            "startedAt": started_at.isoformat(),
            "updatedAt": now.isoformat(),
        }
        await r.setex(progress_key, _PROGRESS_TTL, json.dumps(progress))

        if job_type and market:
            from app.ws.redis_fanout import publish_collection_progress
            from app.ws.protocol import make_collection_progress
            await publish_collection_progress(
                make_collection_progress(
                    market, done, total, upserted, pct,
                    error_count=error_count, elapsed_seconds=elapsed,
                    job_type=job_type, estimated_remaining=estimated_remaining,
                )
            )
    except Exception:
        pass


async def _clear_progress(progress_key: str) -> None:
    try:
        r = await get_redis()
        await r.delete(progress_key)
    except Exception:
        pass
