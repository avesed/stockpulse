"""Fundamentals collection pipeline — three independent jobs.

1. **Financials** (PE/EPS/ROE) — weekly, data changes quarterly
2. **Analyst ratings** — daily, analysts update frequently
3. **Northbound holdings** — daily (CN only), updated each trading day

Each job has its own lock, progress key, and run_type in collection_runs.

Redis key patterns:
- Lock: ``sp:{job_type}:{market}:lock``
- Progress: ``sp:{job_type}:{market}:progress``
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import date, datetime, timezone
from typing import Any, Optional

from app.core.database import get_db_pool
from app.core.redis import get_redis
from app.services import collection_run_service
from app.services import symbol_resolver

logger = logging.getLogger(__name__)

_LOCK_TTL = 7200  # 2 hours
_PROGRESS_TTL = 3600

_YF_BATCH = 5
_AK_BATCH = 20
_YF_DELAY = 5.0
_AK_DELAY = 0.5

_RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""

# Map provider field names → DB column names
_FIN_TO_DB = {
    "price_to_book": "pb_ratio",
    "revenue_growth": "revenue_growth_yoy",
}
_FIN_DB_COLS = [
    "pe_ratio", "pb_ratio", "roe", "roa", "profit_margin", "gross_margin",
    "operating_margin", "revenue", "revenue_growth_yoy", "net_income", "eps",
    "eps_growth", "debt_to_equity", "current_ratio", "dividend_yield",
    "dividend_rate", "forward_pe", "book_value", "payout_ratio",
]


# ---------------------------------------------------------------------------
# Public API — three independent collection jobs
# ---------------------------------------------------------------------------

async def collect_financials(
    market: str, *, triggered_by: str = "scheduler",
) -> dict[str, Any]:
    """Collect financial metrics (PE/EPS/ROE etc.) for a market. Weekly."""
    market = market.lower()
    use_yf = market in ("us", "hk", "metal")

    async def _per_symbol(pool, sym, provider, today):
        if use_yf:
            bundle = await provider.get_fundamentals_bundle(sym, market)
            fin = bundle.get("financials")
        else:
            ak_market = "sh" if sym.endswith(".SS") else "sz"
            fin = await provider.get_financials(sym, ak_market)
        if fin:
            ok = await _upsert_fundamental(pool, sym, market, today, fin)
            return 1 if ok else 0
        return 0

    return await _run_job("financials", market, _per_symbol, use_yf, triggered_by)


async def collect_analyst_ratings(
    market: str, *, triggered_by: str = "scheduler",
) -> dict[str, Any]:
    """Collect analyst ratings + price targets for a market. Daily (US/HK only)."""
    market = market.lower()
    if market not in ("us", "hk"):
        return {"symbol_count": 0, "errors": [], "skipped": "no analyst data for this market"}

    async def _per_symbol(pool, sym, provider, today):
        bundle = await provider.get_fundamentals_bundle(sym, market)
        analyst = bundle.get("analyst")
        if analyst:
            ok = await _upsert_analyst_rating(pool, sym, market, analyst, today)
            return 1 if ok else 0
        return 0

    return await _run_job("analyst_ratings", market, _per_symbol, True, triggered_by)


async def collect_northbound(
    market: str = "cn", *, triggered_by: str = "scheduler",
) -> dict[str, Any]:
    """Collect northbound capital flow holdings. Daily (CN only)."""
    market = market.lower()
    if market != "cn":
        return {"symbol_count": 0, "errors": [], "skipped": "northbound is CN only"}

    async def _per_symbol(pool, sym, provider, today):
        try:
            nb = await provider.get_northbound_holding(sym, days=5)
            if nb and nb.get("holdings"):
                return await _upsert_northbound(pool, sym, nb["holdings"])
        except Exception as exc:
            logger.debug("Northbound skipped for %s: %s", sym, exc)
        return 0

    return await _run_job("northbound", market, _per_symbol, False, triggered_by)


async def collect_institutional_holders(
    market: str, *, triggered_by: str = "scheduler",
) -> dict[str, Any]:
    """Collect institutional holders for a market. Quarterly (US/HK only)."""
    market = market.lower()
    if market not in ("us", "hk"):
        return {"symbol_count": 0, "errors": [], "skipped": "no institutional data for this market"}

    async def _per_symbol(pool, sym, provider, today):
        data = await provider.get_institutional_holders(sym)
        if data and data.get("holders"):
            return await _upsert_institutional_holders(pool, sym, market, data["holders"])
        return 0

    return await _run_job("institutional_holders", market, _per_symbol, True, triggered_by)


async def collect_fund_holdings(
    market: str = "cn", *, triggered_by: str = "scheduler",
) -> dict[str, Any]:
    """Collect fund holdings for CN stocks. Quarterly."""
    market = market.lower()
    if market != "cn":
        return {"symbol_count": 0, "errors": [], "skipped": "fund holdings is CN only"}

    async def _per_symbol(pool, sym, provider, today):
        data = await provider.get_fund_holdings_cn(sym)
        if data and data.get("holdings") and isinstance(data["holdings"], dict):
            return await _upsert_fund_holding(pool, sym, data.get("quarter", ""), data["holdings"])
        return 0

    return await _run_job("fund_holdings", market, _per_symbol, False, triggered_by)


# ---------------------------------------------------------------------------
# Progress / unlock helpers (used by admin API)
# ---------------------------------------------------------------------------

async def get_progress(job_type: str, market: str) -> Optional[dict[str, Any]]:
    """Read collection progress from Redis."""
    try:
        r = await get_redis()
        data = await r.get(f"sp:{job_type}:{market}:progress")
        if data is not None:
            return json.loads(data)
    except Exception:
        pass
    return None


async def force_unlock(job_type: str, market: str) -> bool:
    """Force-release the collection lock."""
    try:
        r = await get_redis()
        return (await r.delete(f"sp:{job_type}:{market}:lock")) > 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Generic job runner
# ---------------------------------------------------------------------------

async def _run_job(
    job_type: str,
    market: str,
    per_symbol_fn,
    use_yf: bool,
    triggered_by: str,
) -> dict[str, Any]:
    """Generic collection job backbone with lock/progress/audit."""
    lock_key = f"sp:{job_type}:{market}:lock"
    progress_key = f"sp:{job_type}:{market}:progress"

    owner = await _acquire_lock(lock_key)
    if owner is None:
        return {"symbol_count": 0, "errors": ["Already running"]}

    run_id: int | None = None
    started_at = datetime.now(timezone.utc)
    try:
        run = await collection_run_service.create_run(market, job_type, triggered_by)
        run_id = run.id
        started_at = run.started_at or started_at
    except Exception:
        logger.warning("Failed to create %s run record for %s", job_type, market)

    try:
        symbols = await symbol_resolver.get_symbols(market)
        if not symbols:
            if run_id:
                await collection_run_service.fail_run(run_id, "No symbols")
            return {"symbol_count": 0, "errors": ["No symbols"]}

        logger.info("%s collection started: market=%s, symbols=%d", job_type, market, len(symbols))

        batch_size = _YF_BATCH if use_yf else _AK_BATCH
        delay = _YF_DELAY if use_yf else _AK_DELAY

        pool = get_db_pool()
        today = datetime.now(timezone.utc).date()
        errors: list[dict] = []
        done = 0
        upserted = 0

        # Instantiate provider once
        if use_yf:
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

            await _update_progress(progress_key, done, len(symbols), started_at, len(errors))

            if i + batch_size < len(symbols):
                await asyncio.sleep(delay)

        logger.info(
            "%s complete: market=%s, symbols=%d, upserted=%d, errors=%d",
            job_type, market, len(symbols), upserted, len(errors),
        )

        if run_id:
            try:
                await collection_run_service.complete_run(
                    run_id, len(symbols), done, upserted, errors,
                )
            except Exception:
                pass

        return {"symbol_count": len(symbols), "upserted": upserted, "errors": errors}

    except Exception as exc:
        logger.exception("%s failed for market=%s: %s", job_type, market, exc)
        if run_id:
            try:
                await collection_run_service.fail_run(run_id, str(exc))
            except Exception:
                pass
        return {"symbol_count": 0, "errors": [{"symbol": "", "error": str(exc), "category": "fatal"}]}
    finally:
        await _clear_progress(progress_key)
        await _release_lock(lock_key, owner)


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

async def _upsert_fundamental(
    pool, symbol: str, market: str, today: date, data: dict,
) -> bool:
    vals: dict[str, Any] = {}
    for provider_key, db_col in _FIN_TO_DB.items():
        if provider_key in data:
            vals[db_col] = data[provider_key]
    for col in _FIN_DB_COLS:
        if col not in vals and col in data:
            vals[col] = data[col]

    source = data.get("source", "unknown")

    try:
        async with pool.acquire(timeout=5) as conn:
            await conn.execute(
                """
                INSERT INTO stock_fundamentals
                    (symbol, market, date, record_type,
                     pe_ratio, pb_ratio, roe, roa,
                     profit_margin, gross_margin, operating_margin,
                     revenue, revenue_growth_yoy, net_income, eps, eps_growth,
                     debt_to_equity, current_ratio, dividend_yield, dividend_rate,
                     forward_pe, book_value, payout_ratio, data_source)
                VALUES ($1,$2,$3,'daily_snapshot',
                        $4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
                ON CONFLICT (symbol, date, record_type) DO UPDATE SET
                    market=EXCLUDED.market,
                    pe_ratio=EXCLUDED.pe_ratio, pb_ratio=EXCLUDED.pb_ratio,
                    roe=EXCLUDED.roe, roa=EXCLUDED.roa,
                    profit_margin=EXCLUDED.profit_margin, gross_margin=EXCLUDED.gross_margin,
                    operating_margin=EXCLUDED.operating_margin,
                    revenue=EXCLUDED.revenue, revenue_growth_yoy=EXCLUDED.revenue_growth_yoy,
                    net_income=EXCLUDED.net_income, eps=EXCLUDED.eps, eps_growth=EXCLUDED.eps_growth,
                    debt_to_equity=EXCLUDED.debt_to_equity, current_ratio=EXCLUDED.current_ratio,
                    dividend_yield=EXCLUDED.dividend_yield, dividend_rate=EXCLUDED.dividend_rate,
                    forward_pe=EXCLUDED.forward_pe, book_value=EXCLUDED.book_value,
                    payout_ratio=EXCLUDED.payout_ratio, data_source=EXCLUDED.data_source
                """,
                symbol, market, today,
                vals.get("pe_ratio"), vals.get("pb_ratio"),
                vals.get("roe"), vals.get("roa"),
                vals.get("profit_margin"), vals.get("gross_margin"),
                vals.get("operating_margin"),
                vals.get("revenue"), vals.get("revenue_growth_yoy"),
                vals.get("net_income"), vals.get("eps"), vals.get("eps_growth"),
                vals.get("debt_to_equity"), vals.get("current_ratio"),
                vals.get("dividend_yield"), vals.get("dividend_rate"),
                vals.get("forward_pe"), vals.get("book_value"),
                vals.get("payout_ratio"), source,
            )
        return True
    except Exception as exc:
        logger.warning("Upsert fundamental %s failed: %s", symbol, exc)
        return False


async def _upsert_analyst_rating(pool, symbol: str, market: str, data: dict, today: date | None = None) -> bool:
    if today is None:
        today = datetime.now(timezone.utc).date()
    try:
        async with pool.acquire(timeout=5) as conn:
            await conn.execute(
                """
                INSERT INTO analyst_ratings
                    (symbol, market, date, recommendation, recommendation_mean,
                     target_mean_price, target_high_price, target_low_price,
                     target_median_price, number_of_analysts,
                     current_price, upside_pct, data_source, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
                ON CONFLICT (symbol, date) DO UPDATE SET
                    market=EXCLUDED.market,
                    recommendation=EXCLUDED.recommendation,
                    recommendation_mean=EXCLUDED.recommendation_mean,
                    target_mean_price=EXCLUDED.target_mean_price,
                    target_high_price=EXCLUDED.target_high_price,
                    target_low_price=EXCLUDED.target_low_price,
                    target_median_price=EXCLUDED.target_median_price,
                    number_of_analysts=EXCLUDED.number_of_analysts,
                    current_price=EXCLUDED.current_price,
                    upside_pct=EXCLUDED.upside_pct,
                    data_source=EXCLUDED.data_source,
                    updated_at=NOW()
                """,
                symbol, market, today,
                data.get("recommendation"), data.get("recommendation_mean"),
                data.get("target_mean_price"), data.get("target_high_price"),
                data.get("target_low_price"), data.get("target_median_price"),
                data.get("number_of_analysts"),
                data.get("current_price"), data.get("upside_pct"),
                data.get("source", "yfinance"),
            )
        return True
    except Exception as exc:
        logger.warning("Upsert analyst rating %s failed: %s", symbol, exc)
        return False


async def _upsert_northbound(pool, symbol: str, holdings: list[dict]) -> int:
    sql = """
        INSERT INTO northbound_holdings
            (symbol, date, close_price, holding_shares,
             holding_value, holding_pct, change_shares)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (symbol, date) DO UPDATE SET
            close_price=EXCLUDED.close_price,
            holding_shares=EXCLUDED.holding_shares,
            holding_value=EXCLUDED.holding_value,
            holding_pct=EXCLUDED.holding_pct,
            change_shares=EXCLUDED.change_shares
    """
    rows = []
    for h in holdings:
        hdate = h.get("holding_date") or h.get("date")
        if not hdate:
            continue
        if isinstance(hdate, str):
            hdate = date.fromisoformat(hdate)
        rows.append((
            symbol, hdate,
            h.get("close_price"), h.get("holding_shares"),
            h.get("holding_value"), h.get("holding_pct"),
            h.get("change_shares"),
        ))
    if not rows:
        return 0
    try:
        async with pool.acquire(timeout=5) as conn:
            async with conn.transaction():
                await conn.executemany(sql, rows)
        return len(rows)
    except Exception as exc:
        logger.debug("Northbound upsert %s failed: %s", symbol, exc)
        return 0


async def _upsert_institutional_holders(pool, symbol: str, market: str, holders: list[dict]) -> int:
    sql = """
        INSERT INTO institutional_holders
            (symbol, market, holder, date_reported, pct_held, shares, value, pct_change, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
        ON CONFLICT (symbol, holder) DO UPDATE SET
            market=EXCLUDED.market, date_reported=EXCLUDED.date_reported,
            pct_held=EXCLUDED.pct_held, shares=EXCLUDED.shares,
            value=EXCLUDED.value, pct_change=EXCLUDED.pct_change, updated_at=NOW()
    """
    rows = []
    for h in holders:
        holder_name = h.get("holder", "")
        if not holder_name:
            continue
        date_reported = h.get("date_reported")
        if date_reported and isinstance(date_reported, str):
            try:
                date_reported = date.fromisoformat(date_reported)
            except ValueError:
                date_reported = None
        rows.append((
            symbol, market, holder_name, date_reported,
            h.get("pct_held"), h.get("shares"), h.get("value"), h.get("pct_change"),
        ))
    if not rows:
        return 0
    try:
        async with pool.acquire(timeout=5) as conn:
            async with conn.transaction():
                await conn.executemany(sql, rows)
        return len(rows)
    except Exception as exc:
        logger.warning("Institutional holders upsert %s failed: %s", symbol, exc)
        return 0


async def _upsert_fund_holding(pool, symbol: str, quarter: str, holdings: dict) -> int:
    if not quarter:
        return 0
    try:
        async with pool.acquire(timeout=5) as conn:
            await conn.execute(
                """
                INSERT INTO fund_holdings
                    (symbol, market, quarter, institution_count, institution_count_change,
                     holding_pct, holding_pct_change, float_pct, float_pct_change, updated_at)
                VALUES ($1,'cn',$2,$3,$4,$5,$6,$7,$8,NOW())
                ON CONFLICT (symbol, quarter) DO UPDATE SET
                    institution_count=EXCLUDED.institution_count,
                    institution_count_change=EXCLUDED.institution_count_change,
                    holding_pct=EXCLUDED.holding_pct, holding_pct_change=EXCLUDED.holding_pct_change,
                    float_pct=EXCLUDED.float_pct, float_pct_change=EXCLUDED.float_pct_change,
                    updated_at=NOW()
                """,
                symbol, quarter,
                holdings.get("institution_count"), holdings.get("institution_count_change"),
                holdings.get("holding_pct"), holdings.get("holding_pct_change"),
                holdings.get("float_pct"), holdings.get("float_pct_change"),
            )
        return 1
    except Exception as exc:
        logger.warning("Fund holding upsert %s/%s failed: %s", symbol, quarter, exc)
        return 0


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
) -> None:
    try:
        r = await get_redis()
        now = datetime.now(timezone.utc)
        elapsed = (now - started_at).total_seconds()
        pct = int(done * 100 / total) if total > 0 else 0
        progress = {
            "symbolsDone": done,
            "symbolsTotal": total,
            "percent": pct,
            "elapsedSeconds": round(elapsed, 1),
            "errorsCount": error_count,
            "updatedAt": now.isoformat(),
        }
        await r.setex(progress_key, _PROGRESS_TTL, json.dumps(progress))
    except Exception:
        pass


async def _clear_progress(progress_key: str) -> None:
    try:
        r = await get_redis()
        await r.delete(progress_key)
    except Exception:
        pass
