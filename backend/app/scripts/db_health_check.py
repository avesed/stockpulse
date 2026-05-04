#!/usr/bin/env python3
"""Database health check and repair script.

Run inside the app container:
    python -m app.scripts.db_health_check [--fix]

Checks:
  1. stock_symbols completeness — ensures all symbols found in data tables
     exist in stock_symbols (prevents broken joins after TRUNCATE incidents)
  2. Table row counts and freshness
  3. Orphaned Redis locks
  4. Alembic migration version

With --fix: automatically repairs detected issues.
Without --fix: dry-run, only reports problems.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger(__name__)


async def check_symbol_completeness(conn, fix: bool) -> int:
    """Ensure all symbols in data tables exist in stock_symbols."""
    issues = 0

    data_tables = [
        ("stock_daily_bars", "symbol", "market"),
        ("stock_fundamentals", "symbol", "market"),
        ("analyst_ratings", "symbol", "market"),
        ("valuation_history", "symbol", "market"),
        ("insider_transactions", "symbol", "market"),
        ("upgrades_downgrades", "symbol", "market"),
    ]

    for table, sym_col, mkt_col in data_tables:
        missing = await conn.fetch(f"""
            SELECT DISTINCT d.{sym_col} as symbol, d.{mkt_col} as market
            FROM {table} d
            LEFT JOIN stock_symbols s ON d.{sym_col} = s.symbol
            WHERE s.symbol IS NULL
            LIMIT 1000
        """)
        if missing:
            issues += len(missing)
            print(f"  [!] {table}: {len(missing)} symbols missing from stock_symbols")
            if fix:
                for row in missing:
                    await conn.execute(
                        "INSERT INTO stock_symbols (symbol, market, name) "
                        "VALUES ($1, $2, $1) ON CONFLICT DO NOTHING",
                        row["symbol"], row["market"],
                    )
                print(f"      -> Fixed: inserted {len(missing)} symbols")

    if issues == 0:
        print("  [OK] All data table symbols exist in stock_symbols")
    return issues


async def check_table_freshness(conn) -> int:
    """Check data freshness for time-series tables."""
    issues = 0
    today = date.today()
    stale_threshold = today - timedelta(days=7)

    tables_with_date = [
        ("stock_daily_bars", "date", 2),
        ("stock_fundamentals", "date", 14),
        ("analyst_ratings", "date", 14),
        ("northbound_holdings", "date", 7),
    ]

    for table, date_col, max_age_days in tables_with_date:
        latest = await conn.fetchval(f"SELECT MAX({date_col}) FROM {table}")
        rows = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        if latest is None:
            print(f"  [!] {table}: EMPTY (0 rows)")
            issues += 1
        elif latest < today - timedelta(days=max_age_days):
            print(f"  [!] {table}: stale (latest={latest}, {(today - latest).days}d old, {rows:,} rows)")
            issues += 1
        else:
            print(f"  [OK] {table}: latest={latest}, {rows:,} rows")

    return issues


async def check_table_counts(conn) -> None:
    """Print row counts for all tables."""
    all_tables = [
        "stock_symbols", "stock_daily_bars", "stock_fundamentals",
        "stock_profiles", "analyst_ratings", "northbound_holdings",
        "institutional_holders", "fund_holdings",
        "valuation_history", "insider_transactions", "insider_sentiment",
        "earnings_surprises", "recommendation_trends", "upgrades_downgrades",
        "sec_financials", "earnings_calendar", "options_sentiment",
        "short_interest", "macro_daily", "economic_indicators",
        "cn_alternative_data", "collection_runs",
    ]

    total = 0
    for t in all_tables:
        try:
            rows = await conn.fetchval(f"SELECT COUNT(*) FROM {t}")
            total += rows
            print(f"  {t:30s} {rows:>12,}")
        except Exception as e:
            print(f"  {t:30s} ERROR: {e}")

    print(f"  {'TOTAL':30s} {total:>12,}")


async def check_redis_locks(fix: bool) -> int:
    """Check for orphaned Redis locks."""
    from app.core.redis import get_redis

    issues = 0
    r = await get_redis()

    for prefix in ("sp:daily_bars:", "sp:ml:", "sp:fundamentals:", "sp:stock_profile:"):
        keys = await r.keys(f"{prefix}*:lock")
        for k in keys:
            k_str = k.decode() if isinstance(k, bytes) else k
            ttl = await r.ttl(k)
            issues += 1
            print(f"  [!] Orphaned lock: {k_str} (ttl={ttl}s)")
            if fix:
                await r.delete(k)
                print(f"      -> Deleted")

    if issues == 0:
        print("  [OK] No orphaned locks")
    return issues


async def check_alembic_version(conn) -> None:
    """Check current migration version."""
    ver = await conn.fetchval("SELECT version_num FROM alembic_version")
    print(f"  Alembic version: {ver}")
    if ver != "010":
        print(f"  [!] Expected 010, got {ver}")


async def main():
    fix = "--fix" in sys.argv

    print(f"{'=' * 60}")
    print(f"StockPulse DB Health Check {'(DRY RUN)' if not fix else '(FIX MODE)'}")
    print(f"{'=' * 60}")

    from app.core.database import init_db_pool, get_db_pool

    await init_db_pool()
    pool = get_db_pool()

    async with pool.acquire() as conn:
        print("\n1. Migration version")
        await check_alembic_version(conn)

        print("\n2. Table row counts")
        await check_table_counts(conn)

        print("\n3. Symbol completeness")
        issues1 = await check_symbol_completeness(conn, fix)

        print("\n4. Data freshness")
        issues2 = await check_table_freshness(conn)

    print("\n5. Redis locks")
    issues3 = await check_redis_locks(fix)

    total_issues = issues1 + issues2 + issues3
    print(f"\n{'=' * 60}")
    if total_issues == 0:
        print("All checks passed!")
    else:
        print(f"{total_issues} issues found.")
        if not fix:
            print("Run with --fix to auto-repair.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
