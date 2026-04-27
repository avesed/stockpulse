"""3-layer cache for stock history data within data-service.

Layer 1 - Real-time (Redis): Handled externally (quote caching in app.core.cache).
Layer 2 - Intraday tiers (Disk MessagePack):
    T1m:  1min bars,  7 days,   TTL 4h   (yfinance: 7d)
    T5m:  5min bars,  59 days,  TTL 12h  (yfinance: 60d)
    T1h:  1hour bars, 729 days, TTL 48h  (yfinance: 730d)
    T1d:  daily bars, 365 days, TTL 7d   (disk cache for daily)
Layer 3 - Daily bars beyond 1 year (PostgreSQL stock_daily_bars):
    Reads directly from DB -- no disk archive.

Storage path: /app/data/stock_cache/{SAFE_SYMBOL}/canon_{interval}.msgpack

Core concept: a request for 15m data uses the T5m canonical cache and
downsamples.  One provider fetch serves all resolutions that share the same
canonical tier.

Key difference from backend version: Layer 3 reads directly from PostgreSQL
stock_daily_bars table instead of disk msgpack archive.  Daily bars for
Layer 2 (<=365 days) still use disk cache for performance.
"""

import asyncio
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import msgpack
import pandas as pd

from app.providers.constants import SH, SZ, detect_market

logger = logging.getLogger(__name__)

# Per-symbol locks for append_bars to prevent lost-update on concurrent writes.
# Bounded to prevent memory leaks from unbounded symbol growth.
_MAX_APPEND_LOCKS = 5000
_append_locks: Dict[str, asyncio.Lock] = {}


def _get_append_lock(key: str) -> asyncio.Lock:
    """Get or create a per-key asyncio.Lock with LRU-style eviction."""
    if key not in _append_locks:
        if len(_append_locks) >= _MAX_APPEND_LOCKS:
            # Evict oldest 1000 entries (Python 3.7+ dicts are insertion-ordered)
            for old_key in list(_append_locks.keys())[:1000]:
                del _append_locks[old_key]
        _append_locks[key] = asyncio.Lock()
    return _append_locks[key]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CACHE_BASE = Path("/app/data/stock_cache")

# Tier definitions: canonical interval -> max lookback days, TTL seconds
# max_days reflects the actual provider limit (yfinance) with a small safety margin.
TIER_DEFS: Dict[str, Dict[str, Any]] = {
    "1m": {"max_days": 7,   "ttl_seconds": 14400},   # yfinance: 7d,   TTL 4h
    "5m": {"max_days": 59,  "ttl_seconds": 43200},   # yfinance: 60d,  TTL 12h
    "1h": {"max_days": 729, "ttl_seconds": 172800},   # yfinance: 730d, TTL 48h
    "1d": {"max_days": 365, "ttl_seconds": 604800},   # daily bars,     TTL 7d
}

# Map: user-requested interval -> canonical tier interval
RESAMPLE_MAP: Dict[str, str] = {
    "1m": "1m",
    "2m": "1m",
    "5m": "5m",
    "15m": "5m",
    "30m": "5m",
    "1h": "1h",
    "1d": "1d",
    "1wk": "1d",
    "1mo": "1d",
}

# Pandas resample frequency strings
FREQ_MAP: Dict[str, str] = {
    "2m": "2min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "1d": "1D",
    "1wk": "W",
    "1mo": "ME",
}

# Escalation order when the requested tier cannot cover the requested days
_ESCALATION_ORDER = ["1m", "5m", "1h", "1d"]

# Lua script for atomic CAS lock release (same pattern as collection_service)
_RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""

# Period string to approximate day count mapping (used by API endpoint)
PERIOD_DAYS: Dict[str, int] = {
    "1d": 1,
    "5d": 5,
    "1mo": 30,
    "3mo": 90,
    "6mo": 180,
    "1y": 365,
    "2y": 730,
    "5y": 1825,
    "10y": 3650,
    "max": 9999,
    "ytd": 365,
}


# ---------------------------------------------------------------------------
# TierResolution
# ---------------------------------------------------------------------------

@dataclass
class TierResolution:
    """Result of resolving a user request to a canonical tier."""

    tier_interval: str      # "1m" / "5m" / "1h" / "1d"
    layer: int              # 2 or 3
    needs_resample: bool    # True if user interval != tier interval
    ttl_seconds: int
    cache_filename: str     # e.g. "canon_1m.msgpack"
    max_days: int           # max lookback days for this tier


def resolve_tier(
    interval: str,
    days: int,
    market: Optional[str] = None,
) -> TierResolution:
    """Map a user-requested interval + day span to the canonical tier.

    A-share special handling: akshare's 1h lookback is roughly the same as 5m
    (~47 days), so for SH/SZ we skip the 1h tier and escalate from 5m directly
    to 1d.
    """
    source = RESAMPLE_MAP.get(interval, interval)

    # A-share special: "1h" tier is unreliable via akshare, reroute
    if market in (SH, SZ) and source == "1h":
        source = "5m" if days <= 45 else "1d"

    # Try the source tier first, then escalate if it cannot cover the days
    start_idx = _ESCALATION_ORDER.index(source) if source in _ESCALATION_ORDER else 0
    for tier_key in _ESCALATION_ORDER[start_idx:]:
        tier = TIER_DEFS[tier_key]
        if days <= tier["max_days"]:
            return TierResolution(
                tier_interval=tier_key,
                layer=2,
                needs_resample=(tier_key != interval),
                ttl_seconds=tier["ttl_seconds"],
                cache_filename=f"canon_{tier_key}.msgpack",
                max_days=tier["max_days"],
            )

    # Beyond all Layer 2 tiers -> Layer 3 (database for daily bars)
    return TierResolution(
        tier_interval="1d",
        layer=3,
        needs_resample=("1d" != interval),
        ttl_seconds=0,  # No disk TTL for DB-backed layer
        cache_filename="",  # No disk file for Layer 3
        max_days=99999,
    )


# ---------------------------------------------------------------------------
# Disk cache helpers (all synchronous -- called via asyncio.to_thread)
# ---------------------------------------------------------------------------

def _cache_path(symbol: str, filename: str) -> Path:
    """Build the disk path for a cache file."""
    # Sanitise symbol for filesystem safety (e.g. GC=F -> GC_F)
    safe_symbol = symbol.replace("=", "_").replace("/", "_")
    return CACHE_BASE / safe_symbol / filename


def _read_cache(path: Path, ttl: int) -> Optional[List[dict]]:
    """Read a MessagePack cache file and validate TTL.

    Returns the bar list if the file exists and has not expired, otherwise None.
    """
    try:
        if not path.exists():
            return None
        raw = path.read_bytes()
        data = msgpack.unpackb(raw, raw=False)
        updated_at = data.get("updated_at", 0)
        if (time.time() - updated_at) > ttl:
            logger.debug(
                "Cache expired: %s (age %.0fs > %ds)",
                path, time.time() - updated_at, ttl,
            )
            return None
        bars = data.get("bars")
        if not bars:
            return None
        logger.debug("Cache hit: %s (%d bars)", path, len(bars))
        return bars
    except Exception as exc:
        logger.warning("Failed to read cache file %s: %s", path, exc)
        return None


def _read_cache_no_ttl(path: Path) -> Optional[List[dict]]:
    """Read a cache file without TTL validation (for merge operations)."""
    try:
        if not path.exists():
            return None
        raw = path.read_bytes()
        data = msgpack.unpackb(raw, raw=False)
        return data.get("bars")
    except Exception as exc:
        logger.warning("Failed to read cache file (no-ttl) %s: %s", path, exc)
        return None


def _write_cache(path: Path, bars: List[dict], ttl: int) -> None:
    """Atomically write bars to a MessagePack cache file."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": time.time(),
            "ttl": ttl,
            "bars": bars,
        }
        packed = msgpack.packb(payload, use_bin_type=True)
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_bytes(packed)
        os.rename(str(tmp_path), str(path))
        logger.debug("Cache written: %s (%d bars)", path, len(bars))
    except Exception as exc:
        logger.error("Failed to write cache file %s: %s", path, exc)


def _append_bars(path: Path, new_bars: List[dict], ttl: int) -> None:
    """Merge new_bars into an existing cache file, dedup by date, and write."""
    existing = _read_cache_no_ttl(path) or []

    # Build a dict keyed by date for dedup (new bars overwrite existing)
    merged: Dict[str, dict] = {}
    for bar in existing:
        merged[bar["date"]] = bar
    for bar in new_bars:
        merged[bar["date"]] = bar

    # Sort by date string (ISO format sorts lexicographically)
    sorted_bars = sorted(merged.values(), key=lambda b: b["date"])

    _write_cache(path, sorted_bars, ttl)
    logger.debug(
        "Cache appended: %s (%d existing + %d new -> %d merged)",
        path, len(existing), len(new_bars), len(sorted_bars),
    )


# ---------------------------------------------------------------------------
# Resampling
# ---------------------------------------------------------------------------

def resample_bars(
    bars: List[dict],
    source_interval: str,
    target_interval: str,
) -> List[dict]:
    """Downsample bars from source_interval to target_interval.

    If source == target, returns bars as-is.
    """
    if source_interval == target_interval or not bars:
        return bars

    freq = FREQ_MAP.get(target_interval)
    if not freq:
        logger.warning(
            "No resample frequency for target '%s', returning raw bars",
            target_interval,
        )
        return bars

    try:
        df = pd.DataFrame(bars)

        # Detect original timezone from first bar so we can restore it after
        # resampling.  pandas resample requires a uniform tz (we use UTC), but
        # the frontend relies on the original offset for display.
        original_tz = None
        first_date = bars[0].get("date", "")
        tz_match = re.search(r"[+-]\d{2}:\d{2}$", first_date)
        if tz_match:
            offset_str = tz_match.group()  # e.g. "-05:00"
            sign = 1 if offset_str[0] == "+" else -1
            hours = int(offset_str[1:3])
            minutes = int(offset_str[4:6])
            original_tz = timezone(timedelta(hours=sign * hours, minutes=sign * minutes))

        # Parse date column -- handle both datetime and date-only strings
        df["date"] = pd.to_datetime(df["date"], utc=True, format="mixed")
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

        # Ensure numeric types
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        resampled = (
            df.resample(freq)
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            .dropna(subset=["open", "close"])
        )

        # Restore original timezone so the frontend can display market-local time
        if original_tz is not None:
            resampled.index = resampled.index.tz_convert(original_tz)

        result: List[dict] = []
        for idx, row in resampled.iterrows():
            result.append(
                {
                    "date": idx.isoformat(),
                    "open": round(float(row["open"]), 4),
                    "high": round(float(row["high"]), 4),
                    "low": round(float(row["low"]), 4),
                    "close": round(float(row["close"]), 4),
                    "volume": int(row["volume"]),
                }
            )

        logger.debug(
            "Resampled %d bars (%s -> %s) to %d bars",
            len(bars), source_interval, target_interval, len(result),
        )
        return result
    except Exception as exc:
        logger.error(
            "Resample failed (%s -> %s): %s",
            source_interval, target_interval, exc,
        )
        return bars


# ---------------------------------------------------------------------------
# HistoryCacheService
# ---------------------------------------------------------------------------

class HistoryCacheService:
    """Manages the 3-layer canonical cache for stock history data.

    Layer 2 (disk MessagePack) covers intraday and short-range daily bars.
    Layer 3 (PostgreSQL stock_daily_bars) covers daily bars beyond 1 year.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_history(
        self,
        symbol: str,
        interval: str,
        period_days: int,
        market: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Optional[List[dict]]:
        """Get historical bars, leveraging the canonical cache.

        1. Resolve the canonical tier for the requested interval/days.
        2. Layer 2 (disk): For tiers 1m/5m/1h/1d (<=365 days), try disk cache.
        3. Layer 3 (database): For tier 1d (>365 days), query stock_daily_bars.
        4. On L2 miss: acquire Redis distributed lock, fetch from provider,
           write to disk cache.
        5. Trim bars to the requested [start, end] range.
        6. Resample to the user's requested interval if necessary.
        7. Return a list of bar dicts.
        """
        if market is None:
            market = detect_market(symbol)

        resolution = resolve_tier(interval, period_days, market)

        bars: Optional[List[dict]] = None

        if resolution.layer == 3:
            # Layer 3: read daily bars directly from PostgreSQL
            bars = await self._fetch_from_database(symbol, start, end)
            if not bars:
                # Fallback: try provider fetch for symbols not yet in DB
                logger.info(
                    "Layer 3 DB miss for %s, falling back to provider fetch",
                    symbol,
                )
                bars = await self._locked_fetch_provider(
                    symbol, resolution, market,
                )
        else:
            # Layer 2: try disk cache
            path = _cache_path(symbol, resolution.cache_filename)
            bars = await asyncio.to_thread(
                _read_cache, path, resolution.ttl_seconds,
            )

            if bars is None:
                # Disk cache miss -> distributed lock + provider fetch
                logger.info(
                    "Cache miss for %s (tier=%s, file=%s), fetching from provider",
                    symbol, resolution.tier_interval, resolution.cache_filename,
                )
                bars = await self._locked_fetch(
                    symbol, resolution, market, path,
                )

        if not bars:
            return None

        # Trim to requested range
        if start or end:
            bars = self._trim_to_range(bars, start, end)
        elif period_days < resolution.max_days:
            # Period-based request: canonical tier is wider than requested.
            is_intraday = resolution.tier_interval in ("1m", "5m")
            if is_intraday and period_days <= 5:
                # For intraday intervals, "1d" means "last trading day", not
                # "last 24h".  Widen the lookback to cover weekends/holidays,
                # then keep only the last N *trading* days.
                buffer_days = period_days + 4  # cover weekends + holidays
                cutoff = datetime.now(timezone.utc) - timedelta(days=buffer_days)
                cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
                bars = self._trim_to_range(bars, start=cutoff_str)
                # Extract unique trading dates and keep only the last N
                if bars:
                    trading_dates = sorted({b["date"][:10] for b in bars})
                    keep_dates = set(trading_dates[-period_days:])
                    bars = [b for b in bars if b["date"][:10] in keep_dates]
            else:
                cutoff = datetime.now(timezone.utc) - timedelta(days=period_days)
                cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
                bars = self._trim_to_range(bars, start=cutoff_str)

        # Resample if needed
        if resolution.needs_resample and resolution.tier_interval != interval:
            bars = resample_bars(bars, resolution.tier_interval, interval)

        return bars

    async def append_bars(
        self,
        symbol: str,
        tier_interval: str,
        new_bars: List[dict],
    ) -> None:
        """Write-back: merge new bars into the existing disk cache.

        Called by the /history/latest endpoint as a fire-and-forget task.
        Uses a per-symbol asyncio lock to prevent lost-update from concurrent writes.
        Only applicable to intraday tiers (disk-backed).
        """
        if not new_bars:
            return

        tier = TIER_DEFS.get(tier_interval)
        if tier is None:
            logger.warning("append_bars: unknown tier interval '%s'", tier_interval)
            return

        filename = f"canon_{tier_interval}.msgpack"
        path = _cache_path(symbol, filename)
        ttl = tier["ttl_seconds"]
        lock_key = f"{symbol}:{tier_interval}"

        try:
            async with _get_append_lock(lock_key):
                await asyncio.to_thread(_append_bars, path, new_bars, ttl)
        except Exception as exc:
            logger.error("append_bars failed for %s/%s: %s", symbol, tier_interval, exc)

    # ------------------------------------------------------------------
    # Layer 3: Database fetch
    # ------------------------------------------------------------------

    async def _fetch_from_database(
        self,
        symbol: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Optional[List[dict]]:
        """Read daily bars from PostgreSQL stock_daily_bars table.

        This replaces the disk-based Layer 3 archive from the backend version.
        Returns bars as list of dicts compatible with the cache format.
        """
        from app.services.bar_persistence_service import get_daily_bars_row
        from app.core.database import get_db_pool

        try:
            pool = get_db_pool()
        except RuntimeError:
            logger.warning("DB pool not initialized, cannot read daily bars from DB")
            return None

        try:
            from datetime import date as date_type

            start_date: Optional[date_type] = None
            end_date: Optional[date_type] = None

            if start:
                try:
                    start_date = date_type.fromisoformat(start[:10])
                except ValueError:
                    pass
            if end:
                try:
                    end_date = date_type.fromisoformat(end[:10])
                except ValueError:
                    pass

            bars = await get_daily_bars_row(pool, symbol, start_date, end_date)

            if not bars:
                logger.debug("No daily bars in DB for %s", symbol)
                return None

            logger.info(
                "Layer 3 DB hit for %s: %d daily bars (start=%s, end=%s)",
                symbol, len(bars),
                bars[0]["date"] if bars else "N/A",
                bars[-1]["date"] if bars else "N/A",
            )
            return bars

        except Exception as exc:
            logger.error("Failed to read daily bars from DB for %s: %s", symbol, exc)
            return None

    # ------------------------------------------------------------------
    # Layer 2: Disk-cached fetch with distributed lock
    # ------------------------------------------------------------------

    async def _locked_fetch(
        self,
        symbol: str,
        resolution: TierResolution,
        market: str,
        path: Path,
    ) -> Optional[List[dict]]:
        """Fetch bars from the provider under a distributed Redis lock.

        If the lock cannot be acquired (another worker is fetching), we
        wait briefly then re-check the disk cache.
        """
        from app.core.redis import get_redis

        lock_key = f"canon:{symbol}:{resolution.tier_interval}"
        lock_value = str(uuid.uuid4())

        try:
            redis = await get_redis()
            acquired = await redis.set(lock_key, lock_value, nx=True, ex=60)
        except Exception as exc:
            logger.warning("Redis lock acquisition error for %s: %s", lock_key, exc)
            # Fall through to provider fetch without lock
            return await self._fetch_from_provider(symbol, resolution, market)

        if acquired:
            try:
                # Double-check disk after acquiring lock
                bars = await asyncio.to_thread(
                    _read_cache, path, resolution.ttl_seconds,
                )
                if bars is not None:
                    return bars

                # Fetch from provider
                bars = await self._fetch_from_provider(symbol, resolution, market)
                if bars:
                    await asyncio.to_thread(
                        _write_cache, path, bars, resolution.ttl_seconds,
                    )
                return bars
            except Exception as exc:
                logger.error(
                    "Provider fetch failed for %s (tier=%s): %s",
                    symbol, resolution.tier_interval, exc,
                )
                return None
            finally:
                try:
                    redis = await get_redis()
                    await redis.eval(_RELEASE_LOCK_LUA, 1, lock_key, lock_value)
                except Exception as exc:
                    logger.warning("Failed to release lock %s: %s", lock_key, exc)
        else:
            # Another process is fetching; wait with backoff and retry disk cache
            for attempt in range(10):
                await asyncio.sleep(min(0.5 * (1.5 ** attempt), 5.0))
                bars = await asyncio.to_thread(
                    _read_cache, path, resolution.ttl_seconds,
                )
                if bars is not None:
                    return bars
            logger.warning(
                "Lock contention timeout for %s/%s, returning None",
                symbol, resolution.tier_interval,
            )
            return None

    async def _locked_fetch_provider(
        self,
        symbol: str,
        resolution: TierResolution,
        market: str,
    ) -> Optional[List[dict]]:
        """Fetch from provider for Layer 3 (no disk cache, DB is source of truth).

        Uses a Redis lock to prevent thundering herd on the same symbol, but
        does not write to disk -- the caller should use the returned bars
        directly.
        """
        from app.core.redis import get_redis

        lock_key = f"canon:{symbol}:{resolution.tier_interval}:l3"
        lock_value = str(uuid.uuid4())

        try:
            redis = await get_redis()
            acquired = await redis.set(lock_key, lock_value, nx=True, ex=120)
        except Exception as exc:
            logger.warning("Redis lock error for L3 %s: %s", lock_key, exc)
            return await self._fetch_from_provider(symbol, resolution, market)

        if acquired:
            try:
                bars = await self._fetch_from_provider(symbol, resolution, market)
                return bars
            finally:
                try:
                    redis = await get_redis()
                    await redis.eval(_RELEASE_LOCK_LUA, 1, lock_key, lock_value)
                except Exception as exc:
                    logger.warning("Failed to release L3 lock %s: %s", lock_key, exc)
        else:
            # Wait for other worker to finish, then try DB again
            for attempt in range(10):
                await asyncio.sleep(min(0.5 * (1.5 ** attempt), 5.0))
                bars = await self._fetch_from_database(symbol)
                if bars is not None:
                    return bars
            logger.warning(
                "Lock contention timeout for L3 %s, returning None", symbol,
            )
            return None

    # ------------------------------------------------------------------
    # Provider fetch
    # ------------------------------------------------------------------

    async def _fetch_from_provider(
        self,
        symbol: str,
        resolution: TierResolution,
        market: str,
    ) -> Optional[List[dict]]:
        """Fetch bars from the external provider for the canonical tier.

        Layer 2 tiers use start/end dates computed from max_days.
        Layer 3 uses period="max" to get all available daily data.
        """
        from app.services.stock_router import get_stock_router

        sr = await get_stock_router()

        if resolution.layer == 3:
            # Layer 3: fetch all available daily data
            logger.info(
                "Fetching archive data for %s: tier=%s, period=max, market=%s",
                symbol, resolution.tier_interval, market,
            )
            result = await sr.get_history(
                symbol,
                period="max",
                interval=resolution.tier_interval,
                market=market,
            )
        else:
            # Layer 2: use start/end dates derived from the tier's max_days
            now = datetime.now(timezone.utc)
            end_date = now.strftime("%Y-%m-%d")
            start_date = (now - timedelta(days=resolution.max_days)).strftime("%Y-%m-%d")
            logger.info(
                "Fetching canonical data for %s: tier=%s, start=%s, end=%s, market=%s",
                symbol, resolution.tier_interval, start_date, end_date, market,
            )
            result = await sr.get_history(
                symbol,
                period="1y",  # placeholder, ignored when start/end provided
                interval=resolution.tier_interval,
                market=market,
                start=start_date,
                end=end_date,
            )

        if result is None:
            return None

        raw_bars = result.get("bars", [])
        if not raw_bars:
            return None

        bars = [
            {
                "date": str(b.get("date", "")),
                "open": round(float(b.get("open", 0)), 4),
                "high": round(float(b.get("high", 0)), 4),
                "low": round(float(b.get("low", 0)), 4),
                "close": round(float(b.get("close", 0)), 4),
                "volume": int(b.get("volume", 0)) if b.get("volume") is not None else 0,
            }
            for b in raw_bars
        ]
        logger.info(
            "Fetched %d bars for %s (tier=%s, layer=%d)",
            len(bars), symbol, resolution.tier_interval, resolution.layer,
        )
        return bars

    # ------------------------------------------------------------------
    # Range trimming
    # ------------------------------------------------------------------

    @staticmethod
    def _trim_to_range(
        bars: List[dict],
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> List[dict]:
        """Filter bars to [start, end] inclusive range.

        Handles both date-only (YYYY-MM-DD) and datetime (YYYY-MM-DDTHH:MM:SS)
        strings.  Comparison is done lexicographically after normalising the
        'T' separator to a space so that mixed formats compare correctly.
        """
        if not bars:
            return bars

        def normalise(s: str) -> str:
            s = s.replace("T", " ")
            # Strip any timezone offset (e.g. +08:00, -05:00, +00:00, Z)
            s = re.sub(r'[+-]\d{2}:\d{2}$', '', s)
            return s.rstrip("Z")

        result = bars
        if start:
            norm_start = normalise(start)
            result = [b for b in result if normalise(b["date"]) >= norm_start]
        if end:
            norm_end = normalise(end)
            result = [b for b in result if normalise(b["date"]) <= norm_end]
        return result


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_history_cache_service: Optional[HistoryCacheService] = None
_history_cache_service_lock = asyncio.Lock()


async def get_history_cache_service() -> HistoryCacheService:
    """Get the singleton HistoryCacheService instance."""
    global _history_cache_service
    if _history_cache_service is None:
        async with _history_cache_service_lock:
            if _history_cache_service is None:
                _history_cache_service = HistoryCacheService()
    return _history_cache_service


# ---------------------------------------------------------------------------
# Standalone cleanup
# ---------------------------------------------------------------------------

def cleanup_expired_cache_files() -> int:
    """Remove expired msgpack cache files from disk.

    Scans all symbol directories and deletes files whose TTL has elapsed.
    Returns the count of deleted files.  Safe to call from a scheduled task.
    """
    deleted = 0
    if not CACHE_BASE.exists():
        return deleted

    # Build a lookup of filename -> TTL
    filename_ttl: Dict[str, int] = {}
    for interval, tier in TIER_DEFS.items():
        filename_ttl[f"canon_{interval}.msgpack"] = tier["ttl_seconds"]

    now = time.time()
    for symbol_dir in CACHE_BASE.iterdir():
        if not symbol_dir.is_dir():
            continue
        for cache_file in symbol_dir.glob("*.msgpack"):
            ttl = filename_ttl.get(cache_file.name)
            if ttl is None:
                continue  # Unknown file, skip
            try:
                with open(cache_file, "rb") as f:
                    data = msgpack.unpack(f, raw=False)
                if now - data.get("updated_at", 0) > ttl * 2:
                    # Delete files that are 2x past their TTL (generous grace period)
                    cache_file.unlink()
                    deleted += 1
                    logger.debug("Cleaned up expired cache: %s", cache_file)
            except Exception:
                pass  # Corrupted file -- leave for next cleanup

        # Remove empty symbol directories
        try:
            if symbol_dir.exists() and not any(symbol_dir.iterdir()):
                symbol_dir.rmdir()
        except OSError:
            pass

    if deleted:
        logger.info("Disk cache cleanup: removed %d expired files", deleted)
    return deleted
