"""Stock profile collection pipeline for data-service.

Orchestrates per-market stock profile collection, saving results as JSON
files to disk so the backend can download and embed them independently.

Pipeline per market:
1. Acquire Redis lock: ``sp:stock_profile:{market}:lock`` (SET NX, TTL 14400s)
2. Update Redis progress: ``sp:stock_profile:{market}:progress``
3. Collect profiles using existing collection functions in stock_profile_service
4. Save to disk: ``/app/data/profiles/{market}/profiles.json``
5. Write metadata: ``/app/data/profiles/{market}/metadata.json``
6. Publish Redis signal: ``profile_collection:complete:{market}``
7. Clear progress, release lock

Redis key patterns:
- Lock: ``sp:stock_profile:{market}:lock`` (SET NX, TTL 14400s, CAS release)
- Progress: ``sp:stock_profile:{market}:progress`` (JSON, TTL 3600s)

Disk storage structure::

    /app/data/profiles/
    +-- cn/
    |   +-- profiles.json         # List of profile dicts
    |   +-- concept_mapping.json  # {concepts: {code: [concepts]}, names: {code: name}}
    |   +-- metadata.json         # {collected_at, count, market}
    +-- us/
    |   +-- profiles.json
    |   +-- metadata.json
    +-- hk/
        +-- profiles.json
        +-- metadata.json
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from app.core.database import get_db_pool
from app.core.redis import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LOCK_KEY_TEMPLATE = "sp:stock_profile:{market}:lock"
_LOCK_TTL = 14400  # 4 hours

_PROGRESS_KEY_TEMPLATE = "sp:stock_profile:{market}:progress"
_PROGRESS_TTL = 3600  # 1 hour

_DATA_DIR = Path("/app/data/profiles")

# Lua script for atomic CAS lock release (same pattern as collection_service)
_RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def collect_market_profiles(market: str) -> dict[str, Any]:
    """Collect stock profiles for a market and save to disk as JSON.

    Args:
        market: ``'cn'``, ``'us'``, or ``'hk'``.

    Returns:
        Summary dict: ``{collected, market, elapsed_seconds}``.
    """
    market = market.lower()
    if market not in ("cn", "us", "hk"):
        raise ValueError(f"Unsupported profile market: {market}")

    owner = await _acquire_lock(market)
    if owner is None:
        logger.warning(
            "Profile collection for market=%s already running, skipping",
            market,
        )
        return {
            "collected": 0,
            "market": market,
            "elapsed_seconds": 0,
            "error": "already_running",
        }

    t0 = time.monotonic()
    try:
        await _update_progress(market, "collecting", 0, 1)

        # Collect profiles using existing functions
        profiles_data = await _collect_profiles_for_market(market)

        count = len(profiles_data)
        logger.info(
            "Profile collection for %s: %d profiles collected", market, count,
        )

        # Save to disk + DB
        await _update_progress(market, "saving", count, count)
        _save_profiles_to_disk(market, profiles_data)
        db_count = await _save_profiles_to_db(market, profiles_data)
        logger.info("Profile DB upsert for %s: %d rows", market, db_count)

        elapsed = time.monotonic() - t0

        # Publish completion signal (cross-DB: PUBLISH works globally)
        await _publish_completion(market, count)

        # Mark progress as complete before clearing
        await _update_progress(market, "complete", count, count)

        return {
            "collected": count,
            "market": market,
            "elapsed_seconds": round(elapsed, 1),
        }

    except Exception as exc:
        elapsed = time.monotonic() - t0
        logger.exception(
            "Profile collection failed for market=%s after %.0fs: %s",
            market, elapsed, exc,
        )
        return {
            "collected": 0,
            "market": market,
            "elapsed_seconds": round(elapsed, 1),
            "error": str(exc),
        }
    finally:
        await _clear_progress(market)
        await _release_lock(market, owner)


async def collect_cn_concept_mapping() -> dict[str, Any]:
    """Collect A-share concept mapping only (for daily sync).

    Returns:
        Summary dict: ``{stock_count, elapsed_seconds}``.
    """
    from app.services.stock_profile_service import collect_concept_mapping

    t0 = time.monotonic()

    concepts_dict, names_dict = await collect_concept_mapping()

    if not concepts_dict:
        logger.warning("Concept mapping returned no data")
        return {"stock_count": 0, "elapsed_seconds": 0}

    # Save to disk atomically
    mapping_data = {
        "concepts": concepts_dict,
        "names": names_dict,
    }
    market_dir = _DATA_DIR / "cn"
    market_dir.mkdir(parents=True, exist_ok=True)
    mapping_path = market_dir / "concept_mapping.json"
    _atomic_write_json(mapping_path, mapping_data)

    elapsed = time.monotonic() - t0
    logger.info(
        "Concept mapping saved: %d stocks in %.0fs",
        len(concepts_dict), elapsed,
    )

    return {
        "stock_count": len(concepts_dict),
        "elapsed_seconds": round(elapsed, 1),
    }


async def get_market_profiles(market: str) -> Optional[list[dict[str, Any]]]:
    """Load pre-collected profiles, DB-first with disk fallback.

    Returns:
        List of profile dicts, or ``None`` if not available.
    """
    # Try DB first
    try:
        pool = get_db_pool()
        async with pool.acquire(timeout=5) as conn:
            rows = await conn.fetch(
                "SELECT symbol, market, name, name_zh, sector, industry, "
                "concepts, main_business, description "
                "FROM stock_profiles WHERE market = $1 ORDER BY symbol",
                market,
            )
        if rows:
            return [
                {
                    "symbol": r["symbol"],
                    "market": r["market"],
                    "name": r["name"],
                    "name_zh": r["name_zh"],
                    "sector": r["sector"],
                    "industry": r["industry"],
                    "concepts": r["concepts"] if r["concepts"] else [],
                    "main_business": r["main_business"] or "",
                    "description": r["description"],
                }
                for r in rows
            ]
    except Exception as exc:
        logger.debug("DB profile read failed for %s, falling back to disk: %s", market, exc)

    # Fallback to disk
    profiles_path = _DATA_DIR / market / "profiles.json"
    if not profiles_path.exists():
        return None
    try:
        data = json.loads(profiles_path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        logger.warning(
            "Profile data for %s is not a list (type=%s)",
            market, type(data).__name__,
        )
        return None
    except Exception as exc:
        logger.error("Failed to read profiles for %s: %s", market, exc)
        return None


async def get_concept_mapping() -> Optional[dict[str, Any]]:
    """Load pre-collected CN concept mapping from disk.

    Returns:
        Dict with keys ``concepts`` and ``names``, or ``None`` if not available.
    """
    mapping_path = _DATA_DIR / "cn" / "concept_mapping.json"
    if not mapping_path.exists():
        return None
    try:
        data = json.loads(mapping_path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
        return None
    except Exception as exc:
        logger.error("Failed to read concept mapping: %s", exc)
        return None


async def get_collection_progress(market: str) -> Optional[dict[str, Any]]:
    """Read per-market collection progress from Redis.

    Returns:
        Progress dict or ``None`` if no collection is in progress.
    """
    try:
        r = await get_redis()
        data = await r.get(_PROGRESS_KEY_TEMPLATE.format(market=market))
        if data is not None:
            return json.loads(data)
    except Exception as exc:
        logger.warning("Failed to read profile progress for %s: %s", market, exc)
    return None


async def get_collection_metadata(market: str) -> Optional[dict[str, Any]]:
    """Read per-market collection metadata from disk.

    Returns:
        Metadata dict or ``None`` if not available.
    """
    metadata_path = _DATA_DIR / market / "metadata.json"
    if not metadata_path.exists():
        return None
    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error("Failed to read metadata for %s: %s", market, exc)
        return None


async def force_unlock(market: str) -> bool:
    """Force-release per-market lock (admin operation).

    Returns:
        ``True`` if a lock was deleted, ``False`` otherwise.
    """
    try:
        r = await get_redis()
        deleted = await r.delete(_LOCK_KEY_TEMPLATE.format(market=market))
        return deleted > 0
    except Exception as exc:
        logger.warning("Failed to force-unlock profile lock for %s: %s", market, exc)
        return False


# ---------------------------------------------------------------------------
# Internal: per-market collection dispatch
# ---------------------------------------------------------------------------


async def _collect_profiles_for_market(market: str) -> list[dict[str, Any]]:
    """Dispatch collection to the appropriate function in stock_profile_service.

    For CN, uses ``collect_cn_profiles()`` (monolithic: concept mapping +
    individual stock info in one call), then extracts the concept mapping
    from returned profiles to save to disk separately.

    For US/HK, resolves symbols first via ``symbol_resolver``, then calls
    the monolithic collection functions.
    """
    from app.services import stock_profile_service, symbol_resolver

    if market == "cn":
        profiles = await stock_profile_service.collect_cn_profiles()

        # Extract concept mapping from profiles and save to disk.
        # This avoids a separate collect_concept_mapping() call (~200s).
        concepts_dict: dict[str, list[str]] = {}
        names_dict: dict[str, str] = {}
        for p in profiles:
            symbol = p.get("symbol", "")
            code = symbol.split(".")[0] if symbol else ""
            if not code:
                continue
            concepts = p.get("concepts", [])
            if concepts:
                concepts_dict[code] = concepts
            name_zh = p.get("name_zh") or p.get("name", "")
            if name_zh:
                names_dict[code] = name_zh

        if concepts_dict:
            mapping_data = {"concepts": concepts_dict, "names": names_dict}
            market_dir = _DATA_DIR / "cn"
            market_dir.mkdir(parents=True, exist_ok=True)
            mapping_path = market_dir / "concept_mapping.json"
            _atomic_write_json(mapping_path, mapping_data)
            logger.info(
                "CN concept mapping extracted from profiles: %d stocks",
                len(concepts_dict),
            )

        return profiles

    # US and HK need symbol lists
    symbols = await symbol_resolver.get_symbols(market)
    if not symbols:
        logger.warning("No symbols resolved for market=%s", market)
        return []

    if market == "us":
        return await stock_profile_service.collect_us_profiles(symbols)
    else:  # hk
        return await stock_profile_service.collect_hk_profiles(symbols)


# ---------------------------------------------------------------------------
# Internal: disk persistence
# ---------------------------------------------------------------------------


def _atomic_write_json(path: Path, data: Any) -> None:
    """Write JSON data to a file atomically via temp + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=str(path.parent), suffix=".tmp",
    )
    try:
        with open(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, default=str)
        os.rename(tmp_path, str(path))
    except BaseException:
        os.unlink(tmp_path)
        raise


async def _save_profiles_to_db(market: str, profiles: list[dict[str, Any]]) -> int:
    """Upsert profiles to stock_profiles table."""
    if not profiles:
        return 0

    try:
        pool = get_db_pool()
    except RuntimeError:
        logger.debug("DB pool not available for profile upsert")
        return 0

    sql = (
        "INSERT INTO stock_profiles "
        "(symbol, market, name, name_zh, sector, industry, concepts, main_business, description, updated_at) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,NOW()) "
        "ON CONFLICT (symbol) DO UPDATE SET "
        "market=EXCLUDED.market, name=EXCLUDED.name, name_zh=EXCLUDED.name_zh, "
        "sector=EXCLUDED.sector, industry=EXCLUDED.industry, concepts=EXCLUDED.concepts, "
        "main_business=EXCLUDED.main_business, description=EXCLUDED.description, updated_at=NOW()"
    )

    count = 0
    chunk_size = 100
    for i in range(0, len(profiles), chunk_size):
        chunk = profiles[i: i + chunk_size]
        rows = []
        for p in chunk:
            rows.append((
                p.get("symbol", ""),
                market,
                p.get("name"),
                p.get("name_zh"),
                p.get("sector"),
                p.get("industry"),
                json.dumps(p.get("concepts", []), ensure_ascii=False),
                p.get("main_business", ""),
                p.get("description"),
            ))
        try:
            async with pool.acquire(timeout=10) as conn:
                async with conn.transaction():
                    await conn.executemany(sql, rows)
            count += len(rows)
        except Exception as exc:
            logger.warning("Profile DB upsert chunk failed: %s", exc)

    return count


def _save_profiles_to_disk(market: str, profiles: list[dict[str, Any]]) -> None:
    """Save collected profiles and metadata to disk atomically."""
    market_dir = _DATA_DIR / market
    market_dir.mkdir(parents=True, exist_ok=True)

    # Write profiles atomically
    profiles_path = market_dir / "profiles.json"
    _atomic_write_json(profiles_path, profiles)

    # Write metadata atomically
    metadata = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "count": len(profiles),
        "market": market,
    }
    metadata_path = market_dir / "metadata.json"
    _atomic_write_json(metadata_path, metadata)

    logger.info(
        "Saved %d profiles to disk for market=%s at %s",
        len(profiles), market, profiles_path,
    )


# ---------------------------------------------------------------------------
# Internal: Redis lock helpers
# ---------------------------------------------------------------------------


async def _acquire_lock(market: str) -> Optional[str]:
    """Try to acquire the per-market collection lock via Redis SET NX.

    Returns:
        Owner token string if acquired, ``None`` if already held.
    """
    try:
        r = await get_redis()
        owner = str(uuid.uuid4())
        acquired = await r.set(
            _LOCK_KEY_TEMPLATE.format(market=market),
            owner,
            nx=True,
            ex=_LOCK_TTL,
        )
        if acquired:
            return owner
        return None
    except Exception as exc:
        # Redis unavailable -- fail closed to prevent concurrent collection
        logger.error(
            "Redis lock acquisition failed for profile %s, skipping collection: %s", market, exc,
        )
        return None


async def _release_lock(market: str, owner: str) -> None:
    """Release the per-market lock only if we still own it (CAS via Lua)."""
    try:
        r = await get_redis()
        await r.eval(
            _RELEASE_LOCK_LUA,
            1,
            _LOCK_KEY_TEMPLATE.format(market=market),
            owner,
        )
    except Exception as exc:
        logger.warning(
            "Failed to release profile lock for %s: %s", market, exc,
        )


# ---------------------------------------------------------------------------
# Internal: progress helpers
# ---------------------------------------------------------------------------


async def _update_progress(
    market: str,
    phase: str,
    current: int,
    total: int,
) -> None:
    """Write collection progress to Redis for admin UI consumption."""
    try:
        r = await get_redis()
        pct = int(current * 100 / total) if total > 0 else 0
        progress = {
            "phase": phase,
            "current": current,
            "total": total,
            "percent": pct,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }
        key = _PROGRESS_KEY_TEMPLATE.format(market=market)
        await r.setex(key, _PROGRESS_TTL, json.dumps(progress))
    except Exception:
        pass  # Non-critical


async def _clear_progress(market: str) -> None:
    """Clear the progress key from Redis."""
    try:
        r = await get_redis()
        await r.delete(_PROGRESS_KEY_TEMPLATE.format(market=market))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Internal: completion signal
# ---------------------------------------------------------------------------


async def _publish_completion(market: str, count: int) -> None:
    """Publish Redis signal so the backend can react to fresh profiles.

    PUBLISH works across all Redis DBs, so the backend (DB 0) will
    receive signals published from data-service (DB 5).
    """
    try:
        r = await get_redis()
        payload = json.dumps({
            "market": market,
            "count": count,
            "collected_at": datetime.now(timezone.utc).isoformat(),
        })
        await r.publish(
            f"profile_collection:complete:{market}",
            payload,
        )
        logger.info(
            "Published profile completion signal for %s (%d profiles)",
            market, count,
        )
    except Exception as exc:
        logger.warning(
            "Failed to publish profile completion for %s: %s", market, exc,
        )
