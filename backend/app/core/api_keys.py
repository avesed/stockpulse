"""API key manager --- loads keys from DB, caches in memory.

On startup StockPulse reads ``provider_configs`` to obtain API keys that
are configured through the admin UI.  A background Redis subscriber
listens on ``sp:reload_provider_keys`` so the backend can push instant
updates whenever an admin saves new provider configuration.

Providers that configure multiple API keys (via ``config_json.extra_api_keys``)
get automatic round-robin rotation through :func:`get_next_api_key`.

Usage in providers::

    from app.core.api_keys import get_api_key, get_next_api_key

    api_key = get_api_key("finnhub")       # primary key (DB -> env fallback)
    api_key = get_next_api_key("finnhub")   # round-robin across all keys
"""
from __future__ import annotations

import asyncio
import logging
import threading
from typing import Callable, Optional

from app.config import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------
_api_keys: dict[str, str] = {}

# Multi-key pools: provider_name -> list of all keys (primary + extras)
_api_key_pools: dict[str, list[str]] = {}

# Round-robin cursor per provider (thread-safe via _rr_lock)
_api_key_index: dict[str, int] = {}
_rr_lock = threading.Lock()

# Callbacks invoked after keys are reloaded from DB
_reload_callbacks: list[Callable] = []

# provider_name -> env var name on Settings (fallback when not in DB)
_ENV_FALLBACK = {
    "finnhub": "FINNHUB_API_KEY",
    "tiingo": "TIINGO_API_KEY",
    "tushare": "TUSHARE_TOKEN",
    "massive": "MASSIVE_API_KEY",
}

# Providers that support multi-key env var (comma-separated)
_ENV_MULTI_FALLBACK = {
    "finnhub": "FINNHUB_API_KEYS",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def get_api_key(name: str) -> Optional[str]:
    """Return primary API key by provider name.  DB value takes priority over env."""
    val = _api_keys.get(name)
    if val:
        return val
    # Fallback to environment variable via Settings
    env_attr = _ENV_FALLBACK.get(name)
    if env_attr:
        return getattr(get_settings(), env_attr, None) or None
    return None


def get_api_keys(name: str) -> list[str]:
    """Return all configured API keys for a provider.

    Priority: DB pool -> single DB key -> multi env var -> single env var.
    """
    # DB pool (primary + extras from config_json)
    pool = _api_key_pools.get(name)
    if pool:
        return list(pool)

    # Single DB key
    val = _api_keys.get(name)
    if val:
        return [val]

    # Env fallback: multi-key comma-separated
    multi_env = _ENV_MULTI_FALLBACK.get(name)
    if multi_env:
        raw = getattr(get_settings(), multi_env, None)
        if raw:
            keys = _parse_comma_keys(raw)
            if keys:
                return keys

    # Env fallback: single key
    env_attr = _ENV_FALLBACK.get(name)
    if env_attr:
        val = getattr(get_settings(), env_attr, None)
        if val:
            return [val]

    return []


def get_next_api_key(name: str) -> Optional[str]:
    """Return the next API key via round-robin.

    Thread-safe for use in executor threads.
    """
    keys = get_api_keys(name)
    if not keys:
        return None
    if len(keys) == 1:
        return keys[0]
    with _rr_lock:
        idx = _api_key_index.get(name, 0) % len(keys)
        _api_key_index[name] = idx + 1
        return keys[idx]


def get_key_pool_size(name: str) -> int:
    """Return the number of API keys configured for a provider."""
    return len(get_api_keys(name))


def register_reload_callback(fn: Callable) -> None:
    """Register a callback invoked after keys are reloaded from DB."""
    if fn not in _reload_callbacks:
        _reload_callbacks.append(fn)


def unregister_reload_callback(fn: Callable) -> None:
    """Remove a previously registered reload callback."""
    try:
        _reload_callbacks.remove(fn)
    except ValueError:
        pass


# ---------------------------------------------------------------------------
# DB loader (asyncpg pool, lightweight one-shot query)
# ---------------------------------------------------------------------------
async def load_api_keys_from_db() -> None:
    """Read API keys from ``provider_configs`` and cache in memory."""
    try:
        from app.core.database import get_db_pool

        pool = get_db_pool()
        rows = await pool.fetch(
            "SELECT provider_name, api_key, config_json FROM provider_configs "
            "WHERE is_enabled = true"
        )
        loaded = 0
        seen: set[str] = set()
        for row in rows:
            name = row["provider_name"]
            key = row["api_key"]
            config = row["config_json"]
            seen.add(name)

            if key:
                _api_keys[name] = key
                loaded += 1
            else:
                _api_keys.pop(name, None)

            # Build multi-key pool from primary key + config_json.extra_api_keys
            key_pool: list[str] = []
            if key:
                key_pool.append(key)
            # asyncpg returns JSONB as str; parse if needed
            if isinstance(config, str):
                import json
                try:
                    config = json.loads(config)
                except (json.JSONDecodeError, TypeError):
                    config = None
            if isinstance(config, dict):
                extras = config.get("extra_api_keys")
                if isinstance(extras, list):
                    for k in extras:
                        if isinstance(k, str) and k.strip() and k.strip() not in key_pool:
                            key_pool.append(k.strip())
            if len(key_pool) > 1:
                _api_key_pools[name] = key_pool
                logger.info("API key pool for %s: %d keys", name, len(key_pool))
            else:
                _api_key_pools.pop(name, None)

        # Remove keys for providers no longer in DB
        for stale in set(_api_keys.keys()) - seen:
            _api_keys.pop(stale, None)
        for stale in set(_api_key_pools.keys()) - seen:
            _api_key_pools.pop(stale, None)

        logger.info("Loaded %d API keys from provider_configs", loaded)

        # Fire reload callbacks
        await _fire_reload_callbacks()

    except Exception as e:
        logger.warning("Failed to load API keys from DB: %s", e)


async def _fire_reload_callbacks() -> None:
    """Invoke all registered reload callbacks."""
    for fn in _reload_callbacks:
        try:
            result = fn()
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.warning("Reload callback %s error: %s", fn, e)


# ---------------------------------------------------------------------------
# Redis subscriber (background task)
# ---------------------------------------------------------------------------
_subscriber_task: Optional[asyncio.Task] = None

_CHANNEL = "sp:reload_provider_keys"


async def _redis_key_subscriber() -> None:
    """Listen on Redis pub/sub for key-reload signals."""
    from app.core.redis import get_redis

    logger.info("API key subscriber started on channel '%s'", _CHANNEL)

    while True:
        try:
            redis = await get_redis()
            pubsub = redis.pubsub()
            await pubsub.subscribe(_CHANNEL)
            async for message in pubsub.listen():
                if message["type"] == "message":
                    logger.info("Received reload-keys signal, refreshing...")
                    await load_api_keys_from_db()
        except asyncio.CancelledError:
            logger.info("API key subscriber cancelled")
            return
        except Exception as e:
            logger.warning("API key subscriber error: %s --- retrying in 5s", e)
            await asyncio.sleep(5)


def start_subscriber() -> None:
    """Launch the background Redis subscriber task."""
    global _subscriber_task
    if _subscriber_task is None or _subscriber_task.done():
        _subscriber_task = asyncio.create_task(_redis_key_subscriber())


async def stop_subscriber() -> None:
    """Cancel the background subscriber."""
    global _subscriber_task
    if _subscriber_task and not _subscriber_task.done():
        _subscriber_task.cancel()
        try:
            await _subscriber_task
        except asyncio.CancelledError:
            pass
    _subscriber_task = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_comma_keys(raw: str) -> list[str]:
    """Parse comma-separated key string, deduplicate while preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for k in raw.split(","):
        k = k.strip()
        if k and k not in seen:
            seen.add(k)
            result.append(k)
    return result
