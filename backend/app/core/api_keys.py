"""API key manager — loads keys from DB, caches in memory.

On startup StockPulse reads ``provider_configs`` to obtain API keys that
are configured through the admin UI.  A background Redis subscriber
listens on ``sp:reload_provider_keys`` so the backend can push instant
updates whenever an admin saves new provider configuration.

Usage in providers::

    from app.core.api_keys import get_api_key

    api_key = get_api_key("finnhub")  # DB value -> env fallback
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from app.config import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------
_api_keys: dict[str, str] = {}

# provider_name -> env var name on Settings (fallback when not in DB)
_ENV_FALLBACK = {
    "finnhub": "FINNHUB_API_KEY",
    "tiingo": "TIINGO_API_KEY",
    "tushare": "TUSHARE_TOKEN",
    "polygon": "POLYGON_API_KEY",
}


def get_api_key(name: str) -> Optional[str]:
    """Return API key by provider name.  DB value takes priority over env."""
    val = _api_keys.get(name)
    if val:
        return val
    # Fallback to environment variable via Settings
    env_attr = _ENV_FALLBACK.get(name)
    if env_attr:
        return getattr(get_settings(), env_attr, None) or None
    return None


# ---------------------------------------------------------------------------
# DB loader (asyncpg pool, lightweight one-shot query)
# ---------------------------------------------------------------------------
async def load_api_keys_from_db() -> None:
    """Read API keys from ``provider_configs`` and cache in memory."""
    try:
        from app.core.database import get_db_pool

        pool = get_db_pool()
        rows = await pool.fetch(
            "SELECT provider_name, api_key FROM provider_configs "
            "WHERE is_enabled = true AND api_key IS NOT NULL"
        )
        loaded = 0
        seen: set[str] = set()
        for row in rows:
            name = row["provider_name"]
            key = row["api_key"]
            seen.add(name)
            if key:
                _api_keys[name] = key
                loaded += 1
            else:
                _api_keys.pop(name, None)

        # Remove keys for providers no longer in DB
        for stale in set(_api_keys.keys()) - seen:
            _api_keys.pop(stale, None)

        logger.info("Loaded %d API keys from provider_configs", loaded)
    except Exception as e:
        logger.warning("Failed to load API keys from DB: %s", e)


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
            logger.warning("API key subscriber error: %s — retrying in 5s", e)
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
