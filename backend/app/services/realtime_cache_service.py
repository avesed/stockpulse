"""Real-time quote cache service.

Maintains last-known quotes from WebSocket streams in Redis.
Used for REST API fallback and initial snapshot on WS connect.

Key pattern: ``sp:rt:quote:{symbol}`` (TTL 60s)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from app.core.redis import get_redis

logger = logging.getLogger(__name__)

_QUOTE_KEY_PREFIX = "sp:rt:quote:"
_QUOTE_TTL = 60  # seconds


async def update_quote_cache(
    symbol: str, price: float, volume: int, source: str,
) -> None:
    """Update the cached real-time quote for a symbol."""
    try:
        r = await get_redis()
        data = {
            "symbol": symbol.upper(),
            "price": price,
            "volume": volume,
            "source": source,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        await r.setex(
            f"{_QUOTE_KEY_PREFIX}{symbol.upper()}",
            _QUOTE_TTL,
            json.dumps(data),
        )
    except Exception as e:
        logger.debug("Failed to update rt cache for %s: %s", symbol, e)


async def get_quote_cache(symbol: str) -> Optional[dict[str, Any]]:
    """Get the cached real-time quote for a symbol."""
    try:
        r = await get_redis()
        data = await r.get(f"{_QUOTE_KEY_PREFIX}{symbol.upper()}")
        if data is not None:
            return json.loads(data)
    except Exception as e:
        logger.debug("Failed to read rt cache for %s: %s", symbol, e)
    return None


async def get_quotes_cache(symbols: list[str]) -> dict[str, dict[str, Any]]:
    """Get cached real-time quotes for multiple symbols."""
    result = {}
    try:
        r = await get_redis()
        keys = [f"{_QUOTE_KEY_PREFIX}{s.upper()}" for s in symbols]
        if not keys:
            return result
        values = await r.mget(keys)
        for sym, val in zip(symbols, values):
            if val is not None:
                result[sym.upper()] = json.loads(val)
    except Exception as e:
        logger.debug("Failed to read rt cache batch: %s", e)
    return result
