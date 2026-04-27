"""Redis pub/sub fan-out for multi-worker WebSocket broadcasting.

Each uvicorn worker runs a subscriber that dispatches incoming messages
to its local ConnectionManager. Upstream collectors and the collection
service publish events to Redis channels.

Channels:
- ``sp:rt:trades``                — normalized trade events
- ``sp:rt:quotes``                — aggregated quote snapshots
- ``sp:rt:collection_progress``   — collection progress events
- ``sp:rt:collector_status``      — collector lifecycle events
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import orjson
import redis.asyncio as aioredis

from app.core.redis import get_redis
from app.ws.manager import ConnectionManager

logger = logging.getLogger(__name__)

# Channel names
CH_TRADES = "sp:rt:trades"
CH_QUOTES = "sp:rt:quotes"
CH_COLLECTION_PROGRESS = "sp:rt:collection_progress"
CH_COLLECTOR_STATUS = "sp:rt:collector_status"

ALL_CHANNELS = [CH_TRADES, CH_QUOTES, CH_COLLECTION_PROGRESS, CH_COLLECTOR_STATUS]

_subscriber_task: asyncio.Task | None = None
_stop_event: asyncio.Event | None = None


# ---------------------------------------------------------------------------
# Publisher helpers (used by upstream collectors & collection_service)
# ---------------------------------------------------------------------------

async def publish_trade(event: dict[str, Any]) -> None:
    """Publish a normalized trade event."""
    try:
        r = await get_redis()
        await r.publish(CH_TRADES, orjson.dumps(event))
    except Exception as e:
        logger.warning("Failed to publish trade: %s", e)


async def publish_quote(event: dict[str, Any]) -> None:
    """Publish a quote snapshot event."""
    try:
        r = await get_redis()
        await r.publish(CH_QUOTES, orjson.dumps(event))
    except Exception as e:
        logger.warning("Failed to publish quote: %s", e)


async def publish_collection_progress(event: dict[str, Any]) -> None:
    """Publish a collection progress event."""
    try:
        r = await get_redis()
        await r.publish(CH_COLLECTION_PROGRESS, orjson.dumps(event))
    except Exception as e:
        logger.warning("Failed to publish collection progress: %s", e)


async def publish_collector_status(event: dict[str, Any]) -> None:
    """Publish a collector status event."""
    try:
        r = await get_redis()
        await r.publish(CH_COLLECTOR_STATUS, orjson.dumps(event))
    except Exception as e:
        logger.warning("Failed to publish collector status: %s", e)


# ---------------------------------------------------------------------------
# Subscriber (runs as background task in every worker)
# ---------------------------------------------------------------------------

async def _fanout_loop(manager: ConnectionManager) -> None:
    """Subscribe to all rt channels and dispatch to local ConnectionManager."""
    while not _stop_event.is_set():
        try:
            r = await get_redis()
            pubsub = r.pubsub()
            await pubsub.subscribe(*ALL_CHANNELS)
            logger.info("WebSocket fanout subscriber started on %d channels", len(ALL_CHANNELS))

            while not _stop_event.is_set():
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0,
                )
                if msg is None:
                    continue
                if msg["type"] != "message":
                    continue

                try:
                    channel = msg["channel"]
                    # channel may be bytes or str depending on redis config
                    if isinstance(channel, bytes):
                        channel = channel.decode("utf-8")
                    raw_data = msg["data"]
                    if isinstance(raw_data, bytes):
                        data = orjson.loads(raw_data)
                    else:
                        data = orjson.loads(raw_data.encode("utf-8"))

                    await _dispatch(manager, channel, data)
                except Exception:
                    logger.debug("Failed to dispatch fanout message", exc_info=True)

        except asyncio.CancelledError:
            break
        except Exception:
            logger.warning("Fanout subscriber error, reconnecting in 3s...", exc_info=True)
            try:
                await asyncio.wait_for(_stop_event.wait(), timeout=3.0)
                break  # stop_event was set
            except asyncio.TimeoutError:
                continue  # retry

    logger.info("WebSocket fanout subscriber stopped")


async def _dispatch(manager: ConnectionManager, channel: str, data: dict[str, Any]) -> None:
    """Route a pub/sub message to the appropriate manager broadcast method."""
    if channel == CH_TRADES or channel == CH_QUOTES:
        symbol = data.get("symbol")
        if symbol:
            await manager.broadcast_to_symbol(symbol, data)
    elif channel == CH_COLLECTION_PROGRESS:
        market = data.get("market")
        if market:
            await manager.broadcast_collection_progress(market, data)
    elif channel == CH_COLLECTOR_STATUS:
        await manager.broadcast_collector_status(data)


# ---------------------------------------------------------------------------
# Lifecycle (called from main.py lifespan)
# ---------------------------------------------------------------------------

def start_fanout_subscriber(manager: ConnectionManager) -> None:
    """Start the Redis fanout subscriber as a background task."""
    global _subscriber_task, _stop_event
    _stop_event = asyncio.Event()
    _subscriber_task = asyncio.create_task(_fanout_loop(manager))
    logger.info("Fanout subscriber task created")


async def stop_fanout_subscriber() -> None:
    """Stop the Redis fanout subscriber."""
    global _subscriber_task, _stop_event
    if _stop_event is not None:
        _stop_event.set()
    if _subscriber_task is not None:
        _subscriber_task.cancel()
        try:
            await _subscriber_task
        except asyncio.CancelledError:
            pass
        _subscriber_task = None
    logger.info("Fanout subscriber stopped")
