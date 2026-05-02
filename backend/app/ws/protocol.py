"""WebSocket message protocol — types, parsing, serialization.

Uses TypedDict + orjson for minimal overhead on high-frequency messages.
All messages are JSON objects with a ``type`` field.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Literal, TypedDict

import orjson

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Client → Server message types
# ---------------------------------------------------------------------------

ClientMessageType = Literal[
    "subscribe", "unsubscribe", "command", "ping",
]


class SubscribeMessage(TypedDict):
    type: Literal["subscribe"]
    channel: str  # "quotes" | "collection_progress"
    symbols: list[str]  # for quotes
    markets: list[str]  # for collection_progress


class UnsubscribeMessage(TypedDict):
    type: Literal["unsubscribe"]
    channel: str
    symbols: list[str]
    markets: list[str]


class CommandMessage(TypedDict):
    type: Literal["command"]
    action: str  # "start_collector" | "stop_collector"
    provider: str
    symbols: list[str]


class PingMessage(TypedDict):
    type: Literal["ping"]


# ---------------------------------------------------------------------------
# Server → Client event types
# ---------------------------------------------------------------------------

ServerEventType = Literal[
    "connected", "quote", "trade", "collection_progress",
    "collector_status", "subscribed", "unsubscribed",
    "error", "pong", "token_expiring",
]


# ---------------------------------------------------------------------------
# Parsing & serialization
# ---------------------------------------------------------------------------

def parse_client_message(raw: str | bytes) -> dict[str, Any] | None:
    """Parse a raw WebSocket text frame into a message dict.

    Returns None if the message is malformed.
    """
    try:
        if isinstance(raw, bytes):
            msg = orjson.loads(raw)
        else:
            msg = orjson.loads(raw.encode("utf-8"))
        if not isinstance(msg, dict) or "type" not in msg:
            return None
        return msg
    except (orjson.JSONDecodeError, ValueError):
        logger.debug("Malformed WS message: %s", raw[:200] if raw else "")
        return None


def serialize_event(event: dict[str, Any]) -> bytes:
    """Serialize a server event dict to bytes for sending."""
    return orjson.dumps(event)


# ---------------------------------------------------------------------------
# Event builder helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_connected(session_id: str) -> dict[str, Any]:
    return {"type": "connected", "ts": _now_iso(), "session_id": session_id}


def make_pong() -> dict[str, Any]:
    return {"type": "pong", "ts": _now_iso()}


def make_error(code: str, message: str) -> dict[str, Any]:
    return {"type": "error", "code": code, "message": message}


def make_subscribed(channel: str, *, symbols: list[str] | None = None,
                    markets: list[str] | None = None) -> dict[str, Any]:
    event: dict[str, Any] = {"type": "subscribed", "channel": channel}
    if symbols is not None:
        event["symbols"] = symbols
    if markets is not None:
        event["markets"] = markets
    return event


def make_unsubscribed(channel: str, *, symbols: list[str] | None = None,
                      markets: list[str] | None = None) -> dict[str, Any]:
    event: dict[str, Any] = {"type": "unsubscribed", "channel": channel}
    if symbols is not None:
        event["symbols"] = symbols
    if markets is not None:
        event["markets"] = markets
    return event


def make_quote(symbol: str, price: float, volume: int,
               change: float, change_pct: float,
               source: str) -> dict[str, Any]:
    return {
        "type": "quote",
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "change": change,
        "change_pct": change_pct,
        "source": source,
        "ts": _now_iso(),
    }


def make_trade(symbol: str, price: float, volume: int,
               source: str, conditions: list[str] | None = None) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "trade",
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "source": source,
        "ts": _now_iso(),
    }
    if conditions:
        event["conditions"] = conditions
    return event


def make_collection_progress(
    market: str, symbols_done: int, symbols_total: int,
    new_bars: int, percent: int,
    error_count: int = 0, elapsed_seconds: float = 0,
    job_type: str = "collect",
    estimated_remaining: float | None = None,
) -> dict[str, Any]:
    msg: dict[str, Any] = {
        "type": "collection_progress",
        "job_type": job_type,
        "market": market,
        "symbols_done": symbols_done,
        "symbols_total": symbols_total,
        "new_bars": new_bars,
        "percent": percent,
        "error_count": error_count,
        "elapsed_seconds": round(elapsed_seconds, 1),
        "updated_at": _now_iso(),
    }
    if estimated_remaining is not None:
        msg["estimated_remaining"] = round(estimated_remaining, 1)
    return msg


def make_collector_status(
    provider: str, status: str,
    symbols_count: int = 0,
    last_message_at: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "collector_status",
        "provider": provider,
        "status": status,
        "symbols_count": symbols_count,
    }
    if last_message_at:
        event["last_message_at"] = last_message_at
    if error:
        event["error"] = error
    return event


def make_token_expiring(expires_in_seconds: int) -> dict[str, Any]:
    return {
        "type": "token_expiring",
        "expires_in_seconds": expires_in_seconds,
        "ts": _now_iso(),
    }
