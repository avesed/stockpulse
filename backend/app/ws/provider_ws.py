"""Per-provider WebSocket endpoint factory.

Creates dedicated WS endpoints for each data provider with upstream
collector support.  Example:

    WS /api/v1/ws/massive   → Massive real-time trades
    WS /api/v1/ws/finnhub   → Finnhub real-time trades
    WS /api/v1/ws/yfinance  → YFinance polling quotes

Uses the global ConnectionManager with ``provider_filter`` metadata
so that ``broadcast_to_symbol()`` only delivers events matching the
provider source.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.config import get_settings
from app.ws.auth import authenticate_consumer_ws
from app.ws.manager import get_manager
from app.ws.protocol import (
    make_connected,
    make_error,
    make_pong,
    make_subscribed,
    make_unsubscribed,
    parse_client_message,
)

logger = logging.getLogger(__name__)

WS_CLOSE_AUTH_FAILED = 4001


def create_provider_ws_router(provider_name: str) -> APIRouter:
    """Build a router with a single WS endpoint for a specific provider."""

    router = APIRouter()

    @router.websocket(f"/api/v1/ws/{provider_name}")
    async def ws_provider(websocket: WebSocket) -> None:
        consumer = await authenticate_consumer_ws(websocket)
        if consumer is None:
            await websocket.close(code=WS_CLOSE_AUTH_FAILED, reason="Unauthorized")
            return

        await websocket.accept()

        manager = get_manager()
        settings = get_settings()
        session_id = await manager.connect(websocket, {
            "auth_type": "consumer",
            "consumer_id": str(consumer.id),
            "consumer_name": consumer.name,
            "provider_filter": provider_name,
            "max_subscriptions": min(
                settings.WS_MAX_SUBSCRIPTIONS_PER_SESSION,
                consumer.rate_limit or settings.WS_MAX_SUBSCRIPTIONS_PER_SESSION,
            ),
        })

        logger.info(
            "Provider WS connected: session=%s provider=%s consumer=%s",
            session_id, provider_name, consumer.name,
        )

        await manager.send_to_session(session_id, make_connected(session_id))

        try:
            await _provider_message_loop(session_id, websocket, provider_name)
        except WebSocketDisconnect:
            pass
        except Exception:
            logger.debug(
                "Provider WS error: session=%s provider=%s",
                session_id, provider_name, exc_info=True,
            )
        finally:
            await manager.disconnect(session_id)

    return router


async def _provider_message_loop(
    session_id: str,
    websocket: WebSocket,
    provider_name: str,
) -> None:
    """Message loop for per-provider WS — same protocol as /ws/data
    but auto-starts the upstream collector on subscribe."""
    manager = get_manager()

    while True:
        raw = await websocket.receive_text()
        msg = parse_client_message(raw)
        if msg is None:
            await manager.send_to_session(
                session_id, make_error("invalid_message", "Malformed JSON"),
            )
            continue

        msg_type = msg.get("type")

        if msg_type == "ping":
            await manager.send_to_session(session_id, make_pong())

        elif msg_type == "subscribe":
            await _handle_subscribe(session_id, msg, provider_name)

        elif msg_type == "unsubscribe":
            await _handle_unsubscribe(session_id, msg)

        else:
            await manager.send_to_session(
                session_id,
                make_error("unknown_type", f"Unknown message type: {msg_type}"),
            )


async def _handle_subscribe(
    session_id: str,
    msg: dict[str, Any],
    provider_name: str,
) -> None:
    manager = get_manager()
    channel = msg.get("channel", "")

    if channel != "quotes":
        await manager.send_to_session(
            session_id,
            make_error("invalid_channel", f"Only 'quotes' channel is supported on /ws/{provider_name}"),
        )
        return

    symbols = msg.get("symbols", [])
    if not isinstance(symbols, list) or not symbols:
        await manager.send_to_session(
            session_id,
            make_error("invalid_params", "symbols must be a non-empty list"),
        )
        return

    accepted = await manager.subscribe_quotes(session_id, symbols)
    await manager.send_to_session(
        session_id, make_subscribed("quotes", symbols=accepted),
    )

    # Auto-start the upstream collector for this provider
    if accepted:
        try:
            from app.ws.upstream.collector_service import start_collector
            result = await start_collector(provider_name, accepted)
            logger.info(
                "Auto-started %s collector for WS session %s: %s",
                provider_name, session_id, result.get("message", ""),
            )
        except Exception as e:
            logger.warning(
                "Failed to auto-start %s collector: %s", provider_name, e,
            )


async def _handle_unsubscribe(session_id: str, msg: dict[str, Any]) -> None:
    manager = get_manager()
    channel = msg.get("channel", "")

    if channel != "quotes":
        return

    symbols = msg.get("symbols", [])
    if not isinstance(symbols, list) or not symbols:
        return

    removed = await manager.unsubscribe_quotes(session_id, symbols)
    await manager.send_to_session(
        session_id, make_unsubscribed("quotes", symbols=removed),
    )
