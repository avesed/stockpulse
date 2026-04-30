"""WebSocket route handlers.

Two endpoints:
- ``/api/v1/ws/admin``  — JWT auth, admin features (collection progress, quotes, collector control)
- ``/api/v1/ws/data``   — X-API-Key auth, consumer features (real-time quotes)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.config import get_settings
from app.ws.auth import authenticate_admin_ws, authenticate_consumer_ws
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
router = APIRouter()

WS_CLOSE_AUTH_FAILED = 4001
WS_CLOSE_INVALID_MSG = 4002


@router.websocket("/api/v1/ws/admin")
async def ws_admin(websocket: WebSocket) -> None:
    """Admin WebSocket endpoint — JWT auth via query param."""
    user = await authenticate_admin_ws(websocket)
    if user is None:
        await websocket.close(code=WS_CLOSE_AUTH_FAILED, reason="Unauthorized")
        return

    await websocket.accept()

    manager = get_manager()
    settings = get_settings()
    session_id = await manager.connect(websocket, {
        "auth_type": "admin",
        "user_id": user.id,
        "email": user.email,
        "max_subscriptions": settings.WS_MAX_SUBSCRIPTIONS_PER_SESSION,
    })

    await manager.send_to_session(session_id, make_connected(session_id))

    try:
        await _message_loop(session_id, websocket, is_admin=True)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.debug("Admin WS error: session=%s", session_id, exc_info=True)
    finally:
        await manager.disconnect(session_id)


@router.websocket("/api/v1/ws/data")
async def ws_data(websocket: WebSocket) -> None:
    """Public data WebSocket endpoint — X-API-Key auth via query param."""
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
        "max_subscriptions": min(
            settings.WS_MAX_SUBSCRIPTIONS_PER_SESSION,
            consumer.rate_limit or settings.WS_MAX_SUBSCRIPTIONS_PER_SESSION,
        ),
    })

    await manager.send_to_session(session_id, make_connected(session_id))

    try:
        await _message_loop(session_id, websocket, is_admin=False)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.debug("Data WS error: session=%s", session_id, exc_info=True)
    finally:
        await manager.disconnect(session_id)


async def _message_loop(session_id: str, websocket: WebSocket, *, is_admin: bool) -> None:
    """Core receive loop — parses and dispatches client messages."""
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
            await _handle_subscribe(session_id, msg, is_admin=is_admin)

        elif msg_type == "unsubscribe":
            await _handle_unsubscribe(session_id, msg, is_admin=is_admin)

        elif msg_type == "command":
            if not is_admin:
                await manager.send_to_session(
                    session_id, make_error("forbidden", "Commands require admin auth"),
                )
                continue
            await _handle_command(session_id, msg)

        else:
            await manager.send_to_session(
                session_id, make_error("unknown_type", f"Unknown message type: {msg_type}"),
            )


async def _handle_subscribe(session_id: str, msg: dict[str, Any], *, is_admin: bool) -> None:
    manager = get_manager()
    channel = msg.get("channel", "")

    if channel == "quotes":
        symbols = msg.get("symbols", [])
        if not isinstance(symbols, list) or not symbols:
            await manager.send_to_session(
                session_id, make_error("invalid_params", "symbols must be a non-empty list"),
            )
            return
        accepted = await manager.subscribe_quotes(session_id, symbols)
        await manager.send_to_session(
            session_id, make_subscribed("quotes", symbols=accepted),
        )

        if accepted:
            settings = get_settings()
            default_provider = settings.WS_DEFAULT_PROVIDER
            try:
                from app.ws.upstream.collector_service import start_collector
                result = await start_collector(default_provider, accepted)
                logger.info(
                    "Auto-started %s collector for /ws/data session %s: %s",
                    default_provider, session_id, result.get("message", ""),
                )
            except Exception as e:
                logger.warning(
                    "Failed to auto-start %s collector: %s", default_provider, e,
                )

    elif channel == "collection_progress":
        if not is_admin:
            await manager.send_to_session(
                session_id, make_error("forbidden", "collection_progress requires admin auth"),
            )
            return
        markets = msg.get("markets", [])
        if not isinstance(markets, list) or not markets:
            await manager.send_to_session(
                session_id, make_error("invalid_params", "markets must be a non-empty list"),
            )
            return
        accepted = await manager.subscribe_collection(session_id, markets)
        await manager.send_to_session(
            session_id, make_subscribed("collection_progress", markets=accepted),
        )

    else:
        await manager.send_to_session(
            session_id, make_error("invalid_channel", f"Unknown channel: {channel}"),
        )


async def _handle_unsubscribe(session_id: str, msg: dict[str, Any], *, is_admin: bool) -> None:
    manager = get_manager()
    channel = msg.get("channel", "")

    if channel == "quotes":
        symbols = msg.get("symbols", [])
        if not isinstance(symbols, list) or not symbols:
            return
        removed = await manager.unsubscribe_quotes(session_id, symbols)
        await manager.send_to_session(
            session_id, make_unsubscribed("quotes", symbols=removed),
        )

    elif channel == "collection_progress":
        markets = msg.get("markets", [])
        if not isinstance(markets, list) or not markets:
            return
        removed = await manager.unsubscribe_collection(session_id, markets)
        await manager.send_to_session(
            session_id, make_unsubscribed("collection_progress", markets=removed),
        )


async def _handle_command(session_id: str, msg: dict[str, Any]) -> None:
    """Handle admin command messages (start/stop collectors)."""
    manager = get_manager()
    action = msg.get("action", "")

    if action == "start_collector":
        provider = msg.get("provider", "")
        symbols = msg.get("symbols", [])
        if not provider:
            await manager.send_to_session(
                session_id, make_error("invalid_params", "provider is required"),
            )
            return
        try:
            from app.ws.upstream.collector_service import start_collector
            result = await start_collector(provider, symbols)
            await manager.send_to_session(session_id, {
                "type": "command_result",
                "action": "start_collector",
                "success": result.get("success", False),
                "message": result.get("message", ""),
            })
        except ImportError:
            await manager.send_to_session(
                session_id, make_error("not_available", "Collector service not available"),
            )
        except Exception as e:
            await manager.send_to_session(
                session_id, make_error("command_failed", str(e)),
            )

    elif action == "stop_collector":
        provider = msg.get("provider", "")
        if not provider:
            await manager.send_to_session(
                session_id, make_error("invalid_params", "provider is required"),
            )
            return
        try:
            from app.ws.upstream.collector_service import stop_collector
            result = await stop_collector(provider)
            await manager.send_to_session(session_id, {
                "type": "command_result",
                "action": "stop_collector",
                "success": result.get("success", False),
                "message": result.get("message", ""),
            })
        except ImportError:
            await manager.send_to_session(
                session_id, make_error("not_available", "Collector service not available"),
            )
        except Exception as e:
            await manager.send_to_session(
                session_id, make_error("command_failed", str(e)),
            )

    elif action == "get_collector_status":
        try:
            from app.ws.upstream.collector_service import get_all_status
            status = await get_all_status()
            await manager.send_to_session(session_id, {
                "type": "command_result",
                "action": "get_collector_status",
                "success": True,
                "collectors": status,
            })
        except ImportError:
            await manager.send_to_session(
                session_id, make_error("not_available", "Collector service not available"),
            )

    else:
        await manager.send_to_session(
            session_id, make_error("unknown_action", f"Unknown action: {action}"),
        )
