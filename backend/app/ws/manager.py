"""WebSocket ConnectionManager — tracks clients, routes subscriptions, heartbeat.

Module-level singleton, same pattern as ``app.core.redis._redis_client``.
"""
from __future__ import annotations

import asyncio
import logging
from uuid import uuid4
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from app.ws.protocol import serialize_event

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages all active WebSocket connections for this worker."""

    def __init__(self) -> None:
        # session_id → WebSocket
        self._connections: dict[str, WebSocket] = {}
        # session_id → metadata (auth_type, user/consumer, etc.)
        self._session_meta: dict[str, dict[str, Any]] = {}
        # symbol (uppercase) → set of session_ids subscribed to quotes
        self._symbol_subs: dict[str, set[str]] = {}
        # market → set of session_ids subscribed to collection progress
        self._collection_subs: dict[str, set[str]] = {}
        # session_ids subscribed to collector status events
        self._collector_status_subs: set[str] = set()
        self._lock = asyncio.Lock()

    # ----- lifecycle -----

    async def connect(self, websocket: WebSocket, meta: dict[str, Any]) -> str:
        """Register a new connection. Returns the session_id."""
        session_id = uuid4().hex[:16]
        async with self._lock:
            self._connections[session_id] = websocket
            self._session_meta[session_id] = meta
        logger.info(
            "WS connected: session=%s auth_type=%s",
            session_id, meta.get("auth_type"),
        )
        return session_id

    async def disconnect(self, session_id: str) -> None:
        """Remove a connection and all its subscriptions."""
        async with self._lock:
            self._connections.pop(session_id, None)
            self._session_meta.pop(session_id, None)
            self._collector_status_subs.discard(session_id)
            # Clean up symbol subs
            empty_symbols = []
            for symbol, sids in self._symbol_subs.items():
                sids.discard(session_id)
                if not sids:
                    empty_symbols.append(symbol)
            for symbol in empty_symbols:
                del self._symbol_subs[symbol]
            # Clean up collection subs
            empty_markets = []
            for market, sids in self._collection_subs.items():
                sids.discard(session_id)
                if not sids:
                    empty_markets.append(market)
            for market in empty_markets:
                del self._collection_subs[market]
        logger.info("WS disconnected: session=%s", session_id)

    # ----- subscriptions -----

    async def subscribe_quotes(self, session_id: str, symbols: list[str]) -> list[str]:
        """Subscribe session to quote updates for given symbols.

        Returns the list of symbols actually subscribed (after limit check).
        """
        meta = self._session_meta.get(session_id, {})
        max_subs = meta.get("max_subscriptions", 50)

        async with self._lock:
            current_count = sum(
                1 for sids in self._symbol_subs.values()
                if session_id in sids
            )
            accepted = []
            for sym in symbols:
                sym_upper = sym.upper()
                if current_count >= max_subs:
                    break
                sids = self._symbol_subs.setdefault(sym_upper, set())
                if session_id not in sids:
                    sids.add(session_id)
                    current_count += 1
                accepted.append(sym_upper)
            return accepted

    async def unsubscribe_quotes(self, session_id: str, symbols: list[str]) -> list[str]:
        """Unsubscribe session from quote updates for given symbols."""
        removed = []
        async with self._lock:
            for sym in symbols:
                sym_upper = sym.upper()
                sids = self._symbol_subs.get(sym_upper)
                if sids and session_id in sids:
                    sids.discard(session_id)
                    if not sids:
                        del self._symbol_subs[sym_upper]
                    removed.append(sym_upper)
        return removed

    async def subscribe_collection(self, session_id: str, markets: list[str]) -> list[str]:
        """Subscribe session to collection progress for given markets."""
        accepted = []
        async with self._lock:
            for m in markets:
                m_lower = m.lower()
                sids = self._collection_subs.setdefault(m_lower, set())
                sids.add(session_id)
                accepted.append(m_lower)
            # Also subscribe to collector status
            self._collector_status_subs.add(session_id)
        return accepted

    async def unsubscribe_collection(self, session_id: str, markets: list[str]) -> list[str]:
        """Unsubscribe session from collection progress."""
        removed = []
        async with self._lock:
            for m in markets:
                m_lower = m.lower()
                sids = self._collection_subs.get(m_lower)
                if sids and session_id in sids:
                    sids.discard(session_id)
                    if not sids:
                        del self._collection_subs[m_lower]
                    removed.append(m_lower)
        return removed

    # ----- broadcasting -----

    async def _send_to(self, session_id: str, data: bytes) -> bool:
        """Send raw bytes to a single session. Returns False if failed."""
        ws = self._connections.get(session_id)
        if ws is None or ws.client_state != WebSocketState.CONNECTED:
            return False
        try:
            await ws.send_bytes(data)
            return True
        except Exception:
            return False

    async def broadcast_to_symbol(self, symbol: str, event: dict[str, Any]) -> None:
        """Send an event to all sessions subscribed to a symbol."""
        sym_upper = symbol.upper()
        sids = self._symbol_subs.get(sym_upper)
        if not sids:
            return
        data = serialize_event(event)
        dead: list[str] = []
        for sid in list(sids):
            if not await self._send_to(sid, data):
                dead.append(sid)
        for sid in dead:
            await self.disconnect(sid)

    async def broadcast_collection_progress(self, market: str, event: dict[str, Any]) -> None:
        """Send collection progress to all sessions subscribed to a market."""
        m_lower = market.lower()
        sids = self._collection_subs.get(m_lower)
        if not sids:
            return
        data = serialize_event(event)
        dead: list[str] = []
        for sid in list(sids):
            if not await self._send_to(sid, data):
                dead.append(sid)
        for sid in dead:
            await self.disconnect(sid)

    async def broadcast_collector_status(self, event: dict[str, Any]) -> None:
        """Send collector status to all subscribers."""
        if not self._collector_status_subs:
            return
        data = serialize_event(event)
        dead: list[str] = []
        for sid in list(self._collector_status_subs):
            if not await self._send_to(sid, data):
                dead.append(sid)
        for sid in dead:
            await self.disconnect(sid)

    async def send_to_session(self, session_id: str, event: dict[str, Any]) -> bool:
        """Send event to a specific session."""
        data = serialize_event(event)
        return await self._send_to(session_id, data)

    # ----- stats -----

    def get_stats(self) -> dict[str, Any]:
        """Return connection and subscription statistics."""
        return {
            "connections": len(self._connections),
            "symbol_subscriptions": sum(len(s) for s in self._symbol_subs.values()),
            "unique_symbols": len(self._symbol_subs),
            "collection_subscriptions": sum(len(s) for s in self._collection_subs.values()),
            "collector_status_subscriptions": len(self._collector_status_subs),
        }

    def get_subscribed_symbols(self) -> set[str]:
        """Return set of all symbols with at least one subscriber."""
        return set(self._symbol_subs.keys())


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_manager: ConnectionManager | None = None


def get_manager() -> ConnectionManager:
    """Get or create the singleton ConnectionManager for this worker."""
    global _manager
    if _manager is None:
        _manager = ConnectionManager()
    return _manager
