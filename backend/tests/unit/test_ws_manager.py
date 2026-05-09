"""Tests for WebSocket ConnectionManager and protocol helpers."""
from __future__ import annotations

import pytest

from app.ws.manager import ConnectionManager
from app.ws.protocol import (
    make_connected,
    make_error,
    make_pong,
    make_quote,
    make_subscribed,
    make_unsubscribed,
    parse_client_message,
    serialize_event,
)


# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------

class TestParseClientMessage:
    def test_valid_json(self):
        msg = parse_client_message('{"type": "ping"}')
        assert msg is not None
        assert msg["type"] == "ping"

    def test_bytes_input(self):
        msg = parse_client_message(b'{"type": "subscribe", "channel": "quotes"}')
        assert msg is not None
        assert msg["channel"] == "quotes"

    def test_malformed_json(self):
        assert parse_client_message("not json") is None

    def test_missing_type_field(self):
        assert parse_client_message('{"channel": "quotes"}') is None

    def test_non_dict(self):
        assert parse_client_message("[1, 2, 3]") is None


class TestSerializeEvent:
    def test_produces_bytes(self):
        data = serialize_event({"type": "pong"})
        assert isinstance(data, bytes)
        assert b"pong" in data


class TestEventBuilders:
    def test_make_connected(self):
        e = make_connected("abc123")
        assert e["type"] == "connected"
        assert e["session_id"] == "abc123"
        assert "ts" in e

    def test_make_pong(self):
        e = make_pong()
        assert e["type"] == "pong"

    def test_make_error(self):
        e = make_error("forbidden", "Access denied")
        assert e["type"] == "error"
        assert e["code"] == "forbidden"
        assert e["message"] == "Access denied"

    def test_make_subscribed_with_symbols(self):
        e = make_subscribed("quotes", symbols=["AAPL", "MSFT"])
        assert e["type"] == "subscribed"
        assert e["channel"] == "quotes"
        assert e["symbols"] == ["AAPL", "MSFT"]

    def test_make_unsubscribed_with_markets(self):
        e = make_unsubscribed("collection_progress", markets=["us"])
        assert e["type"] == "unsubscribed"
        assert e["markets"] == ["us"]

    def test_make_quote(self):
        e = make_quote("AAPL", 150.0, 1000, 2.5, 1.7, "yfinance")
        assert e["type"] == "quote"
        assert e["symbol"] == "AAPL"
        assert e["price"] == 150.0
        assert e["source"] == "yfinance"


# ---------------------------------------------------------------------------
# ConnectionManager
# ---------------------------------------------------------------------------

class _FakeWebSocket:
    """Minimal mock to satisfy ConnectionManager's interface."""

    def __init__(self):
        self.sent: list[bytes] = []
        self.client_state = type("S", (), {"CONNECTED": "CONNECTED"})()
        # Make client_state match the check in _send_to
        from starlette.websockets import WebSocketState
        self.client_state = WebSocketState.CONNECTED

    async def send_bytes(self, data: bytes):
        self.sent.append(data)


class TestConnectionManager:
    async def test_connect_returns_session_id(self):
        mgr = ConnectionManager()
        ws = _FakeWebSocket()
        sid = await mgr.connect(ws, {"auth_type": "admin"})
        assert isinstance(sid, str) and len(sid) > 0

    async def test_disconnect_removes_session(self):
        mgr = ConnectionManager()
        ws = _FakeWebSocket()
        sid = await mgr.connect(ws, {"auth_type": "admin"})
        await mgr.disconnect(sid)
        stats = mgr.get_stats()
        assert stats["connections"] == 0

    async def test_subscribe_quotes(self):
        mgr = ConnectionManager()
        ws = _FakeWebSocket()
        sid = await mgr.connect(ws, {"auth_type": "admin", "max_subscriptions": 50})
        accepted = await mgr.subscribe_quotes(sid, ["AAPL", "msft"])
        assert accepted == ["AAPL", "MSFT"]
        assert mgr.get_subscribed_symbols() == {"AAPL", "MSFT"}

    async def test_subscribe_respects_max(self):
        mgr = ConnectionManager()
        ws = _FakeWebSocket()
        sid = await mgr.connect(ws, {"auth_type": "consumer", "max_subscriptions": 2})
        accepted = await mgr.subscribe_quotes(sid, ["A", "B", "C", "D"])
        assert len(accepted) == 2

    async def test_unsubscribe_quotes(self):
        mgr = ConnectionManager()
        ws = _FakeWebSocket()
        sid = await mgr.connect(ws, {"auth_type": "admin", "max_subscriptions": 50})
        await mgr.subscribe_quotes(sid, ["AAPL", "MSFT"])
        removed = await mgr.unsubscribe_quotes(sid, ["AAPL"])
        assert removed == ["AAPL"]
        assert mgr.get_subscribed_symbols() == {"MSFT"}

    async def test_broadcast_to_symbol(self):
        mgr = ConnectionManager()
        ws1 = _FakeWebSocket()
        ws2 = _FakeWebSocket()
        sid1 = await mgr.connect(ws1, {"auth_type": "admin", "max_subscriptions": 50})
        sid2 = await mgr.connect(ws2, {"auth_type": "consumer", "max_subscriptions": 50})
        await mgr.subscribe_quotes(sid1, ["AAPL"])
        await mgr.subscribe_quotes(sid2, ["MSFT"])

        await mgr.broadcast_to_symbol("AAPL", {"type": "quote", "symbol": "AAPL", "price": 150})
        assert len(ws1.sent) == 1
        assert len(ws2.sent) == 0

    async def test_disconnect_cleans_up_subscriptions(self):
        mgr = ConnectionManager()
        ws = _FakeWebSocket()
        sid = await mgr.connect(ws, {"auth_type": "admin", "max_subscriptions": 50})
        await mgr.subscribe_quotes(sid, ["AAPL"])
        await mgr.disconnect(sid)
        assert mgr.get_subscribed_symbols() == set()

    async def test_send_to_session(self):
        mgr = ConnectionManager()
        ws = _FakeWebSocket()
        sid = await mgr.connect(ws, {"auth_type": "admin"})
        ok = await mgr.send_to_session(sid, {"type": "pong"})
        assert ok is True
        assert len(ws.sent) == 1

    async def test_get_stats(self):
        mgr = ConnectionManager()
        ws = _FakeWebSocket()
        sid = await mgr.connect(ws, {"auth_type": "admin", "max_subscriptions": 50})
        await mgr.subscribe_quotes(sid, ["AAPL", "MSFT"])
        stats = mgr.get_stats()
        assert stats["connections"] == 1
        assert stats["unique_symbols"] == 2
        assert stats["symbol_subscriptions"] == 2
