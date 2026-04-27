"""YFinance WebSocket collector.

Connects directly to ``wss://streamer.finance.yahoo.com/?version=2``
using the ``websockets`` library (same as Finnhub/Polygon collectors)
for proper async lifecycle control.

Uses yfinance's protobuf decoder to parse PricingData messages.

Free, no API key required. Data fields:
  id, price, time, day_volume, change, change_percent, open_price,
  previous_close, day_high, day_low, bid, ask, short_name, etc.

Yahoo Finance WS protocol:
- Subscribe:   ``{"subscribe": ["AAPL", "MSFT"]}``
- Unsubscribe: ``{"unsubscribe": ["AAPL"]}``
- Incoming:    ``{"message": "<base64-encoded-protobuf>"}``
"""
from __future__ import annotations

import base64
import json
import logging
from typing import Any

import orjson

from app.ws.protocol import make_quote, make_trade
from app.ws.redis_fanout import publish_quote, publish_trade
from app.ws.upstream.base import BaseUpstreamCollector

logger = logging.getLogger(__name__)


def _decode_pricing_message(base64_message: str) -> dict[str, Any] | None:
    """Decode a base64-encoded protobuf PricingData message to dict."""
    try:
        from yfinance.pricing_pb2 import PricingData
        from google.protobuf.json_format import MessageToDict

        decoded_bytes = base64.b64decode(base64_message)
        pricing_data = PricingData()
        pricing_data.ParseFromString(decoded_bytes)
        return MessageToDict(pricing_data, preserving_proto_field_name=True)
    except Exception as e:
        logger.debug("Failed to decode yfinance message: %s", e)
        return None


class YFinanceCollector(BaseUpstreamCollector):

    @property
    def name(self) -> str:
        return "yfinance"

    async def _get_ws_url(self) -> str:
        return "wss://streamer.finance.yahoo.com/?version=2"

    async def _on_connected(self) -> None:
        if self._symbols:
            await self._send_subscribe(list(self._symbols))

    async def _on_message(self, data: Any) -> None:
        """Process a raw Yahoo Finance WebSocket message."""
        if isinstance(data, bytes):
            msg = orjson.loads(data)
        else:
            msg = orjson.loads(data.encode("utf-8"))

        encoded = msg.get("message", "")
        if not encoded:
            return

        decoded = _decode_pricing_message(encoded)
        if decoded is None:
            return

        symbol = decoded.get("id", "")
        if not symbol:
            return

        price = decoded.get("price", 0.0)
        volume = int(decoded.get("day_volume", 0))
        change = decoded.get("change", 0.0)
        change_pct = decoded.get("change_percent", 0.0)

        trade_event = make_trade(
            symbol=symbol,
            price=price,
            volume=int(decoded.get("last_size", 0)) or volume,
            source="yfinance",
        )
        await publish_trade(trade_event)

        quote_event = make_quote(
            symbol=symbol,
            price=price,
            volume=volume,
            change=change,
            change_pct=change_pct,
            source="yfinance",
        )
        await publish_quote(quote_event)

        try:
            from app.services.realtime_cache_service import update_quote_cache
            await update_quote_cache(symbol, price, volume, "yfinance")
        except Exception:
            pass

    async def _send_subscribe(self, symbols: list[str]) -> None:
        if self._ws is None:
            return
        await self._ws.send(json.dumps({"subscribe": symbols}))
        logger.info("yfinance: subscribed to %d symbols", len(symbols))

    async def _send_unsubscribe(self, symbols: list[str]) -> None:
        if self._ws is None:
            return
        await self._ws.send(json.dumps({"unsubscribe": symbols}))
