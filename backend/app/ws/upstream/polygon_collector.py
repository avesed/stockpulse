"""Polygon.io WebSocket collector.

Connects to ``wss://socket.polygon.io/stocks`` for real-time US trade data.

Polygon protocol:
- Auth:        ``{"action":"auth","params":"<API_KEY>"}``
- Subscribe:   ``{"action":"subscribe","params":"T.AAPL,T.MSFT"}``  (T. for trades)
- Unsubscribe: ``{"action":"unsubscribe","params":"T.AAPL"}``
- Trade data:  ``[{"ev":"T","sym":"AAPL","p":150.0,"s":100,"t":1234567890123}]``
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from app.config import get_settings
from app.core.api_keys import get_api_key
from app.ws.protocol import make_trade
from app.ws.redis_fanout import publish_trade
from app.ws.upstream.base import BaseUpstreamCollector

logger = logging.getLogger(__name__)


class PolygonCollector(BaseUpstreamCollector):

    @property
    def name(self) -> str:
        return "polygon"

    async def _get_ws_url(self) -> str:
        settings = get_settings()
        return settings.POLYGON_WS_URL

    async def _on_connected(self) -> None:
        # Authenticate first
        api_key = get_api_key("polygon") or get_settings().POLYGON_API_KEY
        if not api_key:
            raise ValueError("POLYGON_API_KEY not configured")

        await self._ws.send(orjson.dumps({"action": "auth", "params": api_key}))
        logger.info("polygon: auth message sent")

        # Subscribe after auth
        if self._symbols:
            await self._send_subscribe(list(self._symbols))

    async def _on_message(self, data: Any) -> None:
        if isinstance(data, bytes):
            messages = orjson.loads(data)
        else:
            messages = orjson.loads(data.encode("utf-8"))

        # Polygon sends arrays of events
        if not isinstance(messages, list):
            messages = [messages]

        for msg in messages:
            ev = msg.get("ev")

            # Handle status messages (auth confirmation, etc.)
            if ev == "status":
                status = msg.get("status")
                message = msg.get("message", "")
                logger.info("polygon status: %s - %s", status, message)
                continue

            # Trade events
            if ev == "T":
                symbol = msg.get("sym", "")
                price = msg.get("p", 0.0)
                volume = msg.get("s", 0)

                event = make_trade(
                    symbol=symbol,
                    price=price,
                    volume=volume,
                    source="polygon",
                )
                await publish_trade(event)

                try:
                    from app.services.realtime_cache_service import update_quote_cache
                    await update_quote_cache(symbol, price, volume, "polygon")
                except ImportError:
                    pass

    async def _send_subscribe(self, symbols: list[str]) -> None:
        if self._ws is None:
            return
        params = ",".join(f"T.{sym}" for sym in symbols)
        await self._ws.send(orjson.dumps({"action": "subscribe", "params": params}))
        logger.info("polygon: subscribed to %d symbols", len(symbols))

    async def _send_unsubscribe(self, symbols: list[str]) -> None:
        if self._ws is None:
            return
        params = ",".join(f"T.{sym}" for sym in symbols)
        await self._ws.send(orjson.dumps({"action": "unsubscribe", "params": params}))
