"""Finnhub WebSocket collector.

Connects to ``wss://ws.finnhub.io?token=<API_KEY>`` for real-time US trade data.

Finnhub protocol:
- Subscribe:   ``{"type":"subscribe","symbol":"AAPL"}``
- Unsubscribe: ``{"type":"unsubscribe","symbol":"AAPL"}``
- Trade data:  ``{"data":[{"p":150.0,"s":"AAPL","t":1234567890123,"v":100,"c":["@"]}],"type":"trade"}``
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


class FinnhubCollector(BaseUpstreamCollector):

    @property
    def name(self) -> str:
        return "finnhub"

    async def _get_ws_url(self) -> str:
        settings = get_settings()
        api_key = get_api_key("finnhub") or settings.FINNHUB_API_KEY
        if not api_key:
            raise ValueError("FINNHUB_API_KEY not configured")
        return f"{settings.FINNHUB_WS_URL}?token={api_key}"

    async def _on_connected(self) -> None:
        if self._symbols:
            await self._send_subscribe(list(self._symbols))

    async def _on_message(self, data: Any) -> None:
        if isinstance(data, bytes):
            msg = orjson.loads(data)
        else:
            msg = orjson.loads(data.encode("utf-8"))

        msg_type = msg.get("type")
        if msg_type != "trade":
            return

        trades = msg.get("data", [])
        for trade in trades:
            symbol = trade.get("s", "")
            price = trade.get("p", 0.0)
            volume = trade.get("v", 0)
            conditions = trade.get("c", [])

            event = make_trade(
                symbol=symbol,
                price=price,
                volume=volume,
                source="finnhub",
                conditions=conditions,
            )
            await publish_trade(event)

            # Update realtime cache
            try:
                from app.services.realtime_cache_service import update_quote_cache
                await update_quote_cache(symbol, price, volume, "finnhub")
            except ImportError:
                pass

    async def _send_subscribe(self, symbols: list[str]) -> None:
        if self._ws is None:
            return
        for sym in symbols:
            await self._ws.send(orjson.dumps({"type": "subscribe", "symbol": sym}))
        logger.info("finnhub: subscribed to %d symbols", len(symbols))

    async def _send_unsubscribe(self, symbols: list[str]) -> None:
        if self._ws is None:
            return
        for sym in symbols:
            await self._ws.send(orjson.dumps({"type": "unsubscribe", "symbol": sym}))
