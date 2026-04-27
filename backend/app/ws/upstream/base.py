"""Abstract base for upstream WebSocket collectors.

Provides reconnection with exponential backoff, heartbeat, and message rate tracking.
"""
from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

from app.config import get_settings

logger = logging.getLogger(__name__)


class BaseUpstreamCollector(ABC):
    """Base class for external WebSocket data source collectors."""

    def __init__(self) -> None:
        self._ws = None
        self._connected = False
        self._stop_event = asyncio.Event()
        self._symbols: set[str] = set()
        self._message_count: int = 0
        self._last_message_at: float = 0.0
        self._reconnect_delay: float = 1.0
        self._task: asyncio.Task | None = None

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name (e.g. 'finnhub', 'polygon')."""

    @abstractmethod
    async def _get_ws_url(self) -> str:
        """Return the full WebSocket URL including auth params."""

    @abstractmethod
    async def _on_connected(self) -> None:
        """Called after WebSocket connection established. Subscribe to symbols here."""

    @abstractmethod
    async def _on_message(self, data: Any) -> None:
        """Process a single incoming WebSocket message."""

    @abstractmethod
    async def _send_subscribe(self, symbols: list[str]) -> None:
        """Send subscribe command for symbols to the upstream WS."""

    @abstractmethod
    async def _send_unsubscribe(self, symbols: list[str]) -> None:
        """Send unsubscribe command for symbols to the upstream WS."""

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def symbols(self) -> set[str]:
        return self._symbols.copy()

    def get_status(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "status": "connected" if self._connected else "disconnected",
            "symbols_count": len(self._symbols),
            "message_count": self._message_count,
            "last_message_at": (
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self._last_message_at))
                if self._last_message_at > 0 else None
            ),
        }

    async def start(self, symbols: list[str] | None = None) -> None:
        """Start the collector in a background task."""
        if symbols:
            self._symbols = set(s.upper() for s in symbols)
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        logger.info("%s collector started with %d symbols", self.name, len(self._symbols))

    async def stop(self) -> None:
        """Gracefully stop the collector."""
        self._stop_event.set()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._connected = False
        logger.info("%s collector stopped", self.name)

    async def subscribe_symbols(self, symbols: list[str]) -> None:
        """Add symbols to subscription (live)."""
        new_symbols = [s.upper() for s in symbols if s.upper() not in self._symbols]
        if not new_symbols:
            return
        self._symbols.update(new_symbols)
        if self._connected and self._ws is not None:
            await self._send_subscribe(new_symbols)
        logger.info("%s: subscribed to %d new symbols (total: %d)",
                    self.name, len(new_symbols), len(self._symbols))

    async def unsubscribe_symbols(self, symbols: list[str]) -> None:
        """Remove symbols from subscription (live)."""
        to_remove = [s.upper() for s in symbols if s.upper() in self._symbols]
        if not to_remove:
            return
        self._symbols -= set(to_remove)
        if self._connected and self._ws is not None:
            await self._send_unsubscribe(to_remove)

    async def _run_loop(self) -> None:
        """Main connection loop with exponential backoff reconnection."""
        import websockets

        settings = get_settings()
        max_delay = settings.WS_UPSTREAM_RECONNECT_MAX_DELAY

        while not self._stop_event.is_set():
            try:
                url = await self._get_ws_url()
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    self._reconnect_delay = 1.0
                    logger.info("%s: WebSocket connected", self.name)

                    # Publish connected status
                    from app.ws.redis_fanout import publish_collector_status
                    from app.ws.protocol import make_collector_status
                    await publish_collector_status(
                        make_collector_status(self.name, "connected", len(self._symbols))
                    )

                    await self._on_connected()

                    async for message in ws:
                        if self._stop_event.is_set():
                            break
                        self._message_count += 1
                        self._last_message_at = time.time()
                        try:
                            await self._on_message(message)
                        except Exception:
                            logger.debug("%s: message handler error", self.name, exc_info=True)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                self._ws = None
                logger.warning(
                    "%s: WebSocket disconnected (%s), reconnecting in %.0fs...",
                    self.name, e, self._reconnect_delay,
                )

                # Publish disconnected status
                try:
                    from app.ws.redis_fanout import publish_collector_status
                    from app.ws.protocol import make_collector_status
                    await publish_collector_status(
                        make_collector_status(self.name, "disconnected", len(self._symbols), error=str(e))
                    )
                except Exception:
                    pass

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self._reconnect_delay,
                    )
                    break
                except asyncio.TimeoutError:
                    pass

                self._reconnect_delay = min(self._reconnect_delay * 2, max_delay)

        self._connected = False
        self._ws = None
