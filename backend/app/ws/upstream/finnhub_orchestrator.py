"""Multi-key Finnhub WebSocket orchestrator.

Manages N ``FinnhubCollector`` instances (one per API key), with automatic
symbol sharding across them.  Finnhub free tier limits each WS connection
to 50 symbol subscriptions, so multiple keys allow more concurrent symbols.

Exposes the same duck-type interface as ``BaseUpstreamCollector`` so that
``collector_service.py`` can treat it as a drop-in replacement.
"""
from __future__ import annotations

import asyncio
import logging
import time
from math import ceil
from typing import Any

from app.core.api_keys import (
    get_api_keys,
    register_reload_callback,
    unregister_reload_callback,
)
from app.ws.upstream.finnhub_collector import FinnhubCollector

logger = logging.getLogger(__name__)

SYMBOLS_PER_KEY = 50  # Finnhub free tier limit


class FinnhubOrchestrator:
    """Orchestrates multiple FinnhubCollector instances for multi-key operation."""

    def __init__(self) -> None:
        self._collectors: list[FinnhubCollector] = []
        self._keys: list[str] = []
        self._all_symbols: set[str] = set()
        # symbol -> collector index
        self._symbol_map: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._started = False

    @property
    def is_connected(self) -> bool:
        return any(c.is_connected for c in self._collectors)

    @property
    def symbols(self) -> set[str]:
        return self._all_symbols.copy()

    async def start(self, symbols: list[str] | None = None) -> None:
        """Start collectors, one per API key, sharding symbols across them."""
        self._keys = get_api_keys("finnhub")
        if not self._keys:
            logger.warning("No Finnhub API keys configured")
            return

        if symbols:
            self._all_symbols = set(s.upper() for s in symbols)

        shards = self._shard_symbols(self._all_symbols, self._keys)

        for i, (key, shard_symbols) in enumerate(shards):
            collector = FinnhubCollector(api_key=key, instance_id=i)
            self._collectors.append(collector)
            await collector.start(list(shard_symbols))
            for sym in shard_symbols:
                self._symbol_map[sym] = i

        self._started = True
        register_reload_callback(self._on_keys_changed)

        logger.info(
            "FinnhubOrchestrator started: %d keys, %d symbols, capacity %d",
            len(self._keys), len(self._all_symbols), len(self._keys) * SYMBOLS_PER_KEY,
        )

    async def stop(self) -> None:
        """Stop all collector instances."""
        unregister_reload_callback(self._on_keys_changed)
        for c in self._collectors:
            try:
                await c.stop()
            except Exception:
                logger.debug("Error stopping %s", c.name, exc_info=True)
        self._collectors.clear()
        self._symbol_map.clear()
        self._started = False
        logger.info("FinnhubOrchestrator stopped")

    async def subscribe_symbols(self, symbols: list[str]) -> None:
        """Add symbols, assigning each to the least-loaded collector."""
        async with self._lock:
            new_symbols = [s.upper() for s in symbols if s.upper() not in self._all_symbols]
            if not new_symbols:
                return

            for sym in new_symbols:
                idx = self._find_least_loaded()
                if idx is None:
                    logger.warning(
                        "All Finnhub keys at capacity (%d symbols each), "
                        "cannot subscribe %s (and %d more)",
                        SYMBOLS_PER_KEY, sym, len(new_symbols) - new_symbols.index(sym) - 1,
                    )
                    break
                self._all_symbols.add(sym)
                self._symbol_map[sym] = idx
                await self._collectors[idx].subscribe_symbols([sym])

    async def unsubscribe_symbols(self, symbols: list[str]) -> None:
        """Remove symbols from their assigned collectors."""
        async with self._lock:
            for sym in symbols:
                sym = sym.upper()
                idx = self._symbol_map.pop(sym, None)
                self._all_symbols.discard(sym)
                if idx is not None and idx < len(self._collectors):
                    await self._collectors[idx].unsubscribe_symbols([sym])

    def get_status(self) -> dict[str, Any]:
        """Return aggregate status with per-instance details."""
        per_instance = [c.get_status() for c in self._collectors]
        total_msgs = sum(c._message_count for c in self._collectors)
        last_msgs = [c._last_message_at for c in self._collectors if c._last_message_at > 0]

        return {
            "provider": "finnhub",
            "status": "connected" if self.is_connected else ("disconnected" if self._started else "stopped"),
            "instances": len(self._collectors),
            "keys_count": len(self._keys),
            "symbols_count": len(self._all_symbols),
            "symbols_capacity": len(self._keys) * SYMBOLS_PER_KEY,
            "message_count": total_msgs,
            "last_message_at": (
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(max(last_msgs)))
                if last_msgs else None
            ),
            "per_instance": per_instance,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _find_least_loaded(self) -> int | None:
        """Return the index of the collector with the fewest symbols, or None if all full."""
        best_idx = None
        best_count = SYMBOLS_PER_KEY + 1
        for i, c in enumerate(self._collectors):
            count = len(c.symbols)
            if count < SYMBOLS_PER_KEY and count < best_count:
                best_count = count
                best_idx = i
        return best_idx

    def _shard_symbols(
        self, symbols: set[str], keys: list[str]
    ) -> list[tuple[str, list[str]]]:
        """Distribute symbols evenly across keys, respecting SYMBOLS_PER_KEY cap."""
        if not keys:
            return []
        sorted_symbols = sorted(symbols)
        n_keys = len(keys)
        result: list[tuple[str, list[str]]] = []

        if not sorted_symbols:
            # No symbols yet — still create collectors for each key
            for key in keys:
                result.append((key, []))
            return result

        chunk_size = min(SYMBOLS_PER_KEY, ceil(len(sorted_symbols) / n_keys))
        idx = 0
        for key in keys:
            chunk = sorted_symbols[idx : idx + chunk_size]
            result.append((key, chunk))
            idx += chunk_size
            if idx >= len(sorted_symbols):
                break

        # Remaining keys with no symbols
        for key in keys[len(result):]:
            result.append((key, []))

        return result

    async def _on_keys_changed(self) -> None:
        """Callback when API keys are reloaded from DB.  Adjusts collectors."""
        if not self._started:
            return

        new_keys = get_api_keys("finnhub")
        if new_keys == self._keys:
            return

        logger.info(
            "Finnhub key pool changed: %d -> %d keys, resharding...",
            len(self._keys), len(new_keys),
        )

        async with self._lock:
            # Stop all existing collectors
            for c in self._collectors:
                try:
                    await c.stop()
                except Exception:
                    pass
            self._collectors.clear()
            self._symbol_map.clear()

            # Rebuild with new keys
            self._keys = new_keys
            if not self._keys:
                logger.warning("All Finnhub keys removed, no collectors running")
                return

            shards = self._shard_symbols(self._all_symbols, self._keys)
            for i, (key, shard_symbols) in enumerate(shards):
                collector = FinnhubCollector(api_key=key, instance_id=i)
                self._collectors.append(collector)
                await collector.start(list(shard_symbols))
                for sym in shard_symbols:
                    self._symbol_map[sym] = i

            logger.info(
                "Finnhub resharding complete: %d keys, %d symbols",
                len(self._keys), len(self._all_symbols),
            )
