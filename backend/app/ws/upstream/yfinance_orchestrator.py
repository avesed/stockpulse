"""Multi-shard YFinance WebSocket orchestrator.

Yahoo WS limits each connection to ~100 symbol subscriptions.
This orchestrator manages N ``YFinanceCollector`` instances (shards),
auto-scaling based on symbol count (80 per shard for safety margin).

Exposes the same duck-type interface as ``BaseUpstreamCollector`` so that
``collector_service.py`` can treat it as a drop-in replacement.
"""
from __future__ import annotations

import asyncio
import logging
import time
from math import ceil
from typing import Any

from app.ws.upstream.yfinance_collector import YFinanceCollector

logger = logging.getLogger(__name__)

SYMBOLS_PER_SHARD = 80


class _ShardedYFinanceCollector(YFinanceCollector):
    """YFinanceCollector with a shard id for logging."""

    def __init__(self, shard_id: int) -> None:
        super().__init__()
        self._shard_id = shard_id

    @property
    def name(self) -> str:
        return f"yfinance-{self._shard_id}"


class YFinanceOrchestrator:
    """Orchestrates multiple YFinanceCollector shards for large symbol sets."""

    def __init__(self) -> None:
        self._shards: list[_ShardedYFinanceCollector] = []
        self._all_symbols: set[str] = set()
        self._symbol_map: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._started = False

    @property
    def is_connected(self) -> bool:
        return any(s.is_connected for s in self._shards)

    @property
    def symbols(self) -> set[str]:
        return self._all_symbols.copy()

    async def start(self, symbols: list[str] | None = None) -> None:
        if symbols:
            self._all_symbols = set(s.upper() for s in symbols)

        await self._build_shards(self._all_symbols)
        self._started = True

        logger.info(
            "YFinanceOrchestrator started: %d shards, %d symbols (capacity %d)",
            len(self._shards), len(self._all_symbols),
            len(self._shards) * SYMBOLS_PER_SHARD,
        )

    async def stop(self) -> None:
        for s in self._shards:
            try:
                await s.stop()
            except Exception:
                logger.debug("Error stopping %s", s.name, exc_info=True)
        self._shards.clear()
        self._symbol_map.clear()
        self._started = False
        logger.info("YFinanceOrchestrator stopped")

    async def subscribe_symbols(self, symbols: list[str]) -> None:
        async with self._lock:
            new_symbols = [s.upper() for s in symbols if s.upper() not in self._all_symbols]
            if not new_symbols:
                return

            # Group by target shard, then batch-subscribe
            shard_batches: dict[int, list[str]] = {}
            for sym in new_symbols:
                idx = self._find_least_loaded()
                if idx is None:
                    await self._add_shard()
                    idx = len(self._shards) - 1
                self._all_symbols.add(sym)
                self._symbol_map[sym] = idx
                shard_batches.setdefault(idx, []).append(sym)

            for idx, batch in shard_batches.items():
                await self._shards[idx].subscribe_symbols(batch)

            logger.info(
                "YFinanceOrchestrator: subscribed %d new symbols (total: %d, shards: %d)",
                len(new_symbols), len(self._all_symbols), len(self._shards),
            )

    async def unsubscribe_symbols(self, symbols: list[str]) -> None:
        async with self._lock:
            for sym in symbols:
                sym = sym.upper()
                idx = self._symbol_map.pop(sym, None)
                self._all_symbols.discard(sym)
                if idx is not None and idx < len(self._shards):
                    await self._shards[idx].unsubscribe_symbols([sym])

            await self._shrink_shards()

    def get_status(self) -> dict[str, Any]:
        per_shard = [s.get_status() for s in self._shards]
        total_msgs = sum(s._message_count for s in self._shards)
        last_msgs = [s._last_message_at for s in self._shards if s._last_message_at > 0]

        return {
            "provider": "yfinance",
            "status": "connected" if self.is_connected else (
                "disconnected" if self._started else "stopped"
            ),
            "shards": len(self._shards),
            "symbols_per_shard": SYMBOLS_PER_SHARD,
            "symbols_count": len(self._all_symbols),
            "symbols_capacity": len(self._shards) * SYMBOLS_PER_SHARD,
            "message_count": total_msgs,
            "last_message_at": (
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(max(last_msgs)))
                if last_msgs else None
            ),
            "per_shard": per_shard,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _build_shards(self, symbols: set[str]) -> None:
        sorted_syms = sorted(symbols)
        n_shards = max(1, ceil(len(sorted_syms) / SYMBOLS_PER_SHARD))

        for i in range(n_shards):
            chunk = sorted_syms[i * SYMBOLS_PER_SHARD : (i + 1) * SYMBOLS_PER_SHARD]
            shard = _ShardedYFinanceCollector(shard_id=i)
            self._shards.append(shard)
            await shard.start(chunk)
            for sym in chunk:
                self._symbol_map[sym] = i

    async def _add_shard(self) -> None:
        shard = _ShardedYFinanceCollector(shard_id=len(self._shards))
        self._shards.append(shard)
        await shard.start()
        logger.info("YFinanceOrchestrator: added shard %d", shard._shard_id)

    async def _shrink_shards(self) -> None:
        """Remove trailing empty shards."""
        while len(self._shards) > 1:
            last = self._shards[-1]
            if len(last.symbols) == 0:
                await last.stop()
                self._shards.pop()
                logger.info("YFinanceOrchestrator: removed empty shard %d", last._shard_id)
            else:
                break

    def _find_least_loaded(self) -> int | None:
        best_idx = None
        best_count = SYMBOLS_PER_SHARD + 1
        for i, s in enumerate(self._shards):
            count = len(s.symbols)
            if count < SYMBOLS_PER_SHARD and count < best_count:
                best_count = count
                best_idx = i
        return best_idx
