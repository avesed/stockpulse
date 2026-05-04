"""Global rate-limited priority queue for external API providers.

Centralises all outbound calls to yfinance and Finnhub behind a token-bucket
rate limiter with priority-aware scheduling.  Higher-priority requests
(real-time quotes from WebStock) preempt lower-priority work (scheduled
collection, backfill) at the queue level.

Each provider gets its own :class:`ProviderQueue` instance.  Callers use the
module-level :func:`submit` helper:

    from app.core.provider_queue import submit, Priority

    result = await submit("finnhub", fetch_sync, symbol,
                          priority=Priority.FRONTEND, timeout=15.0)

The queue processor drains items in priority order (lower number = higher
priority).  When a REALTIME or FRONTEND request is enqueued while the
processor is busy with SCHEDULED/BACKFILL work, the high-priority item will
be served as soon as the current executor call returns — effectively
preempting background work.

Token bucket refill rates are dynamically adjusted when API keys are added
or removed via the admin UI (detected through ``api_keys`` reload callbacks).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import IntEnum
from functools import partial
from typing import Any, Callable, Optional, TypeVar

from app.core.executor import ExecutorPool, run_in_executor

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Priority levels (lower = higher priority)
# ---------------------------------------------------------------------------
class Priority(IntEnum):
    REALTIME = 0
    FRONTEND = 1
    SCHEDULED = 5
    BACKFILL = 10


# ---------------------------------------------------------------------------
# Token bucket
# ---------------------------------------------------------------------------
class _TokenBucket:
    """Async token-bucket rate limiter with dynamic rate adjustment."""

    __slots__ = ("_rate", "_capacity", "_tokens", "_last_refill", "_lock")

    def __init__(self, rate: float, capacity: int) -> None:
        self._rate = rate
        self._capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def update_rate(self, rate: float, capacity: int) -> None:
        self._rate = rate
        self._capacity = capacity

    async def acquire(self) -> None:
        """Block until a token is available."""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            await asyncio.sleep(0.05)

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now


# ---------------------------------------------------------------------------
# Queue item
# ---------------------------------------------------------------------------
_sequence = 0


def _next_seq() -> int:
    global _sequence
    _sequence += 1
    return _sequence


@dataclass(order=True)
class _QueueItem:
    priority: int
    seq: int = field(compare=True)
    future: asyncio.Future = field(compare=False)
    func: Callable = field(compare=False)
    args: tuple = field(compare=False)
    kwargs: dict = field(compare=False)
    timeout: float = field(compare=False)
    pool: ExecutorPool = field(compare=False)


# ---------------------------------------------------------------------------
# ProviderQueue
# ---------------------------------------------------------------------------
class ProviderQueue:
    """Rate-limited priority queue for a single provider."""

    def __init__(self, name: str, rate: float, capacity: int) -> None:
        self.name = name
        self._bucket = _TokenBucket(rate, capacity)
        self._queue: asyncio.PriorityQueue[_QueueItem] = asyncio.PriorityQueue()
        self._processor_task: Optional[asyncio.Task] = None
        self._stopping = False

        # Stats
        self._total_processed = 0
        self._total_throttled = 0
        self._high_priority_pending = 0

    # -- public api --

    async def submit(
        self,
        func: Callable[..., T],
        *args: Any,
        priority: int = Priority.SCHEDULED,
        timeout: float = 30.0,
        pool: ExecutorPool = ExecutorPool.BACKGROUND,
        **kwargs: Any,
    ) -> T:
        """Enqueue a sync function for rate-limited execution in the executor.

        Returns the result of ``func(*args, **kwargs)``.
        Raises whatever ``func`` raises, or ``asyncio.TimeoutError``.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future[T] = loop.create_future()

        item = _QueueItem(
            priority=priority,
            seq=_next_seq(),
            future=future,
            func=func,
            args=args,
            kwargs=kwargs,
            timeout=timeout,
            pool=pool,
        )

        if priority <= Priority.FRONTEND:
            self._high_priority_pending += 1

        await self._queue.put(item)
        return await future

    def has_high_priority_pending(self) -> bool:
        """True if REALTIME or FRONTEND items are queued."""
        return self._high_priority_pending > 0

    def update_rate(self, rate: float, capacity: int) -> None:
        """Adjust token bucket parameters (e.g. when keys added/removed)."""
        self._bucket.update_rate(rate, capacity)
        logger.info(
            "Queue[%s] rate updated: %.2f tokens/s, capacity=%d",
            self.name, rate, capacity,
        )

    def stats(self) -> dict:
        return {
            "provider": self.name,
            "queue_depth": self._queue.qsize(),
            "high_priority_pending": self._high_priority_pending,
            "total_processed": self._total_processed,
            "total_throttled": self._total_throttled,
        }

    # -- lifecycle --

    def start(self) -> None:
        if self._processor_task is None or self._processor_task.done():
            self._stopping = False
            self._processor_task = asyncio.create_task(
                self._process_loop(), name=f"queue-{self.name}"
            )
            logger.info("Queue[%s] processor started", self.name)

    async def stop(self) -> None:
        self._stopping = True
        if self._processor_task and not self._processor_task.done():
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        self._processor_task = None
        self._drain_remaining()
        logger.info("Queue[%s] processor stopped", self.name)

    def _drain_remaining(self) -> None:
        """Cancel all pending futures on shutdown."""
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                if not item.future.done():
                    item.future.cancel()
            except asyncio.QueueEmpty:
                break

    # -- processor loop --

    async def _process_loop(self) -> None:
        """Main processing loop: dequeue → acquire token → execute."""
        while not self._stopping:
            try:
                item = await self._queue.get()
            except asyncio.CancelledError:
                return

            if item.future.done():
                # Caller timed out or cancelled
                if item.priority <= Priority.FRONTEND:
                    self._high_priority_pending = max(0, self._high_priority_pending - 1)
                continue

            try:
                await self._bucket.acquire()
                self._total_throttled += 1

                result = await run_in_executor(
                    item.func, *item.args,
                    timeout=item.timeout,
                    pool=item.pool,
                    **item.kwargs,
                )

                if not item.future.done():
                    item.future.set_result(result)

            except asyncio.CancelledError:
                if not item.future.done():
                    item.future.cancel()
                return
            except Exception as exc:
                if not item.future.done():
                    item.future.set_exception(exc)
            finally:
                if item.priority <= Priority.FRONTEND:
                    self._high_priority_pending = max(0, self._high_priority_pending - 1)
                self._total_processed += 1


# ---------------------------------------------------------------------------
# Module-level singletons
# ---------------------------------------------------------------------------
_queues: dict[str, ProviderQueue] = {}

# Per-key rate limits (requests/minute) — same source of truth as api_keys.py
_PER_KEY_RPM: dict[str, int] = {
    "finnhub": 58,   # Finnhub free tier: 60/min, use 58 safety margin
}

# Providers without API keys — fixed rate limits
_FIXED_RPM: dict[str, int] = {
    "yfinance": 600,  # ~10/s — tested safe for quoteSummary/timeseries endpoints
}


def _calc_rate(name: str) -> tuple[float, int]:
    """Calculate (tokens_per_second, burst_capacity) for a provider."""
    from app.core.api_keys import get_key_pool_size

    if name in _PER_KEY_RPM:
        key_count = max(1, get_key_pool_size(name))
        rpm = _PER_KEY_RPM[name] * key_count
        rate = rpm / 60.0
        capacity = key_count * 5
        return rate, capacity

    if name in _FIXED_RPM:
        rate = _FIXED_RPM[name] / 60.0
        return rate, 10

    return 1.0, 5


def _get_or_create(name: str) -> ProviderQueue:
    if name not in _queues:
        rate, capacity = _calc_rate(name)
        _queues[name] = ProviderQueue(name, rate, capacity)
    return _queues[name]


def _recalc_rates() -> None:
    """Recalculate all provider rates based on current key counts."""
    for name, q in _queues.items():
        rate, capacity = _calc_rate(name)
        q.update_rate(rate, capacity)


def _on_keys_reloaded() -> None:
    """Callback fired when API keys are reloaded from DB."""
    _recalc_rates()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def submit(
    provider: str,
    func: Callable[..., T],
    *args: Any,
    priority: int = Priority.SCHEDULED,
    timeout: float = 30.0,
    pool: ExecutorPool = ExecutorPool.BACKGROUND,
    **kwargs: Any,
) -> T:
    """Submit a sync function to the provider's rate-limited queue.

    Args:
        provider: "finnhub" or "yfinance".
        func: Synchronous callable to execute in the thread pool.
        *args: Positional arguments for func.
        priority: Priority level (lower = higher priority).
        timeout: Executor timeout in seconds.
        pool: Which executor pool to use.
        **kwargs: Keyword arguments for func.

    Returns:
        The result of ``func(*args, **kwargs)``.
    """
    q = _get_or_create(provider)
    return await q.submit(
        func, *args,
        priority=priority,
        timeout=timeout,
        pool=pool,
        **kwargs,
    )


def has_high_priority_pending(provider: str) -> bool:
    """Check if the provider queue has REALTIME/FRONTEND requests waiting.

    Collection services call this between batches to yield to high-priority
    work.
    """
    q = _queues.get(provider)
    return q.has_high_priority_pending() if q else False


def get_queue_stats() -> dict:
    """Return stats for all active provider queues."""
    return {name: q.stats() for name, q in _queues.items()}


async def start_queues() -> None:
    """Initialise and start all provider queues.  Call from app lifespan."""
    from app.core.api_keys import register_reload_callback

    register_reload_callback(_on_keys_reloaded)

    for name in list(_PER_KEY_RPM) + list(_FIXED_RPM):
        q = _get_or_create(name)
        q.start()

    _recalc_rates()
    logger.info("Provider queues started: %s", list(_queues.keys()))


async def stop_queues() -> None:
    """Stop all provider queues.  Call from app lifespan shutdown."""
    from app.core.api_keys import unregister_reload_callback

    unregister_reload_callback(_on_keys_reloaded)

    for q in _queues.values():
        await q.stop()
    logger.info("Provider queues stopped")
