"""Tests for provider_queue — Priority ordering, TokenBucket, ProviderQueue stats."""
from __future__ import annotations

import asyncio
import time

import pytest

from app.core.provider_queue import Priority, _TokenBucket, ProviderQueue


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------

class TestPriority:
    def test_ordering(self):
        assert Priority.REALTIME < Priority.FRONTEND
        assert Priority.FRONTEND < Priority.SCHEDULED
        assert Priority.SCHEDULED < Priority.BACKFILL

    def test_values(self):
        assert Priority.REALTIME == 0
        assert Priority.FRONTEND == 1
        assert Priority.SCHEDULED == 5
        assert Priority.BACKFILL == 10

    def test_sortable(self):
        items = [Priority.BACKFILL, Priority.REALTIME, Priority.SCHEDULED, Priority.FRONTEND]
        assert sorted(items) == [Priority.REALTIME, Priority.FRONTEND, Priority.SCHEDULED, Priority.BACKFILL]


# ---------------------------------------------------------------------------
# TokenBucket
# ---------------------------------------------------------------------------

class TestTokenBucket:
    async def test_acquire_consumes_token(self):
        bucket = _TokenBucket(rate=10.0, capacity=5)
        await bucket.acquire()
        assert bucket._tokens < 5.0

    async def test_acquire_exhausts_then_refills(self):
        bucket = _TokenBucket(rate=100.0, capacity=2)
        await bucket.acquire()
        await bucket.acquire()
        assert bucket._tokens < 1.0
        await asyncio.sleep(0.05)
        await bucket.acquire()

    async def test_capacity_limits_burst(self):
        bucket = _TokenBucket(rate=1000.0, capacity=3)
        await asyncio.sleep(0.1)
        bucket._refill()
        assert bucket._tokens <= 3.0

    def test_update_rate(self):
        bucket = _TokenBucket(rate=10.0, capacity=5)
        bucket.update_rate(20.0, 10)
        assert bucket._rate == 20.0
        assert bucket._capacity == 10


# ---------------------------------------------------------------------------
# ProviderQueue
# ---------------------------------------------------------------------------

class TestProviderQueue:
    def test_stats_initial(self):
        q = ProviderQueue("test", rate=10.0, capacity=5)
        stats = q.stats()
        assert stats["provider"] == "test"
        assert stats["queue_depth"] == 0
        assert stats["total_processed"] == 0
        assert stats["in_flight"] == 0

    def test_has_high_priority_pending_initially_false(self):
        q = ProviderQueue("test", rate=10.0, capacity=5)
        assert q.has_high_priority_pending() is False

    def test_update_rate(self):
        q = ProviderQueue("test", rate=10.0, capacity=5)
        q.update_rate(20.0, 10)
        assert q._bucket._rate == 20.0

    async def test_stop_cancels_pending(self):
        q = ProviderQueue("test", rate=0.01, capacity=1)
        q.start()

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        from app.core.provider_queue import _QueueItem, _next_seq
        from app.core.executor import ExecutorPool
        item = _QueueItem(
            priority=Priority.SCHEDULED,
            seq=_next_seq(),
            future=future,
            func=lambda: None,
            args=(),
            kwargs={},
            timeout=5.0,
            pool=ExecutorPool.BACKGROUND,
        )
        await q._queue.put(item)
        await q.stop()
        assert future.done()
