"""ThreadPoolExecutor for bridging synchronous data provider libraries to async.

Many data provider libraries (yfinance, akshare, tushare, finnhub) are synchronous.
This executor runs them in a thread pool so they don't block the FastAPI event loop.

Three isolated pools prevent background collection tasks from starving frontend
requests:
  - FRONTEND: User-facing API calls (stock quotes, search, history)
  - BACKGROUND: Daily bar collection + stock list updates
  - PROFILE: Stock profile collection (knowledge base)

Self-healing: A background watchdog periodically probes each pool and recycles
it when threads are stuck. This prevents gradual thread-pool exhaustion caused by
zombie threads (blocking calls that outlive their asyncio timeout).
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from functools import partial
from typing import Any, Callable, Optional, TypeVar

from app.config import get_settings

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ExecutorPool(Enum):
    FRONTEND = "frontend"
    BACKGROUND = "background"
    PROFILE = "profile"


_executors: dict[ExecutorPool, Optional[ThreadPoolExecutor]] = {
    ExecutorPool.FRONTEND: None,
    ExecutorPool.BACKGROUND: None,
    ExecutorPool.PROFILE: None,
}
_executor_lock = threading.Lock()
_executor_created_at: dict[ExecutorPool, float] = {
    ExecutorPool.FRONTEND: 0.0,
    ExecutorPool.BACKGROUND: 0.0,
    ExecutorPool.PROFILE: 0.0,
}

# Pool configuration: (settings attribute, thread name prefix)
_POOL_CONFIG: dict[ExecutorPool, tuple[str, str]] = {
    ExecutorPool.FRONTEND: ("EXECUTOR_MAX_WORKERS", "data-provider"),
    ExecutorPool.BACKGROUND: ("EXECUTOR_BACKGROUND_WORKERS", "bg-collect"),
    ExecutorPool.PROFILE: ("EXECUTOR_PROFILE_WORKERS", "bg-profile"),
}

# How often to recycle the executor (seconds). Zombie threads from timed-out
# calls accumulate over time; recycling keeps the pool fresh.
RECYCLE_INTERVAL = 4 * 3600  # 4 hours

# Maximum seconds to wait for the executor health probe.
HEALTH_PROBE_TIMEOUT = 5.0

# Watchdog check interval (seconds)
WATCHDOG_INTERVAL = 120  # 2 minutes

_watchdog_task: Optional[asyncio.Task] = None


def get_executor(pool: ExecutorPool = ExecutorPool.FRONTEND) -> ThreadPoolExecutor:
    """Get or create the shared ThreadPoolExecutor for the given pool."""
    global _executors, _executor_created_at
    with _executor_lock:
        if _executors[pool] is None:
            settings = get_settings()
            attr_name, prefix = _POOL_CONFIG[pool]
            max_workers = getattr(settings, attr_name)
            _executors[pool] = ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix=prefix,
            )
            _executor_created_at[pool] = time.monotonic()
            logger.info(
                "ThreadPoolExecutor[%s] initialized: max_workers=%d",
                pool.value,
                max_workers,
            )
    return _executors[pool]  # type: ignore[return-value]


def _recycle_executor(pool: ExecutorPool, reason: str) -> ThreadPoolExecutor:
    """Shut down the old executor and create a fresh one.

    The old executor is told to shut down with cancel_futures=True, but
    zombie threads may linger (Python limitation). The important thing is
    that new tasks go to a healthy pool.
    """
    global _executors, _executor_created_at
    with _executor_lock:
        old = _executors[pool]
        settings = get_settings()
        attr_name, prefix = _POOL_CONFIG[pool]
        max_workers = getattr(settings, attr_name)
        _executors[pool] = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=prefix,
        )
        _executor_created_at[pool] = time.monotonic()
        logger.warning(
            "Executor[%s] recycled (%s): new pool max_workers=%d",
            pool.value,
            reason,
            max_workers,
        )
    # Shut down old pool in background -- stuck threads won't block us
    if old is not None:
        try:
            old.shutdown(wait=False, cancel_futures=True)
        except Exception as e:
            logger.debug("Old executor[%s] shutdown error (non-fatal): %s", pool.value, e)
    return _executors[pool]  # type: ignore[return-value]


async def check_executor_health(
    pool: ExecutorPool = ExecutorPool.FRONTEND,
) -> bool:
    """Submit a trivial task to the executor and check it completes.

    Returns True if the executor is responsive, False if stuck.
    """
    loop = asyncio.get_running_loop()
    executor = get_executor(pool)

    def _probe() -> bool:
        return True

    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(executor, _probe),
            timeout=HEALTH_PROBE_TIMEOUT,
        )
        return result is True
    except (asyncio.TimeoutError, Exception):
        return False


async def _watchdog_loop() -> None:
    """Background coroutine: periodically checks executor health and age.

    - Recycles on schedule (RECYCLE_INTERVAL) to prevent gradual degradation.
    - Recycles immediately if health probe fails (all threads stuck).
    - Iterates over all pool types each cycle.
    """
    logger.info(
        "Executor watchdog started: probe every %ds, recycle every %ds",
        WATCHDOG_INTERVAL,
        RECYCLE_INTERVAL,
    )
    while True:
        try:
            await asyncio.sleep(WATCHDOG_INTERVAL)

            for pool in ExecutorPool:
                # Only check pools that have been created
                if _executors[pool] is None:
                    continue

                # Check age-based recycling
                age = time.monotonic() - _executor_created_at[pool]
                if age >= RECYCLE_INTERVAL:
                    _recycle_executor(
                        pool, f"age={int(age)}s >= {RECYCLE_INTERVAL}s"
                    )
                    continue

                # Health probe
                healthy = await check_executor_health(pool)
                if not healthy:
                    logger.error(
                        "Executor[%s] health probe FAILED — all threads may be stuck",
                        pool.value,
                    )
                    _recycle_executor(pool, "health probe failed")
        except asyncio.CancelledError:
            logger.info("Executor watchdog cancelled")
            return
        except Exception as e:
            logger.error("Executor watchdog error: %s", e, exc_info=True)
            await asyncio.sleep(10)


def start_watchdog() -> None:
    """Launch the executor watchdog as a background asyncio task."""
    global _watchdog_task
    if _watchdog_task is None or _watchdog_task.done():
        _watchdog_task = asyncio.create_task(_watchdog_loop())


async def stop_watchdog() -> None:
    """Cancel the executor watchdog."""
    global _watchdog_task
    if _watchdog_task and not _watchdog_task.done():
        _watchdog_task.cancel()
        try:
            await _watchdog_task
        except asyncio.CancelledError:
            pass
    _watchdog_task = None


def shutdown_executor() -> None:
    """Gracefully shut down all executor pools. Call on application shutdown."""
    global _executors
    with _executor_lock:
        for pool in ExecutorPool:
            executor = _executors[pool]
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)
                _executors[pool] = None
                logger.info("ThreadPoolExecutor[%s] shut down", pool.value)


async def run_in_executor(
    func: Callable[..., T],
    *args: Any,
    timeout: float = 30.0,
    pool: ExecutorPool = ExecutorPool.FRONTEND,
    **kwargs: Any,
) -> T:
    """Run a synchronous function in the thread pool with timeout.

    Args:
        func: The synchronous function to execute.
        *args: Positional arguments for func.
        timeout: Maximum seconds to wait (default 30).
        pool: Which executor pool to use (default FRONTEND).
        **kwargs: Keyword arguments for func.

    Returns:
        The return value of func.

    Raises:
        asyncio.TimeoutError: If the function exceeds the timeout.
    """
    func_name = getattr(func, "__name__", str(func))
    start = time.monotonic()

    loop = asyncio.get_running_loop()
    executor = get_executor(pool)

    if kwargs:
        call = partial(func, *args, **kwargs)
    else:
        call = partial(func, *args) if args else func

    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(executor, call),
            timeout=timeout,
        )
        elapsed = time.monotonic() - start
        logger.debug("Executor[%s]: %s completed in %.2fs", pool.value, func_name, elapsed)
        return result
    except asyncio.TimeoutError:
        elapsed = time.monotonic() - start
        logger.warning(
            "Executor[%s]: %s timed out after %.2fs (limit: %.1fs)",
            pool.value,
            func_name,
            elapsed,
            timeout,
        )
        raise
    except Exception as exc:
        elapsed = time.monotonic() - start
        logger.warning(
            "Executor[%s]: %s failed after %.2fs — %s: %s",
            pool.value,
            func_name,
            elapsed,
            type(exc).__name__,
            str(exc)[:200],
        )
        raise


async def run_in_background_executor(
    func: Callable[..., T],
    *args: Any,
    timeout: float = 60.0,
    **kwargs: Any,
) -> T:
    """Convenience wrapper for background collection tasks (daily bars, stock lists)."""
    return await run_in_executor(func, *args, timeout=timeout, pool=ExecutorPool.BACKGROUND, **kwargs)


async def run_in_profile_executor(
    func: Callable[..., T],
    *args: Any,
    timeout: float = 60.0,
    **kwargs: Any,
) -> T:
    """Convenience wrapper for stock profile collection tasks."""
    return await run_in_executor(func, *args, timeout=timeout, pool=ExecutorPool.PROFILE, **kwargs)
