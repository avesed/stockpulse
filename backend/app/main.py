"""StockPulse FastAPI application.

Universal stock data platform — market data collection, caching, and API.
Provides stock quotes, history, news, content extraction, market indices,
analyst ratings, and more via a unified API with dual auth:
- JWT Bearer for admin UI
- X-API-Key for machine consumers
"""
from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.core.redis import close_redis
from app.core.database import close_db_pool, init_db_pool
from app.core.orm import get_engine
from app.core.request_id import RequestIdFilter, RequestIdMiddleware

settings = get_settings()

# Configure logging with request ID injection
LOG_FORMAT = "%(asctime)s [pulse] %(levelname).1s [%(request_id)s] %(message)s"
LOG_DATEFMT = "%H:%M:%S"

_root = logging.getLogger()
_root.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
for _h in _root.handlers[:]:
    _root.removeHandler(_h)
_handler = logging.StreamHandler(sys.stdout)
_handler.addFilter(RequestIdFilter())
_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
_root.addHandler(_handler)
for _name in ("httpx", "httpcore", "urllib3", "asyncio", "watchfiles"):
    logging.getLogger(_name).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: startup and shutdown hooks."""
    logger.info("Starting StockPulse...")

    # Initialize SQLAlchemy engine (for ORM operations)
    get_engine()

    # Initialize asyncpg pool (for bulk operations)
    await init_db_pool()

    # Bootstrap JWT secret (env or DB)
    from app.core.secrets import bootstrap_jwt_secret
    await bootstrap_jwt_secret()

    # Load API keys from provider_configs and start Redis subscriber
    from app.core.api_keys import load_api_keys_from_db, start_subscriber, stop_subscriber
    await load_api_keys_from_db()
    start_subscriber()

    # Start provider rate-limit queues (must be after API keys are loaded)
    from app.core.provider_queue import start_queues, stop_queues
    await start_queues()

    # Start executor watchdog (3-pool health monitoring)
    from app.core.executor import start_watchdog, stop_watchdog, shutdown_executor
    start_watchdog()

    # Start yfinance process pool (subprocess isolation for memory leak mitigation)
    from app.core.yf_process_pool import start_pool as start_yf_pool, stop_pool as stop_yf_pool
    from app.core.api_keys import get_provider_config
    _yf_cfg = get_provider_config("yfinance")
    _yf_proxy = str(_yf_cfg.get("proxy", "")).strip() or None
    try:
        _yf_ui = int(_yf_cfg.get("concurrency", 0))
        _yf_workers = max(1, _yf_ui) if _yf_ui > 0 else settings.YF_POOL_WORKERS
    except (ValueError, TypeError):
        _yf_workers = settings.YF_POOL_WORKERS
    start_yf_pool(
        workers=_yf_workers,
        maxtasksperchild=settings.YF_POOL_MAX_TASKS_PER_CHILD,
        proxy=_yf_proxy,
    )

    # Clean up stale collection_runs and Redis locks left by previous crashes
    from app.services.collection_run_service import mark_stale_runs_failed
    await mark_stale_runs_failed()
    from app.core.redis import get_redis
    _r = await get_redis()
    _stale_locks = await _r.keys("sp:*:lock")
    if _stale_locks:
        await _r.delete(*_stale_locks)
        logger.info("Cleared %d stale collection locks from previous run", len(_stale_locks))

    # Start scheduler (leader election ensures single-worker execution)
    from app.core.scheduler import start_scheduler, stop_scheduler
    await start_scheduler()

    # Start WebSocket fanout subscriber (every worker)
    from app.ws.manager import get_manager
    from app.ws.redis_fanout import start_fanout_subscriber, stop_fanout_subscriber
    ws_manager = get_manager()
    start_fanout_subscriber(ws_manager)

    yield

    logger.info("Shutting down StockPulse...")

    from app.ws.upstream.collector_service import stop_all_collectors
    await stop_all_collectors()
    await stop_fanout_subscriber()
    await stop_scheduler()
    await stop_queues()
    await stop_watchdog()
    await stop_subscriber()
    stop_yf_pool()
    shutdown_executor()
    await close_db_pool()
    await close_redis()
    logger.info("StockPulse shut down")


app = FastAPI(
    title="StockPulse",
    version="1.0.0",
    description="Universal stock data platform — market data collection, caching, and API",
    lifespan=lifespan,
)

# Middleware
from app.core.request_logger import RequestLoggerMiddleware  # noqa: E402
app.add_middleware(RequestLoggerMiddleware)
app.add_middleware(RequestIdMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
from app.api.health import router as health_router  # noqa: E402
from app.api.auth import router as auth_router  # noqa: E402
from app.api.admin.consumers import router as consumers_router  # noqa: E402
from app.api.admin.users import router as users_router  # noqa: E402
from app.api.admin.providers import router as providers_router  # noqa: E402
from app.api.admin.settings import router as settings_router  # noqa: E402
from app.api.admin.scheduler import router as scheduler_router  # noqa: E402
from app.api.admin.stats import router as stats_router  # noqa: E402

# Public data API routers (X-API-Key auth)
from app.api.public.stock import router as stock_router  # noqa: E402
from app.api.public.market import router as market_router  # noqa: E402
from app.api.public.analysis import router as analysis_router  # noqa: E402
from app.api.public.reference import router as reference_router  # noqa: E402
from app.api.public.internal import router as internal_router  # noqa: E402
from app.api.public.news import router as news_router  # noqa: E402
from app.api.public.health_summary import router as health_summary_router  # noqa: E402
from app.api.public.ml_data import router as ml_data_router  # noqa: E402
from app.api.public.ml_data import queue_stats_router as ml_queue_stats_router  # noqa: E402

# Admin collection router (JWT auth)
from app.api.admin.collection import router as collection_router  # noqa: E402

app.include_router(health_router)
app.include_router(auth_router)
app.include_router(consumers_router)
app.include_router(users_router)
app.include_router(providers_router)
app.include_router(settings_router)
app.include_router(scheduler_router)
app.include_router(stats_router)
app.include_router(stock_router)
app.include_router(market_router)
app.include_router(analysis_router)
app.include_router(reference_router)
app.include_router(internal_router)
app.include_router(news_router)
app.include_router(health_summary_router)
app.include_router(ml_data_router)
app.include_router(ml_queue_stats_router)
app.include_router(collection_router)

# Per-provider data API routers (X-API-Key auth, direct provider access)
from app.api.public.provider_endpoints import create_provider_router  # noqa: E402
for _provider_name in ("yfinance", "akshare", "tushare", "tiingo", "massive", "finnhub"):
    app.include_router(create_provider_router(_provider_name))

# WebSocket router (unified)
from app.ws.endpoints import router as ws_router  # noqa: E402
app.include_router(ws_router)

# Per-provider WebSocket routers
from app.ws.provider_ws import create_provider_ws_router  # noqa: E402
for _ws_provider in ("massive", "finnhub", "yfinance"):
    app.include_router(create_provider_ws_router(_ws_provider))
