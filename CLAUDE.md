# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

StockPulse is a standalone universal stock data platform providing market data collection, caching, and API access. Extracted from WebStock's data-service module, it serves as a reusable data backend for any consumer.

**Stack**: FastAPI (Python 3.11) + React 18 (TypeScript) + PostgreSQL 16 (pgvector) + Redis 7
**Ports**: 8010 (backend), 80 (production nginx), 5433 (postgres dev), 6380 (redis dev)

---

## Common Commands

```bash
# Development — full-stack from source (postgres + redis + app)
docker compose up -d --build

# Production — pre-built image
docker compose -f docker-compose.prod.yml up -d

# Database migrations (run inside container or with backend/ as cwd)
cd backend && alembic upgrade head

# Frontend dev (standalone, outside Docker)
cd frontend && npm install && npm run dev        # Vite on :5173
cd frontend && npm run build                     # production build
cd frontend && npm run type-check                # tsc --noEmit
cd frontend && npm run lint                      # eslint

# Rebuild just the app container (preserves postgres/redis volumes)
docker compose up -d --build app
```

No test suite exists yet. Validate changes by building the Docker image and testing via the admin UI or API.

---

## Architecture

### Dual Database Layer

The backend uses **two database access patterns** side by side:
- **asyncpg pool** (`core/database.py`) — raw SQL for bulk operations (collection upserts, batch queries). Used in all collection services via `get_db_pool()`.
- **SQLAlchemy async** (`core/orm.py`) — ORM for CRUD on `users`, `api_consumers`, `system_settings`, `provider_configs`. Used in auth and admin endpoints.

Both connect to the same PostgreSQL instance. The split exists because bulk collection upserts (thousands of `ON CONFLICT` inserts) are significantly faster with raw asyncpg.

### Concurrency Model: Sync Providers in Async App

Most data providers (yfinance, akshare, tushare, finnhub SDK) are synchronous blocking libraries. The app bridges them to async FastAPI via:

1. **Three ThreadPoolExecutor pools** (`core/executor.py`) — FRONTEND (user API, 20 threads), BACKGROUND (collection, 10 threads), PROFILE (knowledge base, 5 threads). Isolated to prevent collection from starving user requests. Self-healing watchdog probes health every 2 min, recycles pools every 4 hours.

2. **ProviderQueue** (`core/provider_queue.py`) — rate-limited priority queue per provider. Token-bucket enforces rate limits (yfinance: 600/min, Finnhub: 58/min per key). Priority levels: REALTIME(0) > FRONTEND(1) > SCHEDULED(5) > BACKFILL(10). All yfinance/Finnhub calls go through `submit()`.

3. **Multiprocessing pool** (`ml_collection_service.py`) — separate `mp.Pool` for yfinance `valuation_history` collection (CN/HK). Subprocess isolation for memory leak mitigation. Currently only covers one job type; expansion planned.

**yfinance memory leak**: yfinance leaks memory via internal LRU caches and `requests.Session` objects. The multiprocessing pool mitigates this through process-level isolation (`maxtasksperchild` for automatic worker recycling). Worker functions must call `ticker._data.cache_get.cache_clear()` after each fetch.

### Provider Routing

`StockRouter` (`services/stock_router.py`) routes requests by market with fallback chains:
- **US/Metal**: yfinance primary → Tiingo fallback
- **HK**: AKShare primary → yfinance fallback
- **A-shares (SH/SZ)**: AKShare primary → Tushare fallback → yfinance fallback

Each provider extends `DataProvider` base class (`providers/base.py`). Provider API keys are managed in `provider_configs` table with env fallback, hot-reloaded via Redis pub/sub (`core/api_keys.py`).

### Collection Pipeline

Two collection services handle scheduled data gathering:

- **`fundamentals_collection_service.py`** — 5 jobs: financials (weekly), analyst_ratings (daily), northbound (daily CN), institutional_holders (quarterly), fund_holdings (quarterly CN). Uses `_run_job()` generic runner with Redis lock, progress tracking, and WebSocket progress broadcast.

- **`ml_collection_service.py`** — 14 jobs: valuation, insider, earnings, sentiment, macro, alternatives. Same `_run_job()` pattern. Mixed providers (Finnhub for US fundamentals, yfinance for scraping, AKShare for CN alternative data).

Both follow the same pattern: Redis distributed lock → resolve symbols → batch loop with per-symbol fetch → asyncpg upsert → progress to Redis + WebSocket → audit record in `collection_runs`.

### WebSocket Architecture

Two-tier WebSocket system:

- **Upstream collectors** (`ws/upstream/`) — persistent connections to Yahoo WS, Finnhub WS, Polygon WS. Parse raw feeds (protobuf for Yahoo, JSON for others) into normalized events.
- **Downstream fan-out** (`ws/redis_fanout.py` + `ws/manager.py`) — Redis pub/sub broadcasts events across Uvicorn workers. `ConnectionManager` dispatches to subscribed client sessions.
- **Two endpoints**: `/api/v1/ws/admin` (JWT, collection progress + quotes) and `/api/v1/ws/data` (API key, quote subscriptions).

The yfinance WS collector (`ws/upstream/yfinance_collector.py`) connects directly to `wss://streamer.finance.yahoo.com` and only uses `yfinance.pricing_pb2` for protobuf decoding — no `yf.Ticker()` calls, no memory leak concern.

### Scheduler

APScheduler with Redis-based leader election ensures only one worker runs cron jobs in multi-process deployments. Jobs defined in `core/scheduler.py`. Leader acquires `sp:scheduler:leader` with TTL; non-leaders skip execution.

---

## Authentication

### Admin UI (JWT Bearer)
- Access token: 30min, refresh token: 7d with atomic jti claiming (Redis)
- First registered user auto-promotes to admin
- `FIRST_ADMIN_EMAIL` env restricts bootstrap admin slot

### Machine Consumers (X-API-Key)
- SHA-256 hash stored in `api_consumers` table
- Per-consumer rate limiting + allowed_endpoints filtering

---

## API Tiers

| Tier | Prefix | Auth | Purpose |
|------|--------|------|---------|
| Health | `/health` | None | Docker health check |
| Auth | `/api/v1/auth/*` | None/JWT | Register, login, refresh, logout |
| Public | `/api/v1/data/*` | X-API-Key | Stock quotes, history, news, analysis |
| Internal | `/api/v1/data/internal/*` | X-API-Key | Symbols list, history batch (machine-to-machine) |
| Admin | `/api/v1/admin/*` | JWT Bearer | Consumers, providers, collection, scheduler, stats |
| WS Admin | `/api/v1/ws/admin` | JWT (query param) | Real-time collection progress, quotes |
| WS Data | `/api/v1/ws/data` | X-API-Key (query param) | Real-time quote subscriptions |

---

## Redis

- **DB**: 0 (own instance, port 6380 in dev)
- **Key prefix**: `sp:` — scheduler leader, cache, stats, rate limit, collection locks/progress, JWT denylist
- **Policy**: noeviction (critical for JWT denylist and distributed locks)

---

## Environment Variables

See `.env.example` for full list.

**Required**: `DATABASE_URL`, `REDIS_URL`
**Optional**: `JWT_SECRET_KEY` (auto-generated), `FIRST_ADMIN_EMAIL`, `FINNHUB_API_KEY`, `FINNHUB_API_KEYS` (comma-separated multi-key), `CORS_ORIGINS`

---

## Frontend

React 18 admin dashboard using Vite, Tailwind CSS, Radix UI components, TanStack React Query, Zustand stores, and i18next (en/zh). Axios client with JWT interceptor auto-refreshes tokens. Built frontend is served by nginx in the Docker image.

---

## Test Account

**Admin**: `admin@stockpulse.dev` / `Admin123`
