# CLAUDE.md — StockPulse Project Guide

## Overview

StockPulse is a standalone universal stock data platform providing market data collection, caching, and API access. Extracted from WebStock's data-service module, it serves as a reusable data backend for any consumer.

**Stack**: FastAPI (Python 3.11) + React 18 (TypeScript) + PostgreSQL 16 (pgvector) + Redis 7
**Port**: 8010 (backend), 80 (production nginx)

---

## Project Structure

```
stockpulse/
├── backend/
│   ├── pyproject.toml
│   ├── alembic.ini
│   ├── alembic/versions/          # 001_initial_schema
│   └── app/
│       ├── main.py                # FastAPI app with lifespan
│       ├── config.py              # Pydantic Settings
│       ├── core/
│       │   ├── auth.py            # JWT (admin) + X-API-Key (consumers) dual auth
│       │   ├── database.py        # asyncpg pool (bulk ops)
│       │   ├── orm.py             # SQLAlchemy async (ORM CRUD)
│       │   ├── redis.py           # Redis cache wrapper
│       │   ├── secrets.py         # JWT secret bootstrap (env → DB)
│       │   ├── executor.py        # 3-pool ThreadPoolExecutor + watchdog
│       │   ├── scheduler.py       # APScheduler + Redis leader election + 7 cron jobs
│       │   ├── api_keys.py        # Provider key management (DB + env fallback + pub/sub)
│       │   ├── request_id.py      # Request ID middleware
│       │   └── request_logger.py  # API usage stats to Redis
│       ├── models/                # SQLAlchemy ORM (user, api_consumer, system_setting, provider_config)
│       ├── schemas/               # Pydantic (base/CamelModel, ApiResponse, auth, stock, market, analysis, news, content, reference)
│       ├── providers/             # 14 data source adapters (yfinance, akshare, finnhub, tiingo, tushare, trafilatura, playwright, tavily, polygon, + news)
│       ├── services/              # 17 business logic services
│       ├── api/
│       │   ├── health.py          # GET /health (no auth)
│       │   ├── auth.py            # /api/v1/auth/* (register, login, refresh, logout, me)
│       │   ├── public/            # /api/v1/data/* (X-API-Key auth — stock, market, analysis, news, content, reference, internal)
│       │   └── admin/             # /api/v1/admin/* (JWT auth — consumers, users, providers, settings, collection, scheduler, stats)
│       └── utils/
├── frontend/src/                  # React admin dashboard (WebStock-style UI)
│   ├── components/ui/             # 20 Radix UI components (from WebStock)
│   ├── components/layout/         # AdminLayout (collapsible sidebar)
│   ├── pages/                     # 7 pages: Dashboard, Collection, Consumers, Providers, Stats, Settings, Login
│   ├── api/                       # Axios client with JWT interceptor
│   ├── stores/                    # Zustand (auth, theme, toast)
│   └── i18n/                      # en/zh translations
├── docker/                        # nginx.conf, supervisord.conf, entrypoint.sh
├── docker-compose.yml             # Production: postgres + redis + app
├── docker-compose.dev.yml         # Dev: postgres:5433 + redis:6380 + backend:8010
└── Dockerfile                     # Multi-stage: frontend-builder → production (supervisord)
```

---

## Authentication

### Admin UI (JWT Bearer)
- Access token: 30min, refresh token: 7d with atomic jti claiming (Redis)
- First registered user auto-promotes to admin
- `FIRST_ADMIN_EMAIL` env restricts bootstrap admin slot

### Machine Consumers (X-API-Key)
- SHA-256 hash stored in `api_consumers` table
- Raw key shown only at creation time
- Per-consumer rate limiting + allowed_endpoints filtering
- `last_used_at` updated on each request

---

## API Tiers

| Tier | Prefix | Auth | Purpose |
|------|--------|------|---------|
| Health | `/health` | None | Docker health check |
| Auth | `/api/v1/auth/*` | None/JWT | Register, login, refresh, logout |
| Public | `/api/v1/data/*` | X-API-Key | Stock quotes, history, news, content, analysis |
| Internal | `/api/v1/data/internal/*` | X-API-Key | Symbols list, history batch (machine-to-machine) |
| Admin | `/api/v1/admin/*` | JWT Bearer | Consumers, providers, collection, settings, scheduler, stats |

---

## Database Schema

**PostgreSQL 16** with 7 tables:
- `users` — admin accounts (int PK)
- `api_consumers` — machine consumers (UUID PK, SHA-256 hashed API key)
- `system_settings` — key-value store (JWT secret, etc.)
- `provider_configs` — data source health and API keys
- `stock_daily_bars` — OHLCV data (bigserial PK, unique symbol+date)
- `stock_symbols` — stock list ~37K symbols (symbol PK)

---

## Scheduler (APScheduler + Redis Leader Election)

| Job | Schedule (UTC) | Purpose |
|-----|---------------|---------|
| collect_cn | 08:00 | A-share daily bars |
| collect_hk | 09:00 | HK daily bars |
| collect_us | 22:00 | US daily bars |
| collect_metal | 22:30 | Precious metals bars |
| update_stock_list | 05:30 | Build ~37K symbol list |
| build_stock_kb | Sun 06:00 | Stock profile collection |
| sync_concept_boards | Mon-Sat 06:00 | A-share concept mapping |

---

## Common Commands

```bash
# Development
docker compose -f docker-compose.dev.yml up -d    # Start postgres + redis + backend
cd frontend && npm run dev                          # Start frontend dev server (port 3000)

# Production
docker compose up -d --build

# Database
cd backend && alembic upgrade head
```

---

## Redis

- **DB**: 0 (own instance)
- **Key prefix**: `sp:` (scheduler, cache, stats, rate limit)
- **Policy**: noeviction (critical for JWT denylist)

---

## Environment Variables

See `.env.example` for full list.

**Required**: `DATABASE_URL`, `REDIS_URL`
**Optional**: `JWT_SECRET_KEY` (auto-generated), `FIRST_ADMIN_EMAIL`, `FINNHUB_API_KEY`, `CORS_ORIGINS`

---

## Test Account

**Admin**: `admin@stockpulse.dev` / `Admin123`
