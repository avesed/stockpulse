"""Configuration for StockPulse."""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # App
    app_name: str = "StockPulse"
    debug: bool = False

    # Database — dual layer: asyncpg pool (bulk ops) + SQLAlchemy (ORM)
    DATABASE_URL: str = "postgresql+asyncpg://stockpulse:stockpulse@postgres:5432/stockpulse"

    # asyncpg pool settings
    DATABASE_POOL_MIN_SIZE: int = 2
    DATABASE_POOL_MAX_SIZE: int = 10
    DATABASE_COMMAND_TIMEOUT: int = 120

    # Redis (own instance, DB 0)
    REDIS_URL: str = "redis://redis:6379/0"

    # JWT
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # CORS
    CORS_ORIGINS: str = "http://localhost:3000,http://localhost:5173"

    # Admin bootstrap — first user with this email gets admin role
    FIRST_ADMIN_EMAIL: str = ""

    # External API keys (env fallback; DB provider_configs takes priority)
    FINNHUB_API_KEY: str = ""
    TUSHARE_TOKEN: str = ""
    TIINGO_API_KEY: str = ""
    POLYGON_API_KEY: str = ""

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8010
    LOG_LEVEL: str = "info"

    # Executor pools
    EXECUTOR_MAX_WORKERS: int = 20       # Frontend API requests
    EXECUTOR_BACKGROUND_WORKERS: int = 10  # Daily bar + stock list collection
    EXECUTOR_PROFILE_WORKERS: int = 5    # Stock profile collection

    # WebSocket
    WS_HEARTBEAT_INTERVAL: int = 30          # seconds between server pings
    WS_HEARTBEAT_TIMEOUT: int = 10           # seconds to wait for pong
    WS_MAX_CONNECTIONS_PER_CONSUMER: int = 5  # per API consumer
    WS_MAX_SUBSCRIPTIONS_PER_SESSION: int = 50  # symbols per WS session
    WS_UPSTREAM_RECONNECT_MAX_DELAY: int = 30   # max backoff seconds

    # Upstream WebSocket URLs
    FINNHUB_WS_URL: str = "wss://ws.finnhub.io"
    POLYGON_WS_URL: str = "wss://socket.polygon.io/stocks"

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]


@lru_cache()
def get_settings() -> Settings:
    return Settings()
