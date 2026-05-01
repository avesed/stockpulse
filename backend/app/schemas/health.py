"""Public health summary schemas — exposed via X-API-Key for external dashboards."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from app.schemas.base import CamelModel


class ProviderHealth(CamelModel):
    """Single data provider health snapshot."""
    name: str
    enabled: bool
    health_status: str  # healthy / degraded / error / unknown
    last_check: Optional[datetime] = None
    error_message: Optional[str] = None


class MarketCollectionStatus(CamelModel):
    """Per-market data collection status."""
    market: str  # us / cn / hk / metal / sh / sz
    last_collection_at: Optional[datetime] = None
    total_bars: int = 0
    total_symbols: int = 0


class HealthSummary(CamelModel):
    """Aggregated health summary for external consumer dashboards."""
    status: str  # healthy / degraded
    service: str = "stockpulse"
    version: Optional[str] = None
    redis: str = "unknown"          # ok / down
    database: str = "unknown"
    executor: str = "unknown"
    providers: list[ProviderHealth] = []
    markets: list[MarketCollectionStatus] = []
