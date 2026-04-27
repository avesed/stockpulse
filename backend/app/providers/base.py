"""Abstract base class for data providers in the data-service.

Each external data source (yfinance, akshare, tiingo, tushare) implements this
ABC. Providers return plain dicts; the API layer converts them to Pydantic models.

Design principles:
- Return None on error (no exceptions propagated to caller)
- Log errors internally
- Support market-specific routing via supported_markets property
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set


class DataProvider(ABC):
    """Abstract base for all stock data source providers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider identifier: 'yfinance', 'akshare', 'tiingo', 'tushare'."""
        ...

    @property
    @abstractmethod
    def supported_markets(self) -> Set[str]:
        """Set of market strings this provider supports.

        Valid values: 'us', 'hk', 'sh', 'sz', 'metal'.
        """
        ...

    def supports_market(self, market: str) -> bool:
        """Check if this provider supports a given market."""
        return market.lower() in self.supported_markets

    @classmethod
    def is_available(cls) -> bool:
        """Check if this provider is available (e.g. API key configured).

        Override in subclasses that require configuration.
        """
        return True

    # === Core Abstract Methods (must implement) ===

    @abstractmethod
    async def get_quote(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get real-time quote for a symbol. Returns plain dict or None."""
        ...

    @abstractmethod
    async def get_history(
        self,
        symbol: str,
        market: str,
        period: str,
        interval: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get historical OHLCV data. Returns plain dict or None."""
        ...

    @abstractmethod
    async def search(
        self, query: str, markets: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        """Search for stocks matching query. Returns list of dicts."""
        ...

    # === Optional Methods (default to None / empty) ===

    async def get_info(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get company/asset information. Optional."""
        return None

    async def get_financials(
        self, symbol: str, market: str
    ) -> Optional[Dict[str, Any]]:
        """Get financial metrics. Optional."""
        return None

    async def get_analyst_ratings(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get analyst ratings and price targets. Optional."""
        return None

    async def get_technical_info(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Get pre-calculated technical data (SMA, ADTV, beta). Optional."""
        return None
