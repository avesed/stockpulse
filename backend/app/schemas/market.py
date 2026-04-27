"""Market-level data models -- indices, forex, HSI constituents."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class IndexBar(BaseModel):
    """Single bar for a market index."""

    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int


class IndexData(BaseModel):
    """Market index data with history."""

    symbol: str
    name: str
    bars: list[IndexBar]
    latest_close: Optional[float] = None
    change_pct: Optional[float] = None


class CurrencyInfo(BaseModel):
    """Metadata for a single supported currency."""

    code: str
    symbol: str
    name_en: str
    name_zh: str


class ForexRates(BaseModel):
    """Foreign exchange rates from USD base currency.

    ``rates`` contains all available currency pairs (e.g. {"EUR": 0.92, ...}).
    ``supported_currencies`` lists the subset recognised by the conversion API.
    """

    rates: dict[str, float]
    supported_currencies: list[CurrencyInfo] = []


class HSIConstituents(BaseModel):
    """Hang Seng Index constituent symbols."""

    symbols: list[str]
    count: int
    source: Optional[str] = None
