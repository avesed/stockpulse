"""Stock data models -- quotes, history, info, financials, search."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class QuoteData(BaseModel):
    """Real-time or delayed stock quote."""

    symbol: str
    name: Optional[str] = None
    price: float
    change: float
    change_percent: float
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    open: Optional[float] = None
    previous_close: Optional[float] = None
    timestamp: Optional[str] = None
    market: str  # "us" / "hk" / "sh" / "sz" / "metal"
    currency: Optional[str] = None
    source: Optional[str] = None


class OHLCVBar(BaseModel):
    """Single OHLCV candlestick bar."""

    date: str
    open: float
    high: float
    low: float
    close: float
    volume: Optional[int] = None


class HistoryData(BaseModel):
    """Historical price series for a symbol."""

    symbol: str
    bars: list[OHLCVBar]
    interval: str
    market: str


class InfoData(BaseModel):
    """Company / instrument information."""

    symbol: str
    name: str
    description: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    website: Optional[str] = None
    employees: Optional[int] = None
    market_cap: Optional[float] = None
    currency: Optional[str] = None
    exchange: Optional[str] = None
    market: str
    source: Optional[str] = None


class FinancialsData(BaseModel):
    """Key financial metrics and ratios."""

    symbol: str
    pe_ratio: Optional[float] = None
    forward_pe: Optional[float] = None
    eps: Optional[float] = None
    dividend_yield: Optional[float] = None
    dividend_rate: Optional[float] = None
    book_value: Optional[float] = None
    price_to_book: Optional[float] = None
    revenue: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_income: Optional[float] = None
    profit_margin: Optional[float] = None
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    eps_growth: Optional[float] = None
    payout_ratio: Optional[float] = None
    market: str
    source: Optional[str] = None


class SearchItem(BaseModel):
    """A single search result item."""

    symbol: str
    name: str
    exchange: Optional[str] = None
    market: str


# --- Batch daily bars models ---


class DailyBarSymbolRequest(BaseModel):
    """Per-symbol request in a batch daily bars fetch."""

    symbol: str
    start_date: Optional[str] = None  # YYYY-MM-DD; None = full history


class BatchDailyBarsRequest(BaseModel):
    """Request body for batch daily bars endpoint."""

    symbols: list[DailyBarSymbolRequest]  # max 50
    market: str  # us, hk, cn, metal


class SymbolBarsResult(BaseModel):
    """Bars result for a single symbol."""

    bars: list[OHLCVBar]
    source: str  # "yfinance" or "akshare"


class BatchDailyBarsData(BaseModel):
    """Response data for batch daily bars endpoint."""

    results: dict[str, SymbolBarsResult]  # symbol -> bars
    errors: dict[str, str]  # symbol -> error message
