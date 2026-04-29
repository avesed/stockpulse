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


class AnalystRatingsData(BaseModel):
    """Analyst ratings and price targets."""

    symbol: str
    recommendation: Optional[str] = None
    recommendation_mean: Optional[float] = None
    target_mean_price: Optional[float] = None
    target_high_price: Optional[float] = None
    target_low_price: Optional[float] = None
    target_median_price: Optional[float] = None
    number_of_analysts: Optional[int] = None
    current_price: Optional[float] = None
    upside_pct: Optional[float] = None
    market: str
    source: Optional[str] = None


class NorthboundHoldingEntry(BaseModel):
    """Single northbound holding data point."""

    date: str
    close_price: Optional[float] = None
    holding_shares: Optional[int] = None
    holding_value: Optional[float] = None
    holding_pct: Optional[float] = None
    change_shares: Optional[float] = None


class NorthboundHoldingsData(BaseModel):
    """Northbound capital flow holdings for a CN stock."""

    symbol: str
    holdings: list[NorthboundHoldingEntry] = []
    source: Optional[str] = None


class InstitutionalHolderEntry(BaseModel):
    """Single institutional holder record."""

    holder: str
    date_reported: Optional[str] = None
    pct_held: Optional[float] = None
    shares: Optional[int] = None
    value: Optional[int] = None
    pct_change: Optional[float] = None


class InstitutionalHoldersData(BaseModel):
    """Institutional holders for a stock."""

    symbol: str
    holders: list[InstitutionalHolderEntry] = []
    total_institutional_pct: Optional[float] = None
    source: Optional[str] = None


class FundHoldingsData(BaseModel):
    """Fund (mutual fund) holdings for a CN stock."""

    symbol: str
    quarter: Optional[str] = None
    institution_count: Optional[int] = None
    institution_count_change: Optional[int] = None
    holding_pct: Optional[float] = None
    holding_pct_change: Optional[float] = None
    float_pct: Optional[float] = None
    float_pct_change: Optional[float] = None
    source: Optional[str] = None


class PeerStock(BaseModel):
    """A peer/comparable stock."""

    symbol: str
    name: Optional[str] = None
    market: str
    sector: Optional[str] = None
    industry: Optional[str] = None


class PeersData(BaseModel):
    """Peer stocks for a given symbol."""

    symbol: str
    industry: Optional[str] = None
    sector: Optional[str] = None
    peers: list[PeerStock] = []
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
