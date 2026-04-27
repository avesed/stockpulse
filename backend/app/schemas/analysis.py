"""Analysis-oriented data models -- ratings, technicals, institutional data."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class AnalystRatings(BaseModel):
    """Analyst consensus ratings and price targets."""

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


class TechnicalInfo(BaseModel):
    """Pre-computed technical data points from the data source."""

    symbol: str
    fifty_day_average: Optional[float] = None
    two_hundred_day_average: Optional[float] = None
    average_volume: Optional[int] = None
    average_volume_10days: Optional[int] = None
    beta: Optional[float] = None
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    current_price: Optional[float] = None


class NorthboundData(BaseModel):
    """AKShare northbound (Stock Connect) capital flow data."""

    data: list[dict] = []  # Raw data structure varies by endpoint
    indicator: Optional[str] = None
    code: Optional[str] = None


class InstitutionalData(BaseModel):
    """Institutional holder information."""

    symbol: str
    holders: list[dict] = []
    total_institutional_pct: Optional[float] = None
    data_as_of: Optional[str] = None


class FundHoldings(BaseModel):
    """Fund/ETF holdings breakdown."""

    symbol: str
    holdings: list[dict] = []


class SectorIndustry(BaseModel):
    """Sector, industry, and concept board classification."""

    symbol: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    concept_boards: list[str] = []  # For A-shares (AKShare concept boards)
