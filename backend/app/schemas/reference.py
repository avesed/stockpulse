"""Reference data models -- stock lists, profiles."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class StockListItem(BaseModel):
    """A single entry in the stock list / symbol directory."""

    symbol: str
    name: str
    name_zh: Optional[str] = None
    exchange: Optional[str] = None
    market: str
    pinyin: Optional[str] = None
    pinyin_initial: Optional[str] = None


class StockListResult(BaseModel):
    """Paginated / filtered stock list response."""

    items: list[StockListItem]
    count: int


class StockProfileData(BaseModel):
    """Detailed stock profile for knowledge base construction."""

    symbol: str
    market: str
    name: Optional[str] = None
    name_zh: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    concepts: list[str] = []
    main_business: str = ""
    description: Optional[str] = None


class StockProfileResult(BaseModel):
    """Batch stock profile response."""

    profiles: list[StockProfileData]
    count: int
    market: str


class ConceptMappingResult(BaseModel):
    """CN concept board -> stock mapping result."""

    concepts: dict[str, list[str]]  # 6-digit code -> concept board names
    names: dict[str, str]           # 6-digit code -> name_zh
    count: int                      # number of unique stocks


class BatchProfileRequest(BaseModel):
    """Request for batch stock profile collection (max 50 symbols)."""

    market: Literal["cn", "us", "hk"]
    symbols: list[str] = Field(..., max_length=50)


class IndexConstituentsResult(BaseModel):
    """Index constituent symbols response."""

    symbols: list[str]
    count: int
    index_code: str
    market: str
    source: Optional[str] = None
