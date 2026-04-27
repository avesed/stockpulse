"""Base schemas for StockPulse."""

from __future__ import annotations

from typing import Generic, Optional, TypeVar

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

T = TypeVar("T")


class CamelModel(BaseModel):
    """Base model that auto-converts snake_case to camelCase for JSON."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
        serialize_by_alias=True,
    )


class ApiResponse(BaseModel, Generic[T]):
    """Unified API response envelope for data endpoints."""

    success: bool = True
    data: Optional[T] = None
    error: Optional[str] = None
    source: Optional[str] = None  # "yfinance" / "akshare" / "finnhub" ...
    cached: bool = False
    elapsed_ms: Optional[int] = None
