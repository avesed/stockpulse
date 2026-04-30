"""News article schemas — pass-through wire format for downstream consumers."""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


class NewsItem(BaseModel):
    """A single news article from one source.

    Lightly normalized so that consumers (e.g. NewsForge) can iterate
    without per-source parsing, while ``raw`` preserves full fidelity
    for downstream dedup/enrichment logic.
    """

    source: str  # "yfinance" / "akshare" / "finnhub" / "tiingo" / "massive" / "tushare"
    id: Optional[str] = None  # provider-specific id
    title: str
    summary: Optional[str] = None
    url: Optional[str] = None
    publisher: Optional[str] = None  # "Reuters" / "财联社" / etc.
    published_at: Optional[str] = None  # ISO 8601
    symbols: list[str] = []  # related tickers
    image_url: Optional[str] = None
    language: Optional[str] = None  # "en" / "zh"
    raw: Optional[dict[str, Any]] = None  # original payload, untouched
