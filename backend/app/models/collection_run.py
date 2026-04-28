"""Collection run audit model — tracks each data collection execution."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.core.orm import Base


class CollectionRun(Base):
    __tablename__ = "collection_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market: Mapped[str] = mapped_column(String(10), nullable=False)
    run_type: Mapped[str] = mapped_column(String(20), nullable=False)  # collect, rebuild, scheduled
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")  # running, completed, failed
    symbols_total: Mapped[int] = mapped_column(Integer, default=0)
    symbols_done: Mapped[int] = mapped_column(Integer, default=0)
    new_bars: Mapped[int] = mapped_column(Integer, default=0)
    error_count: Mapped[int] = mapped_column(Integer, default=0)
    errors_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)  # [{symbol, error, category}]
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    triggered_by: Mapped[str] = mapped_column(String(100), nullable=False, default="api")

    __table_args__ = (
        Index("ix_collection_runs_market_started", "market", started_at.desc()),
    )
