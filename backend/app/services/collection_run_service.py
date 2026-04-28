"""Service for managing collection run audit records.

Uses SQLAlchemy async ORM (not asyncpg) since write volume is low
(a few rows per day) and queries are simple.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.orm import get_session_factory
from app.models.collection_run import CollectionRun

logger = logging.getLogger(__name__)

_MAX_ERRORS_STORED = 200


async def create_run(
    market: str,
    run_type: str,
    triggered_by: str = "api",
) -> CollectionRun:
    """Insert a new running collection record and return it."""
    factory = get_session_factory()
    async with factory() as session:
        run = CollectionRun(
            market=market,
            run_type=run_type,
            status="running",
            triggered_by=triggered_by,
        )
        session.add(run)
        await session.commit()
        await session.refresh(run)
        return run


async def complete_run(
    run_id: int,
    symbols_total: int,
    symbols_done: int,
    new_bars: int,
    errors: list[dict],
) -> None:
    """Mark a run as completed with final stats."""
    now = datetime.now(timezone.utc)
    factory = get_session_factory()
    async with factory() as session:
        run = await session.get(CollectionRun, run_id)
        if run is None:
            logger.warning("complete_run: run_id=%d not found", run_id)
            return
        run.status = "completed"
        run.symbols_total = symbols_total
        run.symbols_done = symbols_done
        run.new_bars = new_bars
        run.error_count = len(errors)
        run.errors_json = errors[:_MAX_ERRORS_STORED] if errors else None
        run.finished_at = now
        run.duration_seconds = (now - run.started_at).total_seconds() if run.started_at else None
        await session.commit()


async def fail_run(run_id: int, error_message: str) -> None:
    """Mark a run as failed."""
    now = datetime.now(timezone.utc)
    factory = get_session_factory()
    async with factory() as session:
        run = await session.get(CollectionRun, run_id)
        if run is None:
            logger.warning("fail_run: run_id=%d not found", run_id)
            return
        run.status = "failed"
        run.finished_at = now
        run.duration_seconds = (now - run.started_at).total_seconds() if run.started_at else None
        run.errors_json = [{"symbol": "", "error": error_message, "category": "fatal"}]
        run.error_count = 1
        await session.commit()


async def get_runs(
    market: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Paginated listing of collection runs (without errors_json)."""
    factory = get_session_factory()
    async with factory() as session:
        # Count query
        count_q = select(func.count(CollectionRun.id))
        if market:
            count_q = count_q.where(CollectionRun.market == market)
        total = (await session.execute(count_q)).scalar() or 0

        # Data query
        q = (
            select(CollectionRun)
            .order_by(CollectionRun.started_at.desc())
            .offset(offset)
            .limit(limit)
        )
        if market:
            q = q.where(CollectionRun.market == market)
        rows = (await session.execute(q)).scalars().all()

        return [_to_dict(r, include_errors=False) for r in rows], total


async def get_run(run_id: int) -> dict | None:
    """Single run detail with errors_json."""
    factory = get_session_factory()
    async with factory() as session:
        run = await session.get(CollectionRun, run_id)
        if run is None:
            return None
        return _to_dict(run, include_errors=True)


async def get_last_completed(market: str) -> dict | None:
    """Most recent completed run for a market (for 'last run' summary)."""
    factory = get_session_factory()
    async with factory() as session:
        q = (
            select(CollectionRun)
            .where(CollectionRun.market == market, CollectionRun.status == "completed")
            .order_by(CollectionRun.started_at.desc())
            .limit(1)
        )
        run = (await session.execute(q)).scalar_one_or_none()
        if run is None:
            return None
        return {
            "status": run.status,
            "durationSeconds": run.duration_seconds,
            "errorCount": run.error_count,
            "newBars": run.new_bars,
            "finishedAt": run.finished_at.isoformat() if run.finished_at else None,
        }


async def cleanup_old_runs(keep_per_market: int = 500) -> int:
    """Delete runs beyond the newest `keep_per_market` per market."""
    factory = get_session_factory()
    async with factory() as session:
        # Use a CTE to find IDs to delete
        result = await session.execute(
            text("""
                DELETE FROM collection_runs
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id,
                               ROW_NUMBER() OVER (PARTITION BY market ORDER BY started_at DESC) AS rn
                        FROM collection_runs
                    ) sub
                    WHERE rn > :keep
                )
            """),
            {"keep": keep_per_market},
        )
        await session.commit()
        return result.rowcount or 0


def _to_dict(run: CollectionRun, include_errors: bool = False) -> dict:
    d = {
        "id": run.id,
        "market": run.market,
        "runType": run.run_type,
        "status": run.status,
        "symbolsTotal": run.symbols_total,
        "symbolsDone": run.symbols_done,
        "newBars": run.new_bars,
        "errorCount": run.error_count,
        "startedAt": run.started_at.isoformat() if run.started_at else None,
        "finishedAt": run.finished_at.isoformat() if run.finished_at else None,
        "durationSeconds": run.duration_seconds,
        "triggeredBy": run.triggered_by,
    }
    if include_errors:
        d["errorsJson"] = run.errors_json
    return d
