"""Admin scheduler status and manual trigger endpoints."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from app.core.auth import require_admin
from app.models.user import User
from app.schemas.base import CamelModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin/scheduler", tags=["admin"])


class JobInfo(CamelModel):
    id: str
    name: str
    next_run_time: str | None = None
    trigger: str | None = None


class SchedulerStatus(CamelModel):
    is_leader: bool
    running: bool
    jobs: list[JobInfo]


@router.get("/status", response_model=SchedulerStatus)
async def get_scheduler_status(admin: User = Depends(require_admin)):
    """Get scheduler status and job list."""
    from app.core.scheduler import get_scheduler, is_leader

    scheduler = get_scheduler()
    leader = is_leader()

    jobs = []
    if scheduler is not None:
        for job in scheduler.get_jobs():
            jobs.append(JobInfo(
                id=job.id,
                name=job.name,
                next_run_time=str(job.next_run_time) if job.next_run_time else None,
                trigger=str(job.trigger),
            ))

    return SchedulerStatus(
        is_leader=leader,
        running=scheduler is not None and scheduler.running,
        jobs=jobs,
    )


@router.post("/jobs/{job_id}/trigger")
async def trigger_job(
    job_id: str,
    admin: User = Depends(require_admin),
):
    """Manually trigger a scheduled job."""
    from app.core.scheduler import get_scheduler, is_leader

    if not is_leader():
        raise HTTPException(status_code=409, detail="This worker is not the scheduler leader")

    scheduler = get_scheduler()
    if scheduler is None:
        raise HTTPException(status_code=503, detail="Scheduler not running")

    job = scheduler.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    # Run the job's function immediately
    job.modify(next_run_time=None)  # Reset to run ASAP
    scheduler.wakeup()

    logger.info("Admin %s triggered job %s", admin.email, job_id)
    return {"status": "triggered", "job_id": job_id, "job_name": job.name}
