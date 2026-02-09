from __future__ import annotations

import asyncio
import time
import traceback
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional


@dataclass
class Job:
    job_id: str
    job_type: str
    status: str  # queued | running | succeeded | failed
    created_at: float
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    error: Optional[str] = None
    result: Optional[Any] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class JobManager:
    """A tiny in-memory background job manager.

    Notes:
    - Works best with a single Uvicorn worker (one process).
    - If you run multiple workers, each process has its own in-memory job list.
    """

    def __init__(self) -> None:
        self._jobs: Dict[str, Job] = {}
        self._lock = asyncio.Lock()

    async def create(self, job_type: str, coro_factory: Callable[[], Awaitable[Any]]) -> Job:
        job_id = uuid.uuid4().hex
        job = Job(job_id=job_id, job_type=job_type, status="queued", created_at=time.time())

        async with self._lock:
            self._jobs[job_id] = job

        async def _runner() -> None:
            job.status = "running"
            job.started_at = time.time()
            try:
                job.result = await coro_factory()
                job.status = "succeeded"
            except Exception:
                job.error = traceback.format_exc()
                job.status = "failed"
            finally:
                job.finished_at = time.time()

        # Fire-and-forget background task.
        asyncio.create_task(_runner())
        return job

    async def get(self, job_id: str) -> Optional[Job]:
        async with self._lock:
            return self._jobs.get(job_id)

    async def list(self, limit: int = 50) -> List[Job]:
        async with self._lock:
            jobs = list(self._jobs.values())

        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs[: max(1, min(int(limit), 200))]
