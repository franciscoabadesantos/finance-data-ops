from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import threading
from time import perf_counter
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from app.config import get_settings
from app.executors import JobExecutor
from app.models import ExecuteJobRequest
from app.registry import WorkerRegistryStore
from app.tasks import CloudTasksEnqueuer

LOGGER = logging.getLogger("finance-jobs-worker")
TICKER_JOB_TYPES = {"ticker_validation", "ticker_backfill"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    registry = WorkerRegistryStore(settings.database_url)
    tasks = CloudTasksEnqueuer(settings)
    executor = JobExecutor(settings=settings, registry=registry, tasks=tasks)

    app.state.settings = settings
    app.state.registry = registry
    app.state.tasks = tasks
    app.state.executor = executor
    yield


app = FastAPI(title="finance-jobs-worker", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def auth_and_logging(request: Request, call_next):  # type: ignore[no-untyped-def]
    started = perf_counter()
    settings = get_settings()

    if request.url.path.startswith("/jobs/"):
        expected = str(settings.worker_shared_token or "").strip()
        if expected:
            supplied = str(request.headers.get("authorization") or "").strip()
            if supplied != f"Bearer {expected}":
                raise HTTPException(status_code=403, detail="Forbidden")

    response = await call_next(request)
    duration_ms = (perf_counter() - started) * 1000.0
    LOGGER.info("request method=%s path=%s status=%s duration_ms=%.2f", request.method, request.url.path, response.status_code, duration_ms)
    return response


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/jobs/execute", response_model=None)
def execute_job(
    payload: ExecuteJobRequest,
    x_cloudtasks_taskname: str | None = Header(default=None),
    x_cloudtasks_taskretrycount: str | None = Header(default=None),
) -> Any:
    executor: JobExecutor = app.state.executor
    task_name = str(x_cloudtasks_taskname or "").strip() or None
    task_retry_count = int(str(x_cloudtasks_taskretrycount or "0").strip() or "0")
    attempt = max(task_retry_count + 1, 1)
    job_id = (task_name.split("/")[-1] if task_name else f"job-{uuid4().hex[:12]}")

    if payload.job_type in TICKER_JOB_TYPES:
        _start_ticker_job_background(
            executor=executor,
            payload=payload,
            job_id=job_id,
            attempt=attempt,
            task_name=task_name,
        )
        return JSONResponse(
            status_code=202,
            content={
                "ok": True,
                "accepted": True,
                "job_id": job_id,
                "job_type": payload.job_type,
                "registry_key": payload.registry_key,
                "analysis_job_id": payload.job_id,
            },
        )

    try:
        result = executor.execute(payload, job_id=job_id, attempt=attempt, task_name=task_name)
        return {
            "ok": True,
            "job_id": job_id,
            "job_type": payload.job_type,
            "registry_key": payload.registry_key,
            "analysis_job_id": payload.job_id,
            "result": result,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _start_ticker_job_background(
    *,
    executor: JobExecutor,
    payload: ExecuteJobRequest,
    job_id: str,
    attempt: int,
    task_name: str | None,
) -> None:
    thread = threading.Thread(
        target=_run_ticker_job_background,
        kwargs={
            "executor": executor,
            "payload": payload,
            "job_id": job_id,
            "attempt": attempt,
            "task_name": task_name,
        },
        name=f"ticker-job-{job_id}",
        daemon=True,
    )
    thread.start()


def _run_ticker_job_background(
    *,
    executor: JobExecutor,
    payload: ExecuteJobRequest,
    job_id: str,
    attempt: int,
    task_name: str | None,
) -> None:
    try:
        executor.execute(payload, job_id=job_id, attempt=attempt, task_name=task_name)
    except Exception:
        LOGGER.exception("ticker job failed in background job_id=%s job_type=%s", job_id, payload.job_type)
