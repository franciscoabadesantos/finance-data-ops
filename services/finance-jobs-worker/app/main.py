from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from time import perf_counter
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request

from app.config import get_settings
from app.executors import JobExecutor
from app.models import ExecuteJobRequest
from app.registry import WorkerRegistryStore

LOGGER = logging.getLogger("finance-jobs-worker")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    registry = WorkerRegistryStore(settings.database_url)
    executor = JobExecutor(settings=settings, registry=registry)

    app.state.settings = settings
    app.state.registry = registry
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
def execute_job(payload: ExecuteJobRequest) -> Any:
    executor: JobExecutor = app.state.executor
    job_id = f"analysis-worker-{uuid4().hex[:12]}"

    try:
        result = executor.execute(payload, job_id=job_id, attempt=1, task_name=None)
        return {
            "ok": True,
            "job_id": job_id,
            "job_type": payload.job_type,
            "analysis_job_id": payload.job_id,
            "result": result,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
