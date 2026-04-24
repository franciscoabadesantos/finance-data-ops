# finance-jobs-worker

Cloud Run worker for request-driven ticker jobs.

This service is intentionally separate from the public backend API. It executes
job payloads delivered by Cloud Tasks and calls `finance-data-ops` logic for:

- ticker validation
- ticker backfill
- analyst snapshot job execution (`analysis_job`)
- admin data-ops rebuild jobs (`analysis_job` with `analysis_type=data_ops_rebuild`)
- admin series upsert jobs (`analysis_job` with `analysis_type=data_ops_series_upsert`)

## Endpoints

- `GET /health`
- `POST /jobs/execute`

## Security

Prefer Cloud Tasks OIDC + Cloud Run IAM auth. `WORKER_SHARED_TOKEN` is optional
defense-in-depth for app-layer bearer verification.

## Required env vars

- `SUPABASE_URL`
- `SUPABASE_SECRET_KEY`

## Optional env vars

- `FINANCE_DATA_OPS_ROOT` (default: repo root)
- `CLOUD_TASKS_ENABLED` (default: `true`)
- `GCP_PROJECT_ID`
- `GCP_LOCATION` (default: `us-central1`)
- `GCP_TASKS_QUEUE` (default: `ticker-jobs`)
- `WORKER_BASE_URL` (used for enqueueing chained backfill jobs)
- `TASKS_INVOKER_SERVICE_ACCOUNT_EMAIL`
- `WORKER_SHARED_TOKEN` (optional, if app-layer bearer auth is enabled)

## Runtime sizing

Recommended Cloud Run floor:

- memory: `1Gi`
- timeout: `300s`

Reason:

- historical earnings rebuilds are the most memory-intensive request path in
  this worker because provider fetches load per-ticker history before local
  window filtering
- `512Mi` is not enough for a 30-symbol historical earnings region request and
  was observed to OOM-kill the worker
- market, fundamentals, macro, and release-calendar completed at lower memory,
  but `1Gi` is the correct floor for the shared worker service

## Local run

```bash
cd services/finance-jobs-worker
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8090
```
