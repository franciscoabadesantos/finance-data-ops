# finance-jobs-worker

Cloud Run worker for request-driven ticker jobs.

This service is intentionally separate from the public backend API. It executes
job payloads delivered by Cloud Tasks and calls `finance-data-ops` logic for:

- ticker validation
- ticker backfill

## Endpoints

- `GET /health`
- `POST /jobs/execute`

## Security

Set `WORKER_SHARED_TOKEN` and require `Authorization: Bearer <token>` from the
Cloud Tasks HTTP target.

## Required env vars

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `WORKER_SHARED_TOKEN`

## Optional env vars

- `FINANCE_DATA_OPS_ROOT` (default: repo root)
- `CLOUD_TASKS_ENABLED` (default: `true`)
- `GCP_PROJECT_ID`
- `GCP_LOCATION` (default: `us-central1`)
- `GCP_TASKS_QUEUE` (default: `ticker-jobs`)
- `WORKER_BASE_URL` (used for enqueueing chained backfill jobs)
- `TASKS_INVOKER_SERVICE_ACCOUNT_EMAIL`

## Local run

```bash
cd services/finance-jobs-worker
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8090
```
