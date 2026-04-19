# Cloud Tasks + Cloud Run Deployment Runbook

> Secret-handling baseline:
> - Google Secret Manager is the source of truth for GCP runtime secrets.
> - Vercel Shared Environment Variables are the source of truth for shared Vercel-side secrets/config.
> - Prefer Cloud Tasks OIDC + Cloud Run IAM over static worker bearer tokens.

This runbook deploys request-driven ticker jobs to Google Cloud while keeping Prefect for recurring schedules only.

## Scope

- Prefect remains limited to:
  - `market-daily`
  - `fundamentals-daily`
  - `earnings-daily`
  - `macro-daily`
  - `release-calendar-daily`
- Backend (`finance-backend`) enqueues request-driven jobs through Cloud Tasks.
- Cloud Run worker (`services/finance-jobs-worker`) executes:
  - `ticker_validation`
  - `ticker_backfill`

## 0. Variables

```bash
export PROJECT_ID="<gcp-project-id>"
export REGION="us-central1"
export QUEUE_NAME="ticker-jobs"
export AR_REPO="finance-jobs"
export WORKER_SERVICE="finance-jobs-worker"

export WORKER_RUNTIME_SA="finance-jobs-worker@${PROJECT_ID}.iam.gserviceaccount.com"
export TASKS_INVOKER_SA="finance-tasks-invoker@${PROJECT_ID}.iam.gserviceaccount.com"
export BACKEND_ENQUEUER_SA="finance-backend-enqueuer@${PROJECT_ID}.iam.gserviceaccount.com"

export SECRET_SUPABASE_SERVICE_ROLE_KEY="supabase-service-role-key"
export SECRET_WORKER_SHARED_TOKEN="worker-shared-token" # optional
```

## 1. Enable required APIs

```bash
gcloud services enable \
  run.googleapis.com \
  cloudtasks.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  iamcredentials.googleapis.com \
  --project "${PROJECT_ID}"
```

## 2. Create service accounts

```bash
gcloud iam service-accounts create finance-jobs-worker \
  --project "${PROJECT_ID}" \
  --display-name "Finance Jobs Worker Runtime SA"

gcloud iam service-accounts create finance-tasks-invoker \
  --project "${PROJECT_ID}" \
  --display-name "Cloud Tasks OIDC Invoker SA"

gcloud iam service-accounts create finance-backend-enqueuer \
  --project "${PROJECT_ID}" \
  --display-name "Finance Backend Cloud Tasks Enqueuer SA"
```

## 3. Grant IAM permissions

### 3.1 Worker runtime may enqueue chained jobs

```bash
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${WORKER_RUNTIME_SA}" \
  --role "roles/cloudtasks.enqueuer"
```

### 3.2 Backend enqueuer may create Cloud Tasks

```bash
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${BACKEND_ENQUEUER_SA}" \
  --role "roles/cloudtasks.enqueuer"
```

### 3.3 Backend enqueuer may set OIDC invoker SA on task requests

```bash
gcloud iam service-accounts add-iam-policy-binding "${TASKS_INVOKER_SA}" \
  --member "serviceAccount:${BACKEND_ENQUEUER_SA}" \
  --role "roles/iam.serviceAccountUser"
```

## 4. Create Cloud Tasks queue

```bash
gcloud tasks queues create "${QUEUE_NAME}" \
  --project "${PROJECT_ID}" \
  --location "${REGION}" \
  --max-attempts 10 \
  --min-backoff 10s \
  --max-backoff 300s \
  --max-doublings 5
```

If queue already exists:

```bash
gcloud tasks queues describe "${QUEUE_NAME}" \
  --project "${PROJECT_ID}" \
  --location "${REGION}"
```

## 5. Build and deploy Cloud Run worker

Run from repo root (`finance-data-ops`):

```bash
export IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${WORKER_SERVICE}:$(date +%Y%m%d-%H%M%S)"
```

Create Artifact Registry repo (first time only):

```bash
gcloud artifacts repositories create "${AR_REPO}" \
  --project "${PROJECT_ID}" \
  --repository-format docker \
  --location "${REGION}" \
  --description "Finance worker images"
```

Build image with worker Dockerfile:

```bash
gcloud builds submit . \
  --project "${PROJECT_ID}" \
  --tag "${IMAGE_URI}" \
  --file services/finance-jobs-worker/Dockerfile
```

Create/update Secret Manager secrets (first time + rotations):

```bash
gcloud secrets describe "${SECRET_SUPABASE_SERVICE_ROLE_KEY}" \
  --project "${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud secrets create "${SECRET_SUPABASE_SERVICE_ROLE_KEY}" \
    --project "${PROJECT_ID}" \
    --replication-policy="automatic"

printf '%s' "${SUPABASE_SERVICE_ROLE_KEY}" | \
  gcloud secrets versions add "${SECRET_SUPABASE_SERVICE_ROLE_KEY}" \
    --project "${PROJECT_ID}" \
    --data-file=-
```

Optional worker shared token (only if you want app-layer bearer auth in addition to IAM):

```bash
export WORKER_SHARED_TOKEN="$(openssl rand -hex 32)"
gcloud secrets describe "${SECRET_WORKER_SHARED_TOKEN}" \
  --project "${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud secrets create "${SECRET_WORKER_SHARED_TOKEN}" \
    --project "${PROJECT_ID}" \
    --replication-policy="automatic"

printf '%s' "${WORKER_SHARED_TOKEN}" | \
  gcloud secrets versions add "${SECRET_WORKER_SHARED_TOKEN}" \
    --project "${PROJECT_ID}" \
    --data-file=-
```

Deploy worker service:

```bash
gcloud run deploy "${WORKER_SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${IMAGE_URI}" \
  --service-account "${WORKER_RUNTIME_SA}" \
  --no-allow-unauthenticated \
  --set-env-vars "SUPABASE_URL=${SUPABASE_URL}" \
  --set-secrets "SUPABASE_SERVICE_ROLE_KEY=${SECRET_SUPABASE_SERVICE_ROLE_KEY}:latest" \
  --set-env-vars "FINANCE_DATA_OPS_ROOT=/app" \
  --set-env-vars "CLOUD_TASKS_ENABLED=true" \
  --set-env-vars "GCP_PROJECT_ID=${PROJECT_ID}" \
  --set-env-vars "GCP_LOCATION=${REGION}" \
  --set-env-vars "GCP_TASKS_QUEUE=${QUEUE_NAME}" \
  --set-env-vars "TASKS_INVOKER_SERVICE_ACCOUNT_EMAIL=${TASKS_INVOKER_SA}"
```

Optional if using app-layer worker bearer auth:

```bash
gcloud run services update "${WORKER_SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --set-secrets "WORKER_SHARED_TOKEN=${SECRET_WORKER_SHARED_TOKEN}:latest"
```

Fetch deployed URL and update env:

```bash
export WORKER_URL="$(gcloud run services describe "${WORKER_SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --format='value(status.url)')"

gcloud run services update "${WORKER_SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --update-env-vars "WORKER_BASE_URL=${WORKER_URL}"
```

Allow task-invoker SA to invoke Cloud Run:

```bash
gcloud run services add-iam-policy-binding "${WORKER_SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --member "serviceAccount:${TASKS_INVOKER_SA}" \
  --role "roles/run.invoker"
```

## 6. Backend environment configuration (Vercel)

Create backend enqueuer key only if backend runtime cannot use ADC/workload identity:

```bash
gcloud iam service-accounts keys create /tmp/finance-backend-enqueuer.json \
  --project "${PROJECT_ID}" \
  --iam-account "${BACKEND_ENQUEUER_SA}"
```

Set backend env vars in Vercel Shared Environment Variables (not per-project duplicates):

```bash
vercel env add USE_CLOUD_TASKS production <<< "true"
vercel env add GCP_PROJECT_ID production <<< "${PROJECT_ID}"
vercel env add GCP_LOCATION production <<< "${REGION}"
vercel env add GCP_TASKS_QUEUE production <<< "${QUEUE_NAME}"
vercel env add CLOUD_TASKS_WORKER_URL production <<< "${WORKER_URL}"
vercel env add CLOUD_TASKS_SERVICE_ACCOUNT_EMAIL production <<< "${TASKS_INVOKER_SA}"
vercel env add GCP_SERVICE_ACCOUNT_JSON production < /tmp/finance-backend-enqueuer.json
```

If Cloud Tasks OIDC + Cloud Run IAM is configured correctly, omit
`CLOUD_TASKS_WORKER_AUTH_TOKEN` entirely.

Redeploy backend after env updates:

```bash
vercel --prod
```

## 7. Authentication verification

Unauthorized request to worker should fail:

```bash
curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST "${WORKER_URL}/jobs/execute" \
  -H "Content-Type: application/json" \
  -d '{"job_type":"ticker_validation","registry_key":"A|us|default","ticker":"A","region":"us"}'
```

Expected: `401/403`.

## 8. Cutover checklist

1. Apply DB migration:
   - `sql/006_async_job_runs.sql`
2. Confirm Prefect deployment set is still 5 recurring-only deployments.
3. Deploy worker and confirm `/health`.
4. Set backend env:
   - `USE_CLOUD_TASKS=true`
   - `CLOUD_TASKS_WORKER_URL=${WORKER_URL}`
   - auth/token vars
5. Verify worker auth (unauthorized call fails).
6. Execute one real onboarding E2E (below).
7. Verify:
   - ticker reaches `ready`
   - `async_job_runs` contains validation + backfill rows
8. Keep rollback prepared.

## 9. Real E2E verification commands

```bash
export BACKEND_URL="https://<finance-backend-production-url>"
export TICKER="ANZ"
export REGION_KEY="apac"
export EXCHANGE_KEY="ASX"
```

Submit request:

```bash
curl -s -X POST "${BACKEND_URL}/tickers/request" \
  -H "Content-Type: application/json" \
  -H "x-shared-secret: ${SHARED_SECRET}" \
  -d "{\"ticker\":\"${TICKER}\",\"region\":\"${REGION_KEY}\",\"exchange\":\"${EXCHANGE_KEY}\"}" | jq
```

Check queue dispatch:

```bash
gcloud tasks list \
  --project "${PROJECT_ID}" \
  --location "${REGION}" \
  --queue "${QUEUE_NAME}" \
  --limit 20
```

Poll status until `ready`:

```bash
watch -n 5 "curl -s \"${BACKEND_URL}/tickers/status?ticker=${TICKER}&region=${REGION_KEY}&exchange=${EXCHANGE_KEY}\" -H \"x-shared-secret: ${SHARED_SECRET}\" | jq"
```

Verify async audit rows in Supabase:

```sql
select job_id, job_type, registry_key, status, attempt, started_at, finished_at
from public.async_job_runs
where registry_key = 'ANZ|apac|ASX'
order by created_at desc;
```

Expected:

- one `ticker_validation` row `completed`
- one `ticker_backfill` row `completed`
- registry status API returns `ready`

## 10. Rollback (if enqueue/delivery fails)

1. Disable Cloud Tasks mode in backend:
   - `USE_CLOUD_TASKS=false`
2. Redeploy backend.
3. Keep worker deployed but do not route new requests to tasks mode.
4. Inspect failed jobs:
   - Cloud Tasks retry logs
   - Cloud Run logs
   - `async_job_runs` failure rows
5. Fix credentials/auth/queue settings, then re-enable:
   - `USE_CLOUD_TASKS=true`
