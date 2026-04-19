# Secret Consolidation Inventory

Date: 2026-04-19  
Scope: `finance-data-ops`, `finance-backend`, `spy-signal-site`, `spy-signal-backoffice`, `Finance`

## Critical findings

1. Plaintext secrets were committed in tracked files:
   - `finance-data-ops/.env` (Supabase service role key)
   - `finance-data-ops/worker.env.yaml` (Supabase service role key)
2. Token-based Vercel deploy workflows were duplicated across repos.
3. Shared backend auth was split across multiple names (`SHARED_SECRET`, `FINANCE_BACKEND_SHARED_SECRET`).
4. Worker auth used both OIDC and shared bearer token patterns simultaneously.

## Classification table

| Secret / Config | Current locations | Owning runtime | Target source of truth | Rotation impact |
|---|---|---|---|---|
| `SUPABASE_SERVICE_ROLE_KEY` | `finance-data-ops` workflows + Prefect blocks + local `.env`; `finance-backend` env; `spy-signal-site` server env; `Finance` workflows | GCP worker, Prefect jobs, Vercel backends, GH Actions | GCP: Google Secret Manager (`supabase-service-role-key`); Vercel: Shared Environment Variable `SUPABASE_SERVICE_ROLE_KEY`; Prefect: Prefect Secret block synchronized from same canonical value | High: affects all write paths; rotate with staged rollout and smoke tests |
| `SUPABASE_URL` | All repos env/config | All runtimes | Non-secret shared config: Vercel Shared Env + GCP repo vars + Prefect block | Low-medium |
| `SHARED_SECRET` / `FINANCE_BACKEND_SHARED_SECRET` | `finance-backend` + `spy-signal-site` | Backend ticker endpoints + website proxy | Canonical name: `BACKEND_SHARED_SECRET` (Vercel Shared Env), legacy aliases temporary | Medium; ticker request/status auth path |
| `FINANCE_BACKEND_SERVICE_TOKEN` | `spy-signal-backoffice` | Backoffice -> backend analyst API | Canonical name: `BACKEND_SERVICE_TOKEN` (Vercel Shared Env), legacy alias temporary | Medium; analyst API access control |
| `WORKER_SHARED_TOKEN` / `CLOUD_TASKS_WORKER_AUTH_TOKEN` | backend env, worker env, deployment docs | Cloud Tasks -> Cloud Run worker | Prefer no static token (Cloud Tasks OIDC + Cloud Run IAM). Keep optional GSM secret only if app-layer bearer needed | Medium; job execution path |
| `GCP_SA_KEY` (GitHub secret) | `finance-data-ops` deploy workflow | GitHub Actions deployer | Replace with Workload Identity Federation (`GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_DEPLOYER_SERVICE_ACCOUNT`) | Medium-high; deployment auth |
| `GCP_SERVICE_ACCOUNT_JSON` | `finance-backend` env on Vercel | Vercel backend enqueue to Cloud Tasks | Keep as Vercel Shared Env until backend can run on GCP identity; avoid per-project duplication | Medium |
| `CLERK_SECRET_KEY` | `spy-signal-site`, `spy-signal-backoffice` | Vercel server auth | Vercel Shared Env (per environment) | Medium |
| `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` | `spy-signal-site`, `spy-signal-backoffice` | Browser + server boot | Vercel Shared Env (non-secret but shared) | Low |
| `STRIPE_WEBHOOK_SECRET` | `spy-signal-site` | Stripe webhook validation | Vercel Shared Env | Medium |
| `RESEND_API_KEY` | `spy-signal-site` | Email send path | Vercel Shared Env | Medium |
| `PERPLEXITY_API_KEY` | `spy-signal-site` | AI analyst API route | Vercel Shared Env | Medium |
| `SIGNAL_ALERT_CRON_TOKEN` | `spy-signal-site` | cron endpoint auth | Vercel Shared Env | Medium |
| `VERCEL_TOKEN`/`VERCEL_ORG_ID`/`VERCEL_PROJECT_ID` | GitHub repo secrets (backend/backoffice workflows) | GitHub deployment jobs | Remove from repo secrets; use native Vercel Git integration for deploys | Low-medium |

## Consolidation plan (smallest safe order)

1. Remove committed plaintext secrets and replace with templates.
2. Standardize backend auth names:
   - `BACKEND_SHARED_SECRET` (ticker paths)
   - `BACKEND_SERVICE_TOKEN` (analyst paths)
   - keep legacy aliases temporarily.
3. Remove token-based GH->Vercel deployment secrets by converting deploy workflows to CI-only and using native Vercel Git deploy.
4. For GCP worker deploy:
   - switch GH auth from `GCP_SA_KEY` JSON to WIF.
   - inject runtime secrets via Cloud Run `--set-secrets` from GSM.
5. Keep `GCP_SERVICE_ACCOUNT_JSON` only once in Vercel Shared Env until backend enqueue moves to GCP identity-native runtime.
6. Rotate leaked keys (mandatory because plaintext values existed in git history).

## Applied in this pass

- Deleted tracked `finance-data-ops/.env`.
- Deleted tracked `finance-data-ops/worker.env.yaml`.
- Added `finance-data-ops/worker.env.template.yaml`.
- Added ignore rules for `.env` and `worker.env.yaml`.
- Updated `finance-data-ops` worker deploy workflow to:
  - use Workload Identity Federation auth (no `GCP_SA_KEY` JSON in GH secrets),
  - pull `SUPABASE_SERVICE_ROLE_KEY` from Secret Manager via `--set-secrets`,
  - keep optional `WORKER_SHARED_TOKEN` secret.
- Standardized backend auth names in code (with legacy fallback):
  - `BACKEND_SHARED_SECRET`
  - `BACKEND_SERVICE_TOKEN`
- Updated website/backoffice proxies to use canonical names with backward compatibility.
- Converted backend/backoffice GitHub workflows to CI-only (removed Vercel token dependency).

## Required follow-up (operator actions)

1. Rotate leaked `SUPABASE_SERVICE_ROLE_KEY` immediately.
2. Create/refresh GSM secrets:
   - `supabase-service-role-key`
   - optional `worker-shared-token`
3. Set repo vars for worker deploy:
   - `GCP_WORKLOAD_IDENTITY_PROVIDER`
   - `GCP_DEPLOYER_SERVICE_ACCOUNT`
   - `SUPABASE_URL`
   - `GSM_SUPABASE_SERVICE_ROLE_KEY_SECRET`
   - optional: `GSM_WORKER_SHARED_TOKEN_SECRET`, `GCP_WORKER_RUNTIME_SERVICE_ACCOUNT`, `TASKS_INVOKER_SERVICE_ACCOUNT_EMAIL`
4. Move shared Vercel secrets into Vercel Shared Environment Variables:
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `BACKEND_SHARED_SECRET`
   - `BACKEND_SERVICE_TOKEN`
   - `GCP_SERVICE_ACCOUNT_JSON` (temporary until identity-native replacement)
