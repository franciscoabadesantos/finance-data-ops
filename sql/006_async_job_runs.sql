-- Durable audit trail for request-driven async jobs.

create table if not exists public.async_job_runs (
  job_id text primary key,
  job_type text not null,
  registry_key text not null,
  idempotency_key text not null,
  status text not null,
  attempt integer not null default 1,
  payload_hash text,
  error_message text,
  metadata jsonb not null default '{}'::jsonb,
  started_at timestamptz,
  finished_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (job_type in ('ticker_validation', 'ticker_backfill')),
  check (status in ('queued', 'running', 'completed', 'failed'))
);

create index if not exists idx_async_job_runs_registry_created
  on public.async_job_runs (registry_key, created_at desc);

create index if not exists idx_async_job_runs_status_created
  on public.async_job_runs (status, created_at desc);
