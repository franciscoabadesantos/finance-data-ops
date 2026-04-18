begin;

create table if not exists public.analysis_jobs (
  job_id text primary key,
  user_id text null,
  ticker text not null,
  region text null,
  exchange text null,
  analysis_type text not null,
  status text not null check (status in ('queued', 'running', 'completed', 'failed')),
  created_at timestamptz not null default now(),
  started_at timestamptz null,
  finished_at timestamptz null,
  error_message text null,
  worker_job_id text null,
  result_ref text null
);

create index if not exists idx_analysis_jobs_ticker_created
  on public.analysis_jobs (ticker, created_at desc);

create index if not exists idx_analysis_jobs_status_created
  on public.analysis_jobs (status, created_at desc);

create index if not exists idx_analysis_jobs_type_created
  on public.analysis_jobs (analysis_type, created_at desc);

create table if not exists public.analysis_results (
  job_id text primary key references public.analysis_jobs(job_id) on delete cascade,
  analysis_type text not null,
  result_json jsonb not null,
  summary_text text null,
  created_at timestamptz not null default now()
);

create index if not exists idx_analysis_results_type_created
  on public.analysis_results (analysis_type, created_at desc);

commit;
