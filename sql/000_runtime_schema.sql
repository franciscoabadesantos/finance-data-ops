-- Definitive runtime baseline for fresh Supabase projects.
-- Apply this to an empty project instead of replaying historical migrations.

create table public.market_price_daily (
  ticker text not null,
  symbol text generated always as (ticker) stored,
  date date not null,
  as_of_date date generated always as ("date") stored,
  close double precision not null,
  source text,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  primary key (ticker, date)
);

create index idx_market_price_daily_date
  on public.market_price_daily (date desc);

create index idx_market_price_daily_symbol_date
  on public.market_price_daily (symbol, date desc);


create table public.market_quotes (
  ticker text primary key,
  symbol text generated always as (ticker) stored,
  name text,
  price double precision,
  change double precision,
  change_percent double precision,
  market_cap_text text,
  source text,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index idx_market_quotes_updated_at
  on public.market_quotes (updated_at desc);


create table public.market_quotes_history (
  ticker text not null,
  symbol text generated always as (ticker) stored,
  fetched_at timestamptz not null,
  price double precision,
  change double precision,
  change_percent double precision,
  market_cap double precision,
  source text,
  primary key (ticker, fetched_at)
);

create index idx_market_quotes_history_fetched_at
  on public.market_quotes_history (fetched_at desc);


create materialized view public.mv_latest_prices as
select
  q.ticker,
  q.symbol,
  q.name,
  q.price,
  q.change,
  q.change_percent,
  q.market_cap_text,
  q.source,
  q.fetched_at,
  q.created_at,
  q.updated_at,
  latest_daily.date as latest_price_date,
  latest_daily.as_of_date,
  latest_daily.close as latest_close
from public.market_quotes q
left join lateral (
  select
    d.date,
    d.as_of_date,
    d.close
  from public.market_price_daily d
  where d.ticker = q.ticker
  order by d.date desc, d.created_at desc
  limit 1
) latest_daily on true;

create unique index idx_mv_latest_prices_ticker
  on public.mv_latest_prices (ticker);


create or replace function public.refresh_mv_latest_prices()
returns void
language plpgsql
security definer
as $$
begin
  refresh materialized view public.mv_latest_prices;
end;
$$;


create table public.ticker_market_stats_snapshot (
  ticker text primary key,
  as_of_date date not null,
  last_price double precision,
  return_1d_pct double precision,
  return_1m_pct double precision,
  return_3m_pct double precision,
  return_1y_pct double precision,
  vol_30d_pct double precision,
  drawdown_1y_pct double precision,
  dist_from_52w_high_pct double precision,
  dist_from_52w_low_pct double precision,
  updated_at timestamptz not null default now()
);

create index idx_ticker_market_stats_snapshot_as_of_date
  on public.ticker_market_stats_snapshot (as_of_date desc);


create table public.data_source_runs (
  run_id text primary key,
  job_name text not null,
  source_type text not null,
  scope text not null,
  status text not null,
  started_at timestamptz,
  finished_at timestamptz,
  rows_written integer not null default 0,
  error_class text,
  error_message text,
  failure_classification text,
  symbols_requested integer,
  symbols_succeeded integer,
  symbols_failed integer,
  error_messages jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now()
);

create index idx_data_source_runs_started_at
  on public.data_source_runs (started_at desc);

create index idx_data_source_runs_status_started_at
  on public.data_source_runs (status, started_at desc);


create table public.data_asset_status (
  asset_key text primary key,
  asset_type text not null,
  provider text,
  last_success_at timestamptz,
  last_available_date date,
  freshness_status text not null,
  coverage_status text not null,
  reason text,
  updated_at timestamptz not null default now()
);

create index idx_data_asset_status_updated_at
  on public.data_asset_status (updated_at desc);


create table public.symbol_data_coverage (
  ticker text primary key,
  market_data_available boolean not null default false,
  fundamentals_available boolean not null default false,
  earnings_available boolean not null default false,
  signal_available boolean not null default false,
  market_data_last_date date,
  fundamentals_last_date date,
  next_earnings_date date,
  coverage_status text not null,
  reason text,
  updated_at timestamptz not null default now()
);

create index idx_symbol_data_coverage_status_updated
  on public.symbol_data_coverage (coverage_status, updated_at desc);


create table public.market_fundamentals_v2 (
  ticker text not null,
  period text not null,
  period_end date not null,
  metric text not null,
  value double precision not null,
  value_text text,
  source text,
  fetched_at timestamptz not null default now(),
  primary key (ticker, period, period_end, metric)
);

create index idx_market_fundamentals_v2_period_end
  on public.market_fundamentals_v2 (period_end desc);

create index idx_market_fundamentals_v2_metric
  on public.market_fundamentals_v2 (metric, period_end desc);


create materialized view public.mv_latest_fundamentals as
select
  ranked.ticker,
  ranked.period,
  ranked.period_end,
  ranked.metric,
  ranked.value,
  ranked.value_text,
  ranked.source,
  ranked.fetched_at
from (
  select
    f.*,
    row_number() over (
      partition by f.ticker, f.metric
      order by f.period_end desc, f.fetched_at desc, f.period desc
    ) as row_num
  from public.market_fundamentals_v2 f
) ranked
where ranked.row_num = 1;

create unique index idx_mv_latest_fundamentals_ticker_metric
  on public.mv_latest_fundamentals (ticker, metric);


create or replace function public.refresh_mv_latest_fundamentals()
returns void
language plpgsql
security definer
as $$
begin
  refresh materialized view public.mv_latest_fundamentals;
end;
$$;


create table public.ticker_fundamental_summary (
  ticker text primary key,
  latest_revenue double precision,
  latest_eps double precision,
  trailing_pe double precision,
  market_cap double precision,
  revenue_growth_yoy double precision,
  earnings_growth_yoy double precision,
  latest_period_end date,
  source text,
  updated_at timestamptz not null default now()
);


create table public.market_earnings_events (
  ticker text not null,
  earnings_date date not null,
  earnings_time text,
  fiscal_period text,
  estimate_eps double precision,
  estimate_revenue double precision,
  source text,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (ticker, earnings_date)
);

create index idx_market_earnings_events_date
  on public.market_earnings_events (earnings_date desc);


create table public.market_earnings_history (
  ticker text not null,
  earnings_date date not null,
  fiscal_period text,
  actual_eps double precision,
  estimate_eps double precision,
  surprise_eps double precision,
  actual_revenue double precision,
  estimate_revenue double precision,
  surprise_revenue double precision,
  source text,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (ticker, earnings_date)
);

create index idx_market_earnings_history_date
  on public.market_earnings_history (earnings_date desc);


create materialized view public.mv_next_earnings as
select
  ranked.ticker,
  ranked.earnings_date,
  ranked.earnings_time,
  ranked.fiscal_period,
  ranked.estimate_eps,
  ranked.estimate_revenue,
  ranked.source,
  ranked.fetched_at,
  ranked.created_at,
  ranked.updated_at
from (
  select
    e.*,
    row_number() over (
      partition by e.ticker
      order by e.earnings_date asc, e.updated_at desc, e.fetched_at desc
    ) as row_num
  from public.market_earnings_events e
  where e.earnings_date >= current_date
) ranked
where ranked.row_num = 1;

create unique index idx_mv_next_earnings_ticker
  on public.mv_next_earnings (ticker);


create or replace function public.refresh_mv_next_earnings()
returns void
language plpgsql
security definer
as $$
begin
  refresh materialized view public.mv_next_earnings;
end;
$$;


create table public.ticker_registry (
  registry_key text primary key,
  input_symbol text not null,
  normalized_symbol text,
  region text not null default 'us',
  exchange text,
  instrument_type text not null default 'unknown',
  status text not null default 'pending_validation',
  market_supported boolean not null default false,
  fundamentals_supported boolean not null default false,
  earnings_supported boolean not null default false,
  validation_status text not null default 'pending_validation',
  validation_reason text not null default 'pending_validation',
  promotion_status text not null default 'pending_validation',
  last_validated_at timestamptz,
  notes jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  check (instrument_type in ('equity', 'adr', 'etf', 'index_proxy', 'country_fund', 'unknown')),
  check (status in ('pending_validation', 'active', 'rejected')),
  check (validation_status in ('pending_validation', 'validated_market_only', 'validated_full', 'rejected')),
  check (promotion_status in ('pending_validation', 'validated_market_only', 'validated_full', 'rejected'))
);

create index idx_ticker_registry_region_status
  on public.ticker_registry (region, validation_status, promotion_status);

create unique index idx_ticker_registry_input_scope
  on public.ticker_registry (input_symbol, region, coalesce(exchange, ''));


create table public.macro_series_catalog (
  series_key text primary key,
  source_provider text not null,
  source_code text not null,
  frequency text not null,
  required_by_default boolean not null default false,
  required_from_date date,
  optional boolean not null default false,
  staleness_max_bdays integer not null,
  release_calendar_source text,
  description text,
  updated_at timestamptz not null default now(),
  check (frequency in ('daily', 'weekly', 'monthly'))
);

create index idx_macro_series_catalog_required
  on public.macro_series_catalog (required_by_default desc, series_key);


create table public.macro_observations (
  series_key text not null references public.macro_series_catalog(series_key) on delete cascade,
  observation_period text not null,
  observation_date date not null,
  frequency text not null,
  value double precision not null,
  source_provider text not null,
  source_code text not null,
  release_timestamp_utc timestamptz,
  release_timezone text,
  release_date_local date,
  release_calendar_source text,
  source text,
  fetched_at timestamptz not null default now(),
  ingested_at timestamptz not null default now(),
  primary key (series_key, observation_period),
  check (frequency in ('daily', 'weekly', 'monthly'))
);

create index idx_macro_observations_observation_date
  on public.macro_observations (observation_date desc);

create index idx_macro_observations_release_timestamp
  on public.macro_observations (release_timestamp_utc desc);


create table public.macro_daily (
  as_of_date date not null,
  series_key text not null references public.macro_series_catalog(series_key) on delete cascade,
  value double precision,
  source_observation_period text not null,
  source_observation_date date,
  available_at_utc timestamptz,
  staleness_bdays integer,
  is_stale boolean not null default false,
  alignment_mode text not null,
  ingested_at timestamptz not null default now(),
  primary key (as_of_date, series_key)
);

create index idx_macro_daily_series_date
  on public.macro_daily (series_key, as_of_date desc);


create materialized view public.mv_latest_macro_observations as
select
  ranked.series_key,
  ranked.observation_period,
  ranked.observation_date,
  ranked.frequency,
  ranked.value,
  ranked.source_provider,
  ranked.source_code,
  ranked.release_timestamp_utc,
  ranked.release_timezone,
  ranked.release_date_local,
  ranked.release_calendar_source,
  ranked.source,
  ranked.fetched_at,
  ranked.ingested_at
from (
  select
    m.*,
    row_number() over (
      partition by m.series_key
      order by m.observation_date desc, m.release_timestamp_utc desc nulls last, m.ingested_at desc
    ) as row_num
  from public.macro_observations m
) ranked
where ranked.row_num = 1;

create unique index idx_mv_latest_macro_observations_series_key
  on public.mv_latest_macro_observations (series_key);


create or replace function public.refresh_mv_latest_macro_observations()
returns void
language plpgsql
security definer
as $$
begin
  refresh materialized view public.mv_latest_macro_observations;
end;
$$;


create table public.economic_release_calendar (
  series_key text not null references public.macro_series_catalog(series_key) on delete cascade,
  observation_period text not null,
  observation_date date not null,
  scheduled_release_timestamp_utc timestamptz not null,
  observed_first_available_at_utc timestamptz,
  availability_status text not null,
  availability_source text not null,
  delay_vs_schedule_seconds bigint,
  is_schedule_based_only boolean not null,
  release_timestamp_utc timestamptz not null,
  release_timezone text not null,
  release_date_local date not null,
  release_calendar_source text,
  source text,
  provenance_class text,
  ingested_at timestamptz not null default now(),
  primary key (series_key, observation_period),
  check (release_timestamp_utc = scheduled_release_timestamp_utc),
  check (
    availability_status in (
      'observed_available',
      'scheduled_provisional',
      'late_missing_observation',
      'schedule_only_unsupported_history'
    )
  ),
  check (
    (
      observed_first_available_at_utc is null
      and is_schedule_based_only = true
      and delay_vs_schedule_seconds is null
      and availability_status in (
        'scheduled_provisional',
        'late_missing_observation',
        'schedule_only_unsupported_history'
      )
    )
    or (
      observed_first_available_at_utc is not null
      and is_schedule_based_only = false
      and availability_status = 'observed_available'
      and delay_vs_schedule_seconds is not null
    )
  )
);

create index idx_economic_release_calendar_scheduled_release
  on public.economic_release_calendar (scheduled_release_timestamp_utc desc);

create index idx_economic_release_calendar_observed_release
  on public.economic_release_calendar (observed_first_available_at_utc desc);

create index idx_economic_release_calendar_effective_release
  on public.economic_release_calendar (
    coalesce(observed_first_available_at_utc, scheduled_release_timestamp_utc) desc
  );

comment on column public.economic_release_calendar.release_timestamp_utc is
  'Backward-compatible alias of scheduled_release_timestamp_utc. Use observed_first_available_at_utc for actual observed availability.';

comment on column public.economic_release_calendar.observed_first_available_at_utc is
  'First observed availability timestamp. This is the source of truth for actual release availability when present.';


create materialized view public.mv_latest_economic_release_calendar as
select
  ranked.series_key,
  ranked.observation_period,
  ranked.observation_date,
  ranked.scheduled_release_timestamp_utc,
  ranked.observed_first_available_at_utc,
  coalesce(ranked.observed_first_available_at_utc, ranked.scheduled_release_timestamp_utc) as effective_available_at_utc,
  ranked.availability_status,
  ranked.availability_source,
  ranked.delay_vs_schedule_seconds,
  ranked.is_schedule_based_only,
  ranked.release_timestamp_utc,
  ranked.release_timezone,
  ranked.release_date_local,
  ranked.release_calendar_source,
  ranked.source,
  ranked.provenance_class,
  ranked.ingested_at
from (
  select
    e.*,
    row_number() over (
      partition by e.series_key
      order by coalesce(e.observed_first_available_at_utc, e.scheduled_release_timestamp_utc) desc, e.ingested_at desc
    ) as row_num
  from public.economic_release_calendar e
) ranked
where ranked.row_num = 1;

create unique index idx_mv_latest_economic_release_calendar_series_key
  on public.mv_latest_economic_release_calendar (series_key);


create or replace function public.refresh_mv_latest_economic_release_calendar()
returns void
language plpgsql
security definer
as $$
begin
  refresh materialized view public.mv_latest_economic_release_calendar;
end;
$$;


create table public.async_job_runs (
  job_id text primary key,
  job_type text not null,
  registry_key text not null default '',
  idempotency_key text not null,
  status text not null,
  attempt integer not null default 1,
  payload_hash text not null,
  started_at timestamptz,
  finished_at timestamptz,
  error_message text,
  metadata jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  check (status in ('queued', 'running', 'completed', 'failed', 'cancelled', 'skipped'))
);

create index idx_async_job_runs_registry_updated
  on public.async_job_runs (registry_key, updated_at desc);

create index idx_async_job_runs_type_status_updated
  on public.async_job_runs (job_type, status, updated_at desc);


create table public.analysis_jobs (
  job_id text primary key,
  user_id text,
  ticker text not null,
  region text,
  exchange text,
  analysis_type text not null,
  status text not null,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  finished_at timestamptz,
  error_message text,
  worker_job_id text,
  result_ref text,
  check (status in ('queued', 'running', 'completed', 'failed'))
);

create index idx_analysis_jobs_ticker_created
  on public.analysis_jobs (ticker, created_at desc);

create index idx_analysis_jobs_status_created
  on public.analysis_jobs (status, created_at desc);

create index idx_analysis_jobs_type_created
  on public.analysis_jobs (analysis_type, created_at desc);


create table public.analysis_results (
  job_id text primary key references public.analysis_jobs(job_id) on delete cascade,
  analysis_type text not null,
  result_json jsonb not null,
  summary_text text,
  created_at timestamptz not null default now()
);

create index idx_analysis_results_type_created
  on public.analysis_results (analysis_type, created_at desc);
