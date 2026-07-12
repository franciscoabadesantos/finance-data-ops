-- Data Ops v2 schema surfaces (fundamentals + earnings).
-- Ownership: finance-data-ops fetches/normalizes/publishes these surfaces for frontend consumption.
-- Migration strategy: additive + idempotent; no destructive changes to existing v1 surfaces.

create extension if not exists pgcrypto;

-- Canonical fundamentals history (normalized metric rows).
create table if not exists public.market_fundamentals_v2 (
  ticker text not null,
  metric text not null,
  value double precision,
  period_end date not null,
  period_type text not null default 'unknown',
  fiscal_year integer,
  fiscal_quarter text,
  currency text,
  source text,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (ticker, metric, period_end, period_type)
);

alter table if exists public.market_fundamentals_v2
  add column if not exists ticker text;
alter table if exists public.market_fundamentals_v2
  add column if not exists metric text;
alter table if exists public.market_fundamentals_v2
  add column if not exists value double precision;
alter table if exists public.market_fundamentals_v2
  add column if not exists period_end date;
alter table if exists public.market_fundamentals_v2
  add column if not exists period_type text;
alter table if exists public.market_fundamentals_v2
  add column if not exists fiscal_year integer;
alter table if exists public.market_fundamentals_v2
  add column if not exists fiscal_quarter text;
alter table if exists public.market_fundamentals_v2
  add column if not exists currency text;
alter table if exists public.market_fundamentals_v2
  add column if not exists source text;
alter table if exists public.market_fundamentals_v2
  add column if not exists fetched_at timestamptz not null default now();
alter table if exists public.market_fundamentals_v2
  add column if not exists created_at timestamptz not null default now();
alter table if exists public.market_fundamentals_v2
  add column if not exists updated_at timestamptz not null default now();

create index if not exists idx_market_fundamentals_v2_ticker_period
  on public.market_fundamentals_v2 (ticker, period_end desc);
create index if not exists idx_market_fundamentals_v2_metric_period
  on public.market_fundamentals_v2 (metric, period_end desc);

-- Frontend-serving latest-per-(ticker,metric) snapshot.
create materialized view if not exists public.mv_latest_fundamentals as
select
  ranked.ticker,
  ranked.metric,
  ranked.value,
  ranked.period_end,
  ranked.period_type,
  ranked.fiscal_year,
  ranked.fiscal_quarter,
  ranked.currency,
  ranked.source,
  ranked.fetched_at,
  ranked.updated_at
from (
  select
    f.*,
    row_number() over (
      partition by f.ticker, f.metric
      order by f.period_end desc nulls last, f.fetched_at desc nulls last, f.updated_at desc nulls last
    ) as row_num
  from public.market_fundamentals_v2 f
) ranked
where ranked.row_num = 1;

create unique index if not exists idx_mv_latest_fundamentals_ticker_metric
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

-- Frontend-serving denormalized fundamentals summary table.
create table if not exists public.ticker_fundamental_summary (
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

alter table if exists public.ticker_fundamental_summary
  add column if not exists ticker text;
alter table if exists public.ticker_fundamental_summary
  add column if not exists latest_revenue double precision;
alter table if exists public.ticker_fundamental_summary
  add column if not exists latest_eps double precision;
alter table if exists public.ticker_fundamental_summary
  add column if not exists trailing_pe double precision;
alter table if exists public.ticker_fundamental_summary
  add column if not exists market_cap double precision;
alter table if exists public.ticker_fundamental_summary
  add column if not exists revenue_growth_yoy double precision;
alter table if exists public.ticker_fundamental_summary
  add column if not exists earnings_growth_yoy double precision;
alter table if exists public.ticker_fundamental_summary
  add column if not exists latest_period_end date;
alter table if exists public.ticker_fundamental_summary
  add column if not exists source text;
alter table if exists public.ticker_fundamental_summary
  add column if not exists updated_at timestamptz not null default now();

create index if not exists idx_ticker_fundamental_summary_updated_at
  on public.ticker_fundamental_summary (updated_at desc);

-- Upcoming/scheduled earnings events.
create table if not exists public.market_earnings_events (
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

alter table if exists public.market_earnings_events
  add column if not exists ticker text;
alter table if exists public.market_earnings_events
  add column if not exists earnings_date date;
alter table if exists public.market_earnings_events
  add column if not exists earnings_time text;
alter table if exists public.market_earnings_events
  add column if not exists fiscal_period text;
alter table if exists public.market_earnings_events
  add column if not exists estimate_eps double precision;
alter table if exists public.market_earnings_events
  add column if not exists estimate_revenue double precision;
alter table if exists public.market_earnings_events
  add column if not exists source text;
alter table if exists public.market_earnings_events
  add column if not exists fetched_at timestamptz not null default now();
alter table if exists public.market_earnings_events
  add column if not exists created_at timestamptz not null default now();
alter table if exists public.market_earnings_events
  add column if not exists updated_at timestamptz not null default now();

create index if not exists idx_market_earnings_events_date
  on public.market_earnings_events (earnings_date asc);

-- Historical earnings results.
create table if not exists public.market_earnings_history (
  ticker text not null,
  earnings_date date not null,
  fiscal_period text not null default 'unknown',
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
  primary key (ticker, earnings_date, fiscal_period)
);

alter table if exists public.market_earnings_history
  add column if not exists ticker text;
alter table if exists public.market_earnings_history
  add column if not exists earnings_date date;
alter table if exists public.market_earnings_history
  add column if not exists fiscal_period text;
alter table if exists public.market_earnings_history
  add column if not exists actual_eps double precision;
alter table if exists public.market_earnings_history
  add column if not exists estimate_eps double precision;
alter table if exists public.market_earnings_history
  add column if not exists surprise_eps double precision;
alter table if exists public.market_earnings_history
  add column if not exists actual_revenue double precision;
alter table if exists public.market_earnings_history
  add column if not exists estimate_revenue double precision;
alter table if exists public.market_earnings_history
  add column if not exists surprise_revenue double precision;
alter table if exists public.market_earnings_history
  add column if not exists source text;
alter table if exists public.market_earnings_history
  add column if not exists fetched_at timestamptz not null default now();
alter table if exists public.market_earnings_history
  add column if not exists created_at timestamptz not null default now();
alter table if exists public.market_earnings_history
  add column if not exists updated_at timestamptz not null default now();

create index if not exists idx_market_earnings_history_date
  on public.market_earnings_history (earnings_date desc);

-- Frontend-serving next-earnings snapshot.
create materialized view if not exists public.mv_next_earnings as
select
  ranked.ticker,
  ranked.earnings_date,
  ranked.earnings_time,
  ranked.fiscal_period,
  ranked.estimate_eps,
  ranked.estimate_revenue,
  ranked.source,
  ranked.fetched_at,
  ranked.updated_at
from (
  select
    e.*,
    row_number() over (
      partition by e.ticker
      order by e.earnings_date asc nulls last, e.fetched_at desc nulls last
    ) as row_num
  from public.market_earnings_events e
  where e.earnings_date >= current_date
) ranked
where ranked.row_num = 1;

create unique index if not exists idx_mv_next_earnings_ticker
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

-- Coverage/status compatibility for v2 domains.
alter table if exists public.symbol_data_coverage
  add column if not exists fundamentals_available boolean not null default false;
alter table if exists public.symbol_data_coverage
  add column if not exists fundamentals_last_date date;
alter table if exists public.symbol_data_coverage
  add column if not exists earnings_available boolean not null default false;
alter table if exists public.symbol_data_coverage
  add column if not exists next_earnings_date date;

-- Ensure run-id upsert compatibility for operational run logging.
alter table if exists public.data_source_runs
  add column if not exists run_id text;

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'data_source_runs'
      and column_name = 'id'
  ) and exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'data_source_runs'
      and column_name = 'run_id'
  ) then
    execute '
      update public.data_source_runs
      set run_id = coalesce(run_id, id::text)
      where run_id is null
    ';
  end if;
end $$;

create unique index if not exists idx_data_source_runs_run_id
  on public.data_source_runs (run_id)
  where run_id is not null;
