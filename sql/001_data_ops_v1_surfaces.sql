-- Data Ops v1 schema surfaces aligned with production operational tables.
-- Existing tables/views (market_price_daily, market_quotes, market_quotes_history, mv_latest_prices)
-- remain owned by Data Ops; definitions here only cover v1 additions and operational compatibility.

create extension if not exists pgcrypto;

create table if not exists public.ticker_market_stats_snapshot (
  symbol text not null,
  as_of_date date not null,
  latest_price double precision,
  return_1d double precision,
  return_1m double precision,
  return_3m double precision,
  return_1y double precision,
  volatility_30d double precision,
  drawdown_1y double precision,
  distance_from_52w_high double precision,
  distance_from_52w_low double precision,
  updated_at timestamptz not null default now(),
  primary key (symbol, as_of_date)
);

create table if not exists public.data_source_runs (
  id uuid primary key default gen_random_uuid(),
  job_name text not null,
  source_type text not null,
  scope text not null,
  started_at timestamptz not null,
  finished_at timestamptz,
  status text not null,
  rows_written integer not null default 0,
  error_class text,
  error_message text,
  failure_classification text,
  symbols_requested jsonb not null default '[]'::jsonb,
  symbols_succeeded jsonb not null default '[]'::jsonb,
  symbols_failed jsonb not null default '[]'::jsonb,
  error_messages jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.data_asset_status (
  asset_key text primary key,
  asset_type text not null,
  provider text,
  last_success_at timestamptz,
  last_available_date date,
  freshness_status text not null,
  coverage_status text not null default 'unknown',
  reason text not null default 'unknown',
  updated_at timestamptz not null default now()
);

create table if not exists public.symbol_data_coverage (
  ticker text primary key,
  market_data_available boolean not null default false,
  fundamentals_available boolean not null default false,
  earnings_available boolean not null default false,
  signal_available boolean not null default false,
  market_data_last_date date,
  fundamentals_last_date date,
  next_earnings_date date,
  coverage_status text not null,
  reason text not null default 'unknown',
  updated_at timestamptz not null default now()
);

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'data_source_runs'
      and column_name = 'asset_name'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'data_source_runs'
      and column_name = 'job_name'
  ) then
    execute 'alter table public.data_source_runs rename column asset_name to job_name';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'data_source_runs'
      and column_name = 'ended_at'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'data_source_runs'
      and column_name = 'finished_at'
  ) then
    execute 'alter table public.data_source_runs rename column ended_at to finished_at';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'data_asset_status'
      and column_name = 'asset_name'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'data_asset_status'
      and column_name = 'asset_key'
  ) then
    execute 'alter table public.data_asset_status rename column asset_name to asset_key';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'data_asset_status'
      and column_name = 'last_observed_at'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'data_asset_status'
      and column_name = 'last_success_at'
  ) then
    execute 'alter table public.data_asset_status rename column last_observed_at to last_success_at';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'symbol_data_coverage'
      and column_name = 'symbol'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'symbol_data_coverage'
      and column_name = 'ticker'
  ) then
    execute 'alter table public.symbol_data_coverage rename column symbol to ticker';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'symbol_data_coverage'
      and column_name = 'has_market_price_daily'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'symbol_data_coverage'
      and column_name = 'market_data_available'
  ) then
    execute 'alter table public.symbol_data_coverage rename column has_market_price_daily to market_data_available';
  end if;
end $$;

alter table if exists public.data_source_runs
  add column if not exists job_name text;
alter table if exists public.data_source_runs
  add column if not exists source_type text;
alter table if exists public.data_source_runs
  add column if not exists scope text;
alter table if exists public.data_source_runs
  add column if not exists started_at timestamptz;
alter table if exists public.data_source_runs
  add column if not exists finished_at timestamptz;
alter table if exists public.data_source_runs
  add column if not exists status text;
alter table if exists public.data_source_runs
  add column if not exists rows_written integer not null default 0;
alter table if exists public.data_source_runs
  add column if not exists error_class text;
alter table if exists public.data_source_runs
  add column if not exists error_message text;
alter table if exists public.data_source_runs
  add column if not exists failure_classification text;
alter table if exists public.data_source_runs
  add column if not exists symbols_requested jsonb not null default '[]'::jsonb;
alter table if exists public.data_source_runs
  add column if not exists symbols_succeeded jsonb not null default '[]'::jsonb;
alter table if exists public.data_source_runs
  add column if not exists symbols_failed jsonb not null default '[]'::jsonb;
alter table if exists public.data_source_runs
  add column if not exists error_messages jsonb not null default '[]'::jsonb;
alter table if exists public.data_source_runs
  add column if not exists created_at timestamptz not null default now();

alter table if exists public.data_asset_status
  add column if not exists asset_key text;
alter table if exists public.data_asset_status
  add column if not exists asset_type text;
alter table if exists public.data_asset_status
  add column if not exists provider text;
alter table if exists public.data_asset_status
  add column if not exists last_success_at timestamptz;
alter table if exists public.data_asset_status
  add column if not exists last_available_date date;
alter table if exists public.data_asset_status
  add column if not exists freshness_status text;
alter table if exists public.data_asset_status
  add column if not exists coverage_status text not null default 'unknown';
alter table if exists public.data_asset_status
  add column if not exists reason text not null default 'unknown';
alter table if exists public.data_asset_status
  add column if not exists updated_at timestamptz not null default now();

alter table if exists public.symbol_data_coverage
  add column if not exists ticker text;
alter table if exists public.symbol_data_coverage
  add column if not exists market_data_available boolean not null default false;
alter table if exists public.symbol_data_coverage
  add column if not exists fundamentals_available boolean not null default false;
alter table if exists public.symbol_data_coverage
  add column if not exists earnings_available boolean not null default false;
alter table if exists public.symbol_data_coverage
  add column if not exists signal_available boolean not null default false;
alter table if exists public.symbol_data_coverage
  add column if not exists market_data_last_date date;
alter table if exists public.symbol_data_coverage
  add column if not exists fundamentals_last_date date;
alter table if exists public.symbol_data_coverage
  add column if not exists next_earnings_date date;
alter table if exists public.symbol_data_coverage
  add column if not exists coverage_status text;
alter table if exists public.symbol_data_coverage
  add column if not exists reason text not null default 'unknown';
alter table if exists public.symbol_data_coverage
  add column if not exists updated_at timestamptz not null default now();

do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'data_source_runs' and column_name = 'finished_at'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'data_source_runs' and column_name = 'ended_at'
  ) then
    execute 'update public.data_source_runs set finished_at = coalesce(finished_at, ended_at) where finished_at is null';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'data_source_runs' and column_name = 'error_messages'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'data_source_runs' and column_name = 'error_message'
  ) then
    execute '
      update public.data_source_runs
      set error_message = coalesce(error_message, error_messages ->> 0)
      where error_message is null
        and jsonb_typeof(error_messages) = ''array''
        and jsonb_array_length(error_messages) > 0
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'data_source_runs' and column_name = 'error_messages'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'data_source_runs' and column_name = 'error_message'
  ) then
    execute '
      update public.data_source_runs
      set error_messages = jsonb_build_array(error_message)
      where (error_messages is null or error_messages = ''[]''::jsonb)
        and error_message is not null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'data_asset_status' and column_name = 'as_of_date'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'data_asset_status' and column_name = 'last_available_date'
  ) then
    execute '
      update public.data_asset_status
      set last_available_date = coalesce(last_available_date, as_of_date)
      where last_available_date is null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'data_asset_status' and column_name = 'freshness_status'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'data_asset_status' and column_name = 'coverage_status'
  ) then
    execute '
      update public.data_asset_status
      set coverage_status = coalesce(nullif(coverage_status, ''''), freshness_status, ''unknown'')
      where coverage_status is null or coverage_status = ''''
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'symbol_data_coverage' and column_name = 'latest_market_date'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'symbol_data_coverage' and column_name = 'market_data_last_date'
  ) then
    execute '
      update public.symbol_data_coverage
      set market_data_last_date = coalesce(market_data_last_date, latest_market_date::date)
      where market_data_last_date is null
        and latest_market_date is not null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'symbol_data_coverage' and column_name = 'latest_quote_ts'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'symbol_data_coverage' and column_name = 'market_data_last_date'
  ) then
    execute '
      update public.symbol_data_coverage
      set market_data_last_date = coalesce(market_data_last_date, latest_quote_ts::date)
      where market_data_last_date is null
        and latest_quote_ts is not null
    ';
  end if;
end $$;

create index if not exists idx_data_source_runs_job_started_at
  on public.data_source_runs (job_name, started_at desc);

create index if not exists idx_data_asset_status_freshness_status
  on public.data_asset_status (freshness_status);

create index if not exists idx_data_asset_status_coverage_status
  on public.data_asset_status (coverage_status);

create index if not exists idx_symbol_data_coverage_status
  on public.symbol_data_coverage (coverage_status);
