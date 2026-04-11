-- Data Ops v1 schema surfaces aligned with production operational tables.
-- Existing tables/views (market_price_daily, market_quotes, market_quotes_history, mv_latest_prices)
-- remain owned by Data Ops; definitions here only cover v1 additions and operational compatibility.

create extension if not exists pgcrypto;

create table if not exists public.ticker_market_stats_snapshot (
  ticker text not null,
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
  updated_at timestamptz not null default now(),
  primary key (ticker)
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
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'symbol'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'ticker'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot rename column symbol to ticker';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'latest_price'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'last_price'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot rename column latest_price to last_price';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'return_1d'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'return_1d_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot rename column return_1d to return_1d_pct';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'return_1m'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'return_1m_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot rename column return_1m to return_1m_pct';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'return_3m'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'return_3m_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot rename column return_3m to return_3m_pct';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'return_1y'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'return_1y_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot rename column return_1y to return_1y_pct';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'volatility_30d'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'vol_30d_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot rename column volatility_30d to vol_30d_pct';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'drawdown_1y'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'drawdown_1y_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot rename column drawdown_1y to drawdown_1y_pct';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'distance_from_52w_high'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'dist_from_52w_high_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot rename column distance_from_52w_high to dist_from_52w_high_pct';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'distance_from_52w_low'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'ticker_market_stats_snapshot'
      and column_name = 'dist_from_52w_low_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot rename column distance_from_52w_low to dist_from_52w_low_pct';
  end if;

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

alter table if exists public.ticker_market_stats_snapshot
  add column if not exists ticker text;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists as_of_date date;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists last_price double precision;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists return_1d_pct double precision;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists return_1m_pct double precision;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists return_3m_pct double precision;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists return_1y_pct double precision;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists vol_30d_pct double precision;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists drawdown_1y_pct double precision;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists dist_from_52w_high_pct double precision;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists dist_from_52w_low_pct double precision;
alter table if exists public.ticker_market_stats_snapshot
  add column if not exists updated_at timestamptz not null default now();

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
declare
  ticker_stats_pk text;
begin
  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'ticker'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set ticker = nullif(upper(trim(ticker)), '''')
      where ticker is not null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'ticker'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'symbol'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set ticker = coalesce(nullif(ticker, ''''), nullif(upper(trim(symbol)), ''''))
      where ticker is null or ticker = ''''
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'last_price'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'latest_price'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set last_price = coalesce(last_price, latest_price)
      where last_price is null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1d_pct'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1d'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set return_1d_pct = coalesce(return_1d_pct, return_1d)
      where return_1d_pct is null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1m_pct'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1m'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set return_1m_pct = coalesce(return_1m_pct, return_1m)
      where return_1m_pct is null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_3m_pct'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_3m'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set return_3m_pct = coalesce(return_3m_pct, return_3m)
      where return_3m_pct is null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1y_pct'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1y'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set return_1y_pct = coalesce(return_1y_pct, return_1y)
      where return_1y_pct is null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'vol_30d_pct'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'volatility_30d'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set vol_30d_pct = coalesce(vol_30d_pct, volatility_30d)
      where vol_30d_pct is null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'drawdown_1y_pct'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'drawdown_1y'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set drawdown_1y_pct = coalesce(drawdown_1y_pct, drawdown_1y)
      where drawdown_1y_pct is null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'dist_from_52w_high_pct'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'distance_from_52w_high'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set dist_from_52w_high_pct = coalesce(dist_from_52w_high_pct, distance_from_52w_high)
      where dist_from_52w_high_pct is null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'dist_from_52w_low_pct'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'distance_from_52w_low'
  ) then
    execute '
      update public.ticker_market_stats_snapshot
      set dist_from_52w_low_pct = coalesce(dist_from_52w_low_pct, distance_from_52w_low)
      where dist_from_52w_low_pct is null
    ';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'ticker'
  ) then
    execute '
      delete from public.ticker_market_stats_snapshot
      where ticker is null or ticker = ''''
    ';

    execute '
      with ranked as (
        select
          ctid,
          row_number() over (
            partition by ticker
            order by as_of_date desc nulls last, updated_at desc nulls last
          ) as row_num
        from public.ticker_market_stats_snapshot
      )
      delete from public.ticker_market_stats_snapshot target
      using ranked
      where target.ctid = ranked.ctid
        and ranked.row_num > 1
    ';
  end if;

  select constraint_name
  into ticker_stats_pk
  from information_schema.table_constraints
  where table_schema = 'public'
    and table_name = 'ticker_market_stats_snapshot'
    and constraint_type = 'PRIMARY KEY';

  if ticker_stats_pk is not null then
    execute format('alter table public.ticker_market_stats_snapshot drop constraint %I', ticker_stats_pk);
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'symbol'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'ticker'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot drop column symbol';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'latest_price'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'last_price'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot drop column latest_price';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1d'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1d_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot drop column return_1d';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1m'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1m_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot drop column return_1m';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_3m'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_3m_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot drop column return_3m';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1y'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'return_1y_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot drop column return_1y';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'volatility_30d'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'vol_30d_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot drop column volatility_30d';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'drawdown_1y'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'drawdown_1y_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot drop column drawdown_1y';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'distance_from_52w_high'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'dist_from_52w_high_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot drop column distance_from_52w_high';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'distance_from_52w_low'
  ) and exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'dist_from_52w_low_pct'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot drop column distance_from_52w_low';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'ticker_market_stats_snapshot' and column_name = 'ticker'
  ) then
    execute 'alter table public.ticker_market_stats_snapshot alter column ticker set not null';
    execute 'alter table public.ticker_market_stats_snapshot add constraint ticker_market_stats_snapshot_pkey primary key (ticker)';
  end if;

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
