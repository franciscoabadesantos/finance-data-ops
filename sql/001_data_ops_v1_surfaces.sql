-- Data Ops v1 schema surfaces (create only if missing).
-- Existing tables/views (market_price_daily, market_quotes, market_quotes_history, mv_latest_prices)
-- remain owned by Data Ops; definitions here only cover v1 additions.

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
  run_id text primary key,
  asset_name text not null,
  status text not null,
  failure_classification text,
  started_at timestamptz not null,
  ended_at timestamptz not null,
  symbols_requested jsonb not null default '[]'::jsonb,
  symbols_succeeded jsonb not null default '[]'::jsonb,
  symbols_failed jsonb not null default '[]'::jsonb,
  rows_written integer not null default 0,
  error_messages jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.data_asset_status (
  asset_name text not null,
  as_of_date date not null,
  freshness_status text not null,
  last_observed_at timestamptz,
  details jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  primary key (asset_name, as_of_date)
);

create table if not exists public.symbol_data_coverage (
  symbol text not null,
  as_of_date date not null,
  has_market_price_daily boolean not null default false,
  has_market_quote boolean not null default false,
  latest_market_date timestamptz,
  latest_quote_ts timestamptz,
  coverage_status text not null,
  reason text not null default 'unknown',
  updated_at timestamptz not null default now(),
  primary key (symbol, as_of_date)
);

alter table if exists public.data_source_runs
  add column if not exists failure_classification text;

alter table if exists public.symbol_data_coverage
  add column if not exists latest_market_date timestamptz;

alter table if exists public.symbol_data_coverage
  add column if not exists latest_quote_ts timestamptz;

alter table if exists public.symbol_data_coverage
  add column if not exists reason text not null default 'unknown';

create index if not exists idx_data_source_runs_asset_started_at
  on public.data_source_runs (asset_name, started_at desc);

create index if not exists idx_data_asset_status_status
  on public.data_asset_status (freshness_status);

create index if not exists idx_symbol_data_coverage_status
  on public.symbol_data_coverage (coverage_status);
