-- Yahoo fundamentals/profile/ETF composition capture surfaces.
-- Ownership: finance-data-ops captures raw vendor values; backend formats and scores.

alter table if exists public.market_fundamentals_v2
  add column if not exists value_text text;

alter table if exists public.market_fundamentals_v2
  alter column value drop not null;

create table if not exists public.ticker_profile (
  ticker text primary key,
  description text,
  long_business_summary text,
  etf_category text,
  fund_family text,
  expense_ratio double precision,
  inception_date date,
  legal_type text,
  beta double precision,
  beta_3y double precision,
  source text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table if exists public.ticker_profile
  add column if not exists ticker text;
alter table if exists public.ticker_profile
  add column if not exists description text;
alter table if exists public.ticker_profile
  add column if not exists long_business_summary text;
alter table if exists public.ticker_profile
  add column if not exists etf_category text;
alter table if exists public.ticker_profile
  add column if not exists fund_family text;
alter table if exists public.ticker_profile
  add column if not exists expense_ratio double precision;
alter table if exists public.ticker_profile
  add column if not exists inception_date date;
alter table if exists public.ticker_profile
  add column if not exists legal_type text;
alter table if exists public.ticker_profile
  add column if not exists beta double precision;
alter table if exists public.ticker_profile
  add column if not exists beta_3y double precision;
alter table if exists public.ticker_profile
  add column if not exists source text;
alter table if exists public.ticker_profile
  add column if not exists fetched_at timestamptz not null default now();
alter table if exists public.ticker_profile
  add column if not exists updated_at timestamptz not null default now();

create index if not exists idx_ticker_profile_updated_at
  on public.ticker_profile (updated_at desc);

create table if not exists public.etf_holdings (
  etf_ticker text not null,
  holding_symbol text not null,
  holding_name text,
  weight double precision,
  as_of date not null,
  source text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (etf_ticker, holding_symbol, as_of)
);

alter table if exists public.etf_holdings
  add column if not exists etf_ticker text;
alter table if exists public.etf_holdings
  add column if not exists holding_symbol text;
alter table if exists public.etf_holdings
  add column if not exists holding_name text;
alter table if exists public.etf_holdings
  add column if not exists weight double precision;
alter table if exists public.etf_holdings
  add column if not exists as_of date;
alter table if exists public.etf_holdings
  add column if not exists source text;
alter table if exists public.etf_holdings
  add column if not exists fetched_at timestamptz not null default now();
alter table if exists public.etf_holdings
  add column if not exists updated_at timestamptz not null default now();

create index if not exists idx_etf_holdings_ticker_weight
  on public.etf_holdings (etf_ticker, as_of desc, weight desc);

create table if not exists public.etf_sector_weights (
  etf_ticker text not null,
  sector text not null,
  weight double precision,
  as_of date not null,
  source text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (etf_ticker, sector, as_of)
);

alter table if exists public.etf_sector_weights
  add column if not exists etf_ticker text;
alter table if exists public.etf_sector_weights
  add column if not exists sector text;
alter table if exists public.etf_sector_weights
  add column if not exists weight double precision;
alter table if exists public.etf_sector_weights
  add column if not exists as_of date;
alter table if exists public.etf_sector_weights
  add column if not exists source text;
alter table if exists public.etf_sector_weights
  add column if not exists fetched_at timestamptz not null default now();
alter table if exists public.etf_sector_weights
  add column if not exists updated_at timestamptz not null default now();

create index if not exists idx_etf_sector_weights_ticker_weight
  on public.etf_sector_weights (etf_ticker, as_of desc, weight desc);
