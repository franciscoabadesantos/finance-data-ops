-- Ticker validation registry surface (additive).

create table if not exists public.ticker_registry (
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
  notes text,
  updated_at timestamptz not null default now(),
  check (instrument_type in ('equity','adr','etf','index_proxy','country_fund','unknown')),
  check (validation_status in ('pending_validation','validated_market_only','validated_full','rejected')),
  check (promotion_status in ('pending_validation','validated_market_only','validated_full','rejected'))
);

create index if not exists idx_ticker_registry_region_status
  on public.ticker_registry (region, validation_status, promotion_status);

create unique index if not exists idx_ticker_registry_input_scope
  on public.ticker_registry (input_symbol, region, coalesce(exchange, ''));
