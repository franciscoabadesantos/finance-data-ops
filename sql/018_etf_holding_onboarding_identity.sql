-- Data Ops-owned provider-symbol read model for frontier/onboarding.

create table if not exists public.etf_holding_onboarding_identity (
  etf_ticker text not null,
  theme text,
  source_symbol text not null,
  source_name text,
  source_country text,
  source_exchange text,
  source_exchange_mic text,
  source_isin text,
  source_figi text,
  source_cusip text,
  canonical_entity_id text,
  normalized_entity_symbol text,
  provider text not null default 'yahoo',
  provider_symbol text,
  onboard_symbol text,
  onboard_region text,
  onboard_exchange text,
  is_onboardable boolean not null default false,
  not_onboardable_reason text,
  resolution_source text not null,
  resolution_confidence double precision not null default 0,
  primary key (etf_ticker, source_symbol, source_country)
);

create index if not exists idx_etf_holding_onboarding_identity_onboardable
  on public.etf_holding_onboarding_identity (is_onboardable, onboard_symbol)
  where is_onboardable = true;

create index if not exists idx_etf_holding_onboarding_identity_source
  on public.etf_holding_onboarding_identity (source_symbol, source_country);
