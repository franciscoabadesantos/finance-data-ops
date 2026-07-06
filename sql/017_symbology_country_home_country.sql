alter table if exists public.etf_holdings
  add column if not exists holding_country text;

alter table if exists feature_store.entity_attributes_static
  add column if not exists home_country text;
