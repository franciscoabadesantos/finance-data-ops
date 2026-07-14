-- Definitive runtime baseline for fresh projects.
-- This file is intentionally non-destructive: it creates current runtime
-- schemas and seed data, but does not drop existing objects or data.

create extension if not exists pgcrypto;

create schema if not exists source_cache;
create schema if not exists feature_store;

create table if not exists source_cache.market_price_daily (
  symbol text not null,
  price_date date not null,
  open double precision,
  high double precision,
  low double precision,
  close double precision not null,
  adj_close double precision,
  volume bigint,
  source_updated_at timestamptz,
  ingested_at timestamptz not null default now(),
  primary key (symbol, price_date)
);

create index if not exists idx_source_cache_market_price_daily_date
  on source_cache.market_price_daily (price_date desc);

create table if not exists source_cache.fundamentals (
  symbol text not null,
  report_date date not null,
  metric text not null,
  value double precision,
  value_text text,
  period_end date not null,
  period_type text not null,
  fiscal_year integer,
  fiscal_quarter text,
  currency text not null default 'USD',
  source text,
  source_updated_at timestamptz,
  ingested_at timestamptz not null default now(),
  primary key (symbol, metric, period_end, period_type, report_date)
);

create index if not exists idx_source_cache_fundamentals_period_end
  on source_cache.fundamentals (period_end desc);

create index if not exists idx_source_cache_fundamentals_symbol_metric
  on source_cache.fundamentals (symbol, metric, period_end desc);

create table if not exists source_cache.earnings (
  symbol text not null,
  report_date date not null,
  earnings_date date not null,
  fiscal_period text not null,
  earnings_time text,
  actual_eps double precision,
  estimate_eps double precision,
  surprise_eps double precision,
  actual_revenue double precision,
  estimate_revenue double precision,
  surprise_revenue double precision,
  currency text not null default 'USD',
  source text,
  source_updated_at timestamptz,
  ingested_at timestamptz not null default now(),
  primary key (symbol, report_date, earnings_date, fiscal_period)
);

create index if not exists idx_source_cache_earnings_date
  on source_cache.earnings (earnings_date desc);

create table if not exists source_cache.macro_daily (
  as_of_date date not null,
  series_key text not null,
  value double precision,
  source_updated_at timestamptz,
  alignment_mode text,
  is_stale boolean not null default false,
  staleness_bdays integer,
  ingested_at timestamptz not null default now(),
  primary key (as_of_date, series_key)
);

create index if not exists idx_source_cache_macro_daily_series_date
  on source_cache.macro_daily (series_key, as_of_date desc);

create table if not exists source_cache.calendars (
  exchange_mic text not null,
  session_date date not null,
  is_trading_day boolean not null default true,
  is_half_day boolean not null default false,
  currency text,
  source text,
  source_updated_at timestamptz,
  ingested_at timestamptz not null default now(),
  primary key (exchange_mic, session_date)
);

create table if not exists source_cache.openfigi_mapping_raw (
  request_hash text primary key,
  request_payload jsonb not null,
  response_payload jsonb,
  status text not null,
  error_message text,
  requested_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (status in ('success', 'not_found', 'ambiguous', 'error'))
);

create table if not exists source_cache.gleif_entity_raw (
  lei text primary key,
  response_payload jsonb not null,
  status text not null,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists source_cache.listing_isin_raw (
  symbol text not null,
  provider text not null,
  request_payload jsonb not null,
  response_payload jsonb,
  isin text,
  status text not null,
  error_message text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (symbol, provider)
);

create index if not exists idx_listing_isin_raw_isin
  on source_cache.listing_isin_raw (isin);

create table if not exists source_cache.gleif_isin_lei_raw (
  isin text primary key,
  lei text,
  legal_name text,
  response_payload jsonb,
  status text not null,
  error_message text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_gleif_isin_lei_raw_lei
  on source_cache.gleif_isin_lei_raw (lei);

create table if not exists source_cache.gleif_lei_isin_raw (
  lei text primary key,
  response_payload jsonb,
  isin_list text[] not null default array[]::text[],
  status text not null,
  error_message text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists feature_store.entity_attributes_static (
  entity_id text primary key,
  name text,
  country text,
  home_country text,
  region text,
  exchange text,
  exchange_mic text,
  currency text,
  sector text,
  updated_at timestamptz not null default now()
);

create table if not exists feature_store.entity_master (
  entity_id text primary key,
  legal_name text,
  display_name text,
  home_country text,
  lei text,
  entity_source text not null,
  resolution_confidence double precision not null default 0,
  resolution_status text not null,
  primary_listing_symbol text,
  primary_listing_reason text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  check (resolution_status in ('resolved', 'provisional', 'needs_manual_review', 'conflict', 'rejected', 'superseded', 'ambiguous', 'unresolved', 'manual_review'))
);

create index if not exists idx_entity_master_home_country
  on feature_store.entity_master (home_country);

create table if not exists feature_store.entity_listing (
  symbol text primary key,
  entity_id text not null references feature_store.entity_master(entity_id),
  provider_symbol text,
  exchange text,
  exchange_mic text,
  country text,
  currency text,
  figi text,
  composite_figi text,
  share_class_figi text,
  isin text,
  lei text,
  listing_type text,
  is_primary_listing boolean not null default false,
  primary_listing_reason text,
  resolution_source text not null,
  resolution_confidence double precision not null default 0,
  resolution_status text not null,
  attach_method text,
  attach_confidence text,
  review_state text,
  evidence_payload jsonb not null default '{}'::jsonb,
  source_freshness jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  check (resolution_status in ('resolved', 'provisional', 'needs_manual_review', 'conflict', 'rejected', 'superseded', 'ambiguous', 'unresolved', 'manual_review')),
  check (review_state is null or review_state in ('resolved', 'provisional', 'needs_manual_review', 'conflict', 'rejected', 'superseded', 'ambiguous', 'unresolved', 'manual_review')),
  check (attach_confidence is null or attach_confidence in ('high', 'medium', 'low', 'provisional'))
);

create index if not exists idx_entity_listing_entity_id
  on feature_store.entity_listing (entity_id);

create index if not exists idx_entity_listing_isin
  on feature_store.entity_listing (isin);

create index if not exists idx_entity_listing_figi
  on feature_store.entity_listing (figi);

create index if not exists idx_entity_listing_composite_figi
  on feature_store.entity_listing (composite_figi);

create index if not exists idx_entity_listing_share_class_figi
  on feature_store.entity_listing (share_class_figi);

create index if not exists idx_entity_listing_exchange_mic
  on feature_store.entity_listing (exchange_mic);

create table if not exists feature_store.entity_identity_audit (
  audit_id bigserial primary key,
  symbol text,
  entity_id text,
  issue_type text not null,
  issue_severity text not null default 'warning',
  details jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_entity_identity_audit_symbol_issue_type
  on feature_store.entity_identity_audit (symbol, issue_type);

create table if not exists feature_store.entity_identity_review (
  review_id bigserial primary key,
  symbol text,
  entity_id text,
  candidate_lei text,
  attach_method text,
  resolution_state text not null default 'needs_manual_review',
  review_reason text,
  evidence_summary jsonb not null default '{}'::jsonb,
  conflicting_candidates jsonb not null default '[]'::jsonb,
  reviewer_decision text,
  reviewer_identity text,
  reviewed_at timestamptz,
  override_provenance text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (resolution_state in ('resolved', 'provisional', 'needs_manual_review', 'conflict', 'rejected', 'superseded', 'ambiguous', 'unresolved', 'manual_review'))
);

create index if not exists idx_entity_identity_review_symbol_state
  on feature_store.entity_identity_review (symbol, resolution_state);

create index if not exists idx_entity_identity_review_entity_id
  on feature_store.entity_identity_review (entity_id);

create table if not exists feature_store.entity_identity_review_audit (
  audit_id bigserial primary key,
  review_id bigint references feature_store.entity_identity_review(review_id),
  symbol text,
  entity_id text,
  event_type text not null,
  event_payload jsonb not null default '{}'::jsonb,
  actor text,
  created_at timestamptz not null default now()
);

create index if not exists idx_entity_identity_review_audit_review_id
  on feature_store.entity_identity_review_audit (review_id);

create table if not exists feature_store.technical_features_daily (
  symbol text not null,
  as_of_date date not null,
  features jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  primary key (symbol, as_of_date)
);

create table if not exists feature_store.scorecard_daily (
  symbol text not null,
  as_of_date date not null,
  scorecard jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  primary key (symbol, as_of_date)
);

create table if not exists feature_store.ticker_page_summary (
  symbol text primary key,
  as_of_date date,
  last_price double precision,
  return_1d_pct double precision,
  return_1m_pct double precision,
  return_3m_pct double precision,
  return_1y_pct double precision,
  drawdown_1y_pct double precision,
  summary_json jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists feature_store.ticker_readiness (
  symbol text primary key,
  readiness_status text not null,
  market_data_available boolean not null default false,
  fundamentals_available boolean not null default false,
  earnings_available boolean not null default false,
  technical_features_available boolean not null default false,
  scorecard_available boolean not null default false,
  ticker_page_summary_available boolean not null default false,
  reason text,
  updated_at timestamptz not null default now()
);

do $$
declare
  read_role text;
begin
  if exists (select 1 from pg_roles where rolname = 'finance_data_ops_worker') then
    grant usage on schema source_cache to finance_data_ops_worker;
    grant usage on schema feature_store to finance_data_ops_worker;
    grant select, insert, update, delete
      on source_cache.openfigi_mapping_raw,
         source_cache.gleif_entity_raw,
         source_cache.listing_isin_raw,
         source_cache.gleif_isin_lei_raw,
         source_cache.gleif_lei_isin_raw
      to finance_data_ops_worker;
    grant select, insert, update, delete
      on feature_store.entity_master,
         feature_store.entity_listing,
         feature_store.entity_identity_audit,
         feature_store.entity_identity_review,
         feature_store.entity_identity_review_audit
      to finance_data_ops_worker;
    grant usage, select on sequence feature_store.entity_identity_audit_audit_id_seq
      to finance_data_ops_worker;
    grant usage, select
      on sequence feature_store.entity_identity_review_review_id_seq,
                  feature_store.entity_identity_review_audit_audit_id_seq
      to finance_data_ops_worker;
    grant select on feature_store.ticker_readiness to finance_data_ops_worker;
  end if;

  foreach read_role in array array['finance_backend_read', 'finance_backend', 'backend_read'] loop
    if exists (select 1 from pg_roles where rolname = read_role) then
      execute format('grant usage on schema source_cache to %I', read_role);
      execute format('grant usage on schema feature_store to %I', read_role);
      execute format(
        'grant select on source_cache.openfigi_mapping_raw, source_cache.gleif_entity_raw, source_cache.listing_isin_raw, source_cache.gleif_isin_lei_raw, source_cache.gleif_lei_isin_raw to %I',
        read_role
      );
      execute format(
        'grant select on feature_store.entity_master, feature_store.entity_listing, feature_store.entity_identity_audit, feature_store.entity_identity_review, feature_store.entity_identity_review_audit to %I',
        read_role
      );
    end if;
  end loop;
end $$;

create table if not exists public.data_source_runs (
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

create index if not exists idx_data_source_runs_started_at
  on public.data_source_runs (started_at desc);

create index if not exists idx_data_source_runs_status_started_at
  on public.data_source_runs (status, started_at desc);

create table if not exists public.data_asset_status (
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

create index if not exists idx_data_asset_status_updated_at
  on public.data_asset_status (updated_at desc);

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
  reason text,
  updated_at timestamptz not null default now()
);

create index if not exists idx_symbol_data_coverage_status_updated
  on public.symbol_data_coverage (coverage_status, updated_at desc);

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

create index if not exists idx_ticker_profile_updated_at
  on public.ticker_profile (updated_at desc);

create table if not exists source_cache.etf_holdings (
  etf_ticker text not null,
  holding_symbol text not null,
  holding_name text,
  holding_country text,
  weight double precision,
  as_of date not null,
  source text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (etf_ticker, holding_symbol, as_of)
);

create index if not exists idx_etf_holdings_ticker_weight
  on source_cache.etf_holdings (etf_ticker, as_of desc, weight desc);

-- Operational identity read model for provider/onboarding workflows. Relationship-map
-- holdings and theme catalog consumers must use source_cache.etf_holdings,
-- source_cache.etf_themes, and source_cache.etf_theme_readiness.
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

do $$
begin
  if exists (select 1 from pg_roles where rolname = 'finance_data_ops_worker') then
    grant select, insert, update, delete
      on public.etf_holding_onboarding_identity
      to finance_data_ops_worker;
  end if;
end $$;

create table if not exists source_cache.etf_themes (
  etf_ticker text primary key,
  theme text not null,
  wave integer not null check (wave in (1, 2)),
  issuer text,
  source_type text not null,
  source_ref text,
  holdings_count integer,
  holdings_as_of date,
  holdings_source_depth text not null default 'unknown',
  holdings_shallow boolean not null default false,
  active boolean not null default true,
  fetched_at timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists idx_etf_themes_theme_wave
  on source_cache.etf_themes (theme, wave, active);

create table if not exists source_cache.etf_theme_readiness (
  etf_symbol text primary key,
  theme text,
  active boolean not null default true,
  holdings_count integer not null default 0,
  holdings_as_of date,
  holdings_shallow boolean not null default false,
  priced_constituent_count integer not null default 0,
  technical_constituent_count integer not null default 0,
  tracked_constituent_count integer not null default 0,
  coverage_ratio double precision not null default 0,
  relationship_map_eligible boolean not null default false,
  relationship_map_ineligible_reason text,
  computed_at timestamptz not null default now()
);

create index if not exists idx_etf_theme_readiness_eligibility
  on source_cache.etf_theme_readiness (relationship_map_eligible, active, computed_at desc);

do $$
declare
  read_role text;
begin
  if exists (select 1 from pg_roles where rolname = 'finance_data_ops_worker') then
    grant usage on schema source_cache to finance_data_ops_worker;
    grant select, insert, update, delete
      on source_cache.etf_holdings,
         source_cache.etf_themes,
         source_cache.etf_theme_readiness
      to finance_data_ops_worker;
  end if;

  foreach read_role in array array['finance_backend_read', 'finance_backend', 'backend_read'] loop
    if exists (select 1 from pg_roles where rolname = read_role) then
      execute format('grant usage on schema source_cache to %I', read_role);
      execute format(
        'grant select on source_cache.etf_holdings, source_cache.etf_themes, source_cache.etf_theme_readiness to %I',
        read_role
      );
    end if;
  end loop;
end $$;

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

create index if not exists idx_etf_sector_weights_ticker_weight
  on public.etf_sector_weights (etf_ticker, as_of desc, weight desc);

create table if not exists public.ticker_registry (
  registry_key text primary key,
  input_symbol text not null,
  normalized_symbol text,
  region text not null default 'us',
  exchange text,
  exchange_mic text,
  currency text,
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

create index if not exists idx_ticker_registry_region_status
  on public.ticker_registry (region, validation_status, promotion_status);

create unique index if not exists idx_ticker_registry_input_scope
  on public.ticker_registry (input_symbol, region, coalesce(exchange, ''));

create table if not exists public.exchange_trading_calendar (
  exchange_mic text not null,
  session_date date not null,
  is_half_day boolean not null default false,
  ingested_at timestamptz not null default now(),
  primary key (exchange_mic, session_date)
);

create index if not exists idx_exchange_trading_calendar_mic_date
  on public.exchange_trading_calendar (exchange_mic, session_date);

create table if not exists public.macro_series_catalog (
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

create index if not exists idx_macro_series_catalog_required
  on public.macro_series_catalog (required_by_default desc, series_key);

create table if not exists public.macro_observations (
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

create index if not exists idx_macro_observations_observation_date
  on public.macro_observations (observation_date desc);

create index if not exists idx_macro_observations_release_timestamp
  on public.macro_observations (release_timestamp_utc desc);

create table if not exists public.macro_daily (
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

create index if not exists idx_macro_daily_series_date
  on public.macro_daily (series_key, as_of_date desc);

create materialized view if not exists public.mv_latest_macro_observations as
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

create unique index if not exists idx_mv_latest_macro_observations_series_key
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

create table if not exists public.economic_release_calendar (
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
  check (release_timestamp_utc = scheduled_release_timestamp_utc)
);

create index if not exists idx_economic_release_calendar_scheduled_release
  on public.economic_release_calendar (scheduled_release_timestamp_utc desc);

create index if not exists idx_economic_release_calendar_observed_release
  on public.economic_release_calendar (observed_first_available_at_utc desc);

create materialized view if not exists public.mv_latest_economic_release_calendar as
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

create unique index if not exists idx_mv_latest_economic_release_calendar_series_key
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

create table if not exists public.async_job_runs (
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

create index if not exists idx_async_job_runs_registry_updated
  on public.async_job_runs (registry_key, updated_at desc);

create index if not exists idx_async_job_runs_type_status_updated
  on public.async_job_runs (job_type, status, updated_at desc);

create table if not exists public.analysis_jobs (
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
  summary_text text,
  created_at timestamptz not null default now()
);

create index if not exists idx_analysis_results_type_created
  on public.analysis_results (analysis_type, created_at desc);

create table if not exists public.production_signals (
  signal_date date not null,
  strategy_family text not null,
  universe text not null,
  environment text not null default 'production',
  symbol text not null,
  active_pointer_id text,
  candidate_id text,
  bundle_id text,
  score double precision,
  rank integer,
  percentile double precision,
  selected boolean,
  horizon text,
  target_exposure double precision,
  signal_json jsonb not null default '{}'::jsonb,
  result_ref text,
  orchestrator_run_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (signal_date, strategy_family, universe, environment, symbol)
);

create index if not exists ix_production_signals_date
  on public.production_signals (signal_date desc);

create index if not exists ix_production_signals_pointer
  on public.production_signals (active_pointer_id);

insert into public.macro_series_catalog (
  series_key,
  source_provider,
  source_code,
  frequency,
  required_by_default,
  required_from_date,
  optional,
  staleness_max_bdays,
  release_calendar_source,
  description
)
values
  ('VIX', 'yfinance', '^VIX', 'daily', true, '1990-01-02', false, 5, null, 'CBOE Volatility Index close.'),
  ('VIX3M', 'fred', 'VXVCLS', 'daily', true, '2007-12-04', false, 5, null, 'CBOE 3M volatility index close.'),
  ('VVIX', 'yfinance', '^VVIX', 'daily', true, '2007-01-03', false, 5, null, 'CBOE VVIX close.'),
  ('10Y_Treasury_Yield', 'fred', 'DGS10', 'daily', true, null, false, 5, null, '10Y treasury yield.'),
  ('2Y_Treasury_Yield', 'fred', 'DGS2', 'daily', true, null, false, 5, null, '2Y treasury yield.'),
  ('High_Yield_Spread', 'fred', 'BAMLH0A0HYM2', 'daily', true, null, false, 5, null, 'US high yield OAS.'),
  ('TED_Spread', 'fred', 'TEDRATE', 'daily', false, null, true, 5, null, 'TED spread optional.'),
  ('CPI_Headline', 'fred', 'CPIAUCSL', 'monthly', false, null, false, 45, 'bls_cpi_release_calendar_v1', 'Headline CPI.'),
  ('CPI_Core', 'fred', 'CPILFESL', 'monthly', false, null, false, 45, 'bls_cpi_release_calendar_v1', 'Core CPI.'),
  ('UNRATE', 'fred', 'UNRATE', 'monthly', true, null, false, 45, 'bls_unrate_release_calendar_v1', 'U-3 unemployment rate.'),
  ('U6RATE', 'fred', 'U6RATE', 'monthly', false, null, false, 45, 'bls_unrate_release_calendar_v1', 'U-6 unemployment rate.'),
  ('ICSA', 'fred', 'ICSA', 'weekly', false, null, false, 10, 'dol_icsa_release_calendar_v1', 'Initial jobless claims.'),
  ('CIVPART', 'fred', 'CIVPART', 'monthly', false, null, false, 45, 'bls_unrate_release_calendar_v1', 'Labor force participation.'),
  ('WTI', 'fred', 'DCOILWTICO', 'daily', false, null, false, 5, null, 'WTI spot price.'),
  ('Gasoline_US_Regular', 'fred', 'GASREGW', 'weekly', false, null, false, 10, null, 'US gasoline weekly.'),
  ('NatGas_HenryHub', 'fred', 'DHHNGSP', 'daily', false, null, false, 5, null, 'Henry Hub nat gas.'),
  ('DBC', 'yfinance', 'DBC', 'daily', false, null, false, 5, null, 'DBC ETF close.')
on conflict (series_key) do update set
  source_provider = excluded.source_provider,
  source_code = excluded.source_code,
  frequency = excluded.frequency,
  required_by_default = excluded.required_by_default,
  required_from_date = excluded.required_from_date,
  optional = excluded.optional,
  staleness_max_bdays = excluded.staleness_max_bdays,
  release_calendar_source = excluded.release_calendar_source,
  description = excluded.description,
  updated_at = now();
