# Supabase schema contract

Data Ops writes five product-data domains: market, fundamentals, earnings, macro, and economic release calendar. Fresh projects should bootstrap from the definitive runtime baseline:

- [`sql/000_runtime_schema.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_schema.sql)

The definitive baseline is a single-file bootstrap for empty projects: current tables, materialized
views, refresh functions, indexes, RLS enablement, and minimal runtime seed data.

## Market surfaces

- `source_cache.market_price_daily`
- `feature_store.technical_features_daily`
- `feature_store.scorecard_daily`
- `feature_store.ticker_page_summary`

`source_cache.market_price_daily` write contract (upsert key: `symbol,price_date`):

- `symbol`
- `price_date`
- `open`
- `high`
- `low`
- `close`
- `adj_close`
- `volume`
- `source_updated_at`
- `ingested_at`

## Fundamentals surfaces

- `source_cache.fundamentals`
- `feature_store.ticker_page_summary`
- `feature_store.entity_attributes_static`
- `etf_holding_onboarding_identity` (Data Ops-owned provider-symbol read model for ETF/frontier onboarding)

`source_cache.fundamentals` stores normalized provider fundamentals, including fiscal history and
point-in-time vendor metrics. Feature-store owns derived product read models.

`etf_holding_onboarding_identity` preserves ETF/source identity separately from provider onboarding
identity. Backend/frontier services should read `onboard_symbol` only when `is_onboardable=true`;
country/exchange suffix resolution is owned by `finance_data_ops.identity`.

## Earnings surfaces

- `source_cache.earnings`
- `feature_store.ticker_page_summary`

## Macro surfaces

- `macro_series_catalog`
- `macro_observations`
- `macro_daily`
- `mv_latest_macro_observations`

`macro_observations` write contract (upsert key: `series_key,observation_period`):

- `series_key`
- `observation_period`
- `observation_date`
- `frequency`
- `value`
- `source_provider`
- `source_code`
- `release_timestamp_utc`
- `release_timezone`
- `release_date_local`
- `release_calendar_source`
- `source`
- `fetched_at`
- `ingested_at`

`macro_daily` write contract (upsert key: `as_of_date,series_key`):

- `as_of_date`
- `series_key`
- `value`
- `source_observation_period`
- `source_observation_date`
- `available_at_utc`
- `staleness_bdays`
- `is_stale`
- `alignment_mode`
- `ingested_at`

## Economic release calendar surfaces

- `economic_release_calendar`
- `mv_latest_economic_release_calendar`

`economic_release_calendar` write contract (upsert key: `series_key,observation_period`):

- `series_key`
- `observation_period`
- `observation_date`
- `scheduled_release_timestamp_utc`
- `observed_first_available_at_utc`
- `availability_status`
- `availability_source`
- `delay_vs_schedule_seconds`
- `is_schedule_based_only`
- `release_timestamp_utc`
- `release_timezone`
- `release_date_local`
- `release_calendar_source`
- `source`
- `provenance_class`
- `ingested_at`

Compatibility note:

- `release_timestamp_utc` is retained as a backward-compatible alias of `scheduled_release_timestamp_utc`.
- Treat `release_timestamp_utc` as a scheduled timestamp alias, not as proof that the release was actually observed in the wild.
- Actual observed availability belongs in `observed_first_available_at_utc`.

## Operational surfaces

- `data_source_runs` (run trail for refresh + publish + orchestration)
- `data_asset_status` (freshness/coverage per owned asset)
- `symbol_data_coverage` (diagnostic ticker coverage state rebuilt from materialized source rows)
- `ticker_registry` (ticker lifecycle registry; Data Ops/Prefect owns writes)
- `etf_holding_onboarding_identity` (provider onboarding identity read model)

## Entity identity V0 surfaces

- `source_cache.openfigi_mapping_raw` (raw/cache table for OpenFIGI mapping requests and responses)
- `source_cache.gleif_entity_raw` (optional future LEI enrichment cache)
- `source_cache.listing_isin_raw` (raw/cache table for provider listing-to-ISIN enrichment)
- `source_cache.gleif_isin_lei_raw` (raw/cache table for GLEIF ISIN-to-LEI enrichment)
- `feature_store.entity_master` (canonical company/legal entity read model)
- `feature_store.entity_listing` (one row per provider/listing symbol mapped to an entity)
- `feature_store.entity_identity_audit` (append-only audit rows for unresolved, ambiguous, or conflicting identity cases)

Entity identity V0 is side-by-side only. OpenFIGI ticker mapping is the main listing/security identity source, but it is not sufficient alone for company/entity grouping. Ticker-mapping FIGIs identify securities/listings. Company grouping requires the measured `listing -> ISIN -> LEI` path or an equivalent strong company-level identifier. ISIN alone identifies a security/instrument; LEI groups the legal entity. GLEIF coverage is partial, and ADR grouping is empirical because an ADR ISIN can map to the depositary rather than the underlying issuer. `feature_store.entity_attributes_static` is still a metadata read model, not entity master. Existing product/read paths, frontend search, `ticker_readiness`, prices, technicals, scorecards, and relationship edges do not consume these tables yet. Entity tables remain unpublished until the acceptance set proves the chain.

Macro/release asset keys in `data_asset_status` are required:

- `macro_observations`
- `macro_daily`
- `economic_release_calendar`

Runtime baseline files:

- [`sql/000_runtime_schema.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_schema.sql)
- [`sql/000_runtime_seed.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_seed.sql)
