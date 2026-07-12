# Supabase schema contract

Data Ops writes five product-data domains: market, fundamentals, earnings, macro, and economic release calendar. Fresh projects should bootstrap from the definitive runtime baseline:

- [`sql/000_definitive_runtime_schema.sql`](/home/franciscosantos/finance-data-ops/sql/000_definitive_runtime_schema.sql)

The definitive baseline is a single-file bootstrap for empty projects: current tables, materialized
views, refresh functions, indexes, RLS enablement, and minimal runtime seed data.

Legacy/retirement SQL, retained for older-instance archaeology and explicit cleanup only:

- [`sql/archive/legacy_public_product_surfaces/`](/home/franciscosantos/finance-data-ops/sql/archive/legacy_public_product_surfaces)
- [`sql/019_retire_legacy_public_product_surfaces.sql`](/home/franciscosantos/finance-data-ops/sql/019_retire_legacy_public_product_surfaces.sql)

Historical numbered SQL files in the archive are not part of fresh installs and are not runtime
product outputs. Existing databases that still have retired public product objects should run the
explicit retirement script manually after confirming product readers have cut over.

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

## Retired public product surfaces

These public product objects are retired and must not be created by final runtime schema or product
publishing paths:

- `public.market_price_daily`
- `public.market_quotes`
- `public.market_quotes_history`
- `public.ticker_market_stats_snapshot`
- `public.market_fundamentals_v2`
- `public.ticker_fundamental_summary`
- `public.market_earnings_events`
- `public.market_earnings_history`
- `public.mv_latest_prices`
- `public.mv_latest_fundamentals`
- `public.mv_next_earnings`
- `public.refresh_mv_latest_prices`
- `public.refresh_mv_latest_fundamentals`
- `public.refresh_mv_next_earnings`
- `public.ticker_fundamental_point_in_time`

Use [`sql/019_retire_legacy_public_product_surfaces.sql`](/home/franciscosantos/finance-data-ops/sql/019_retire_legacy_public_product_surfaces.sql)
for controlled cleanup on existing databases. Do not include the retirement script in normal source
refresh or fresh-install bootstrap.

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

Macro/release asset keys in `data_asset_status` are required:

- `macro_observations`
- `macro_daily`
- `economic_release_calendar`

Runtime baseline files:

- [`sql/000_runtime_schema.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_schema.sql)
- [`sql/000_runtime_seed.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_seed.sql)
