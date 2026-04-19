# Supabase schema contract

Data Ops writes five product-data domains: market, fundamentals, earnings, macro, and economic release calendar. Fresh projects should bootstrap from the definitive runtime baseline:

- [`sql/000_runtime_schema.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_schema.sql)
- [`sql/000_runtime_seed.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_seed.sql)

Historical numbered SQL files remain in the repo for older-instance archaeology, not for fresh installs.

## Market surfaces

- `market_price_daily`
- `market_quotes`
- `market_quotes_history`
- `mv_latest_prices` (RPC refresh path: `refresh_mv_latest_prices`)
- `ticker_market_stats_snapshot`

## Fundamentals surfaces

- `market_fundamentals_v2` (canonical normalized history)
- `mv_latest_fundamentals` (latest per `(ticker, metric)`)
- `ticker_fundamental_summary` (frontend snapshot)

## Earnings surfaces

- `market_earnings_events` (upcoming/scheduled)
- `market_earnings_history` (historical results)
- `mv_next_earnings` (next event per ticker)

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
- `symbol_data_coverage` (ticker coverage state)
- `ticker_registry` (symbol normalization + validation registry)

Macro/release asset keys in `data_asset_status` are required:

- `macro_observations`
- `macro_daily`
- `economic_release_calendar`

Runtime baseline files:

- [`sql/000_runtime_schema.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_schema.sql)
- [`sql/000_runtime_seed.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_seed.sql)
