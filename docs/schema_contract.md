# Supabase schema contract (Data Ops v1)

Data Ops v1 writes these surfaces.

## Existing surfaces (must already exist)

- `market_price_daily`
- `market_quotes`
- `market_quotes_history`
- `mv_latest_prices` (RPC refresh path: `refresh_mv_latest_prices`)

## New v1 surfaces (created by SQL in this repo if missing)

- `ticker_market_stats_snapshot`
- `data_source_runs`
- `data_asset_status`
- `symbol_data_coverage`

SQL file: [`sql/001_data_ops_v1_surfaces.sql`](/home/franciscosantos/finance-data-ops/sql/001_data_ops_v1_surfaces.sql)

## Key payload requirements

`data_source_runs`:

- `run_id`
- `asset_name`
- `status`
- `failure_classification` (when failed)
- `started_at`, `ended_at`
- `rows_written`
- `symbols_requested`, `symbols_succeeded`, `symbols_failed`
- `error_messages`

`data_asset_status`:

- `asset_name`
- `as_of_date`
- `freshness_status`
- `last_observed_at`
- `details`

`symbol_data_coverage`:

- `symbol`
- `as_of_date`
- `has_market_price_daily`
- `has_market_quote`
- `latest_market_date`
- `latest_quote_ts`
- `coverage_status`
- `reason`

`ticker_market_stats_snapshot`:

- `latest_price`
- `return_1d`, `return_1m`, `return_3m`, `return_1y`
- `volatility_30d`
- `drawdown_1y`
- `distance_from_52w_high`
- `distance_from_52w_low`
