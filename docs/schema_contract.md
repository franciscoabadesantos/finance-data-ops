# Supabase schema contract (Data Ops v1)

Data Ops v1 writes these surfaces.

## Existing surfaces (must already exist)

- `market_price_daily`
- `market_quotes`
- `market_quotes_history`
- `mv_latest_prices` (RPC refresh path: `refresh_mv_latest_prices`)

`market_price_daily` (production-authoritative minimal close table):

- `ticker`
- `date`
- `close`
- `source`
- `fetched_at`
- `created_at`

Write contract:

- upsert key: `ticker,date`

`market_quotes` (production-authoritative latest quote cache):

- `ticker`
- `name`
- `price`
- `change`
- `change_percent`
- `market_cap_text`
- `source`
- `fetched_at`
- `created_at`
- `updated_at`

Write contract:

- upsert key: `ticker`

## New v1 surfaces (created by SQL in this repo if missing)

- `ticker_market_stats_snapshot`
- `data_source_runs`
- `data_asset_status`
- `symbol_data_coverage`

SQL file: [`sql/001_data_ops_v1_surfaces.sql`](/home/franciscosantos/finance-data-ops/sql/001_data_ops_v1_surfaces.sql)

## Key payload requirements

`data_source_runs`:

- `job_name`
- `source_type`
- `scope`
- `status`
- `started_at`, `finished_at`
- `rows_written`
- `error_class`, `error_message`
- `failure_classification` (when failed)
- `symbols_requested`, `symbols_succeeded`, `symbols_failed`
- `error_messages`
- `created_at`

`data_asset_status`:

- `asset_key`
- `asset_type`
- `provider`
- `last_success_at`
- `last_available_date`
- `freshness_status`
- `coverage_status`
- `reason`
- `updated_at`

`symbol_data_coverage`:

- `ticker`
- `market_data_available`
- `fundamentals_available`
- `earnings_available`
- `signal_available`
- `market_data_last_date`
- `fundamentals_last_date`
- `next_earnings_date`
- `coverage_status`
- `reason`
- `updated_at`

`ticker_market_stats_snapshot`:

- `ticker` (upsert key; latest snapshot row per ticker)
- `last_price`
- `as_of_date` (informational, not part of upsert key)
- `return_1d_pct`, `return_1m_pct`, `return_3m_pct`, `return_1y_pct`
- `vol_30d_pct`
- `drawdown_1y_pct`
- `dist_from_52w_high_pct`
- `dist_from_52w_low_pct`
- `updated_at`
