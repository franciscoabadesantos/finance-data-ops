# Migration runbook (Data Ops v2)

## Purpose

Prepare Supabase for Data Ops-owned market/fundamentals/earnings surfaces and operational metadata.

## SQL sources

Apply in order:

- [`sql/001_data_ops_v1_surfaces.sql`](/home/franciscosantos/finance-data-ops/sql/001_data_ops_v1_surfaces.sql)
- [`sql/002_data_ops_v2_fundamentals_earnings.sql`](/home/franciscosantos/finance-data-ops/sql/002_data_ops_v2_fundamentals_earnings.sql)
- [`sql/003_ticker_registry.sql`](/home/franciscosantos/finance-data-ops/sql/003_ticker_registry.sql)

Both scripts are idempotent (`create ... if not exists`, `add column if not exists`, guarded compatibility updates).

## Existing surfaces expected before first run

- `market_price_daily`
- `market_quotes`
- `market_quotes_history`
- RPC/function `refresh_mv_latest_prices`

## Surfaces created/owned by migrations

- v1:
  - `ticker_market_stats_snapshot`
  - `data_source_runs`
  - `data_asset_status`
  - `symbol_data_coverage`
- v2:
  - `market_fundamentals_v2`
  - `mv_latest_fundamentals` + `refresh_mv_latest_fundamentals`
  - `ticker_fundamental_summary`
  - `market_earnings_events`
  - `market_earnings_history`
  - `mv_next_earnings` + `refresh_mv_next_earnings`

## Apply steps

1. Open Supabase SQL editor (or run via migration tool).
2. Execute `001_data_ops_v1_surfaces.sql`.
3. Execute `002_data_ops_v2_fundamentals_earnings.sql`.
4. Execute `003_ticker_registry.sql`.
5. Run dry flows:
   - `python scripts/run_market_daily.py --symbols SPY --no-publish`
   - `python scripts/run_fundamentals_daily.py --symbols SPY --no-publish`
   - `python scripts/run_earnings_daily.py --symbols SPY --no-publish`
   - `python flows/prefect_dataops_daily.py ticker-validation --input-symbol SPY --region us --no-publish`
6. Verify status rows update in `data_source_runs`, `data_asset_status`, `symbol_data_coverage` and `ticker_registry`.
