# Migration runbook (Data Ops v3)

## Purpose

Prepare Supabase for Data Ops-owned market/fundamentals/earnings/macro/release surfaces and operational metadata.

## SQL sources

Apply in order:

- [`sql/001_data_ops_v1_surfaces.sql`](/home/franciscosantos/finance-data-ops/sql/001_data_ops_v1_surfaces.sql)
- [`sql/002_data_ops_v2_fundamentals_earnings.sql`](/home/franciscosantos/finance-data-ops/sql/002_data_ops_v2_fundamentals_earnings.sql)
- [`sql/003_ticker_registry.sql`](/home/franciscosantos/finance-data-ops/sql/003_ticker_registry.sql)
- [`sql/004_data_ops_v3_macro_release.sql`](/home/franciscosantos/finance-data-ops/sql/004_data_ops_v3_macro_release.sql)

All scripts are idempotent (`create ... if not exists`, `add column if not exists`, guarded compatibility updates).

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
- v3:
  - `macro_series_catalog`
  - `macro_observations`
  - `macro_daily`
  - `economic_release_calendar`
  - `mv_latest_macro_observations` + refresh RPC
  - `mv_latest_economic_release_calendar` + refresh RPC

## Apply steps

1. Open Supabase SQL editor (or run via migration tool).
2. Execute the four migration files in order.
3. Run dry flows:
   - `python scripts/run_market_daily.py --symbols SPY --no-publish`
   - `python scripts/run_fundamentals_daily.py --symbols SPY --no-publish`
   - `python scripts/run_earnings_daily.py --symbols SPY --no-publish`
   - `python scripts/run_macro_daily.py --start 2000-01-01 --end 2020-12-31 --no-publish`
   - `python scripts/run_release_calendar_daily.py --start-date 2000-01-01 --end-date 2020-12-31 --no-publish`
4. Verify status rows update in `data_source_runs` and `data_asset_status` for macro/release assets.

## Historical backfill (idempotent)

All backfill commands support:

- `--start-date`
- `--end-date`
- `--force-recompute`

Backfill commands:

- macro only:
  - `python scripts/run_macro_backfill.py --start-date 2000-01-01 --end-date 2026-04-13`
- release calendar only:
  - `python scripts/run_release_calendar_backfill.py --start-date 2000-01-01 --end-date 2026-04-13`
- macro + release together:
  - `python scripts/run_macro_release_backfill.py --start-date 2000-01-01 --end-date 2026-04-13`

Write behavior:

- deterministic upserts (`on_conflict`) to canonical tables
- no duplicate keys on re-run

## Parity gate before Finance cutover

Run both parity checks and require `status=ok`:

- `python scripts/check_macro_parity.py --start-date 2020-01-01 --end-date 2026-04-13`
- `python scripts/check_release_calendar_parity.py --start-date 2020-01-01 --end-date 2026-04-13`

Tolerance contract is documented in:

- [`docs/parity_rules.md`](/home/franciscosantos/finance-data-ops/docs/parity_rules.md)
