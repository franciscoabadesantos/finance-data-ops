# Runtime baseline runbook

## Purpose

Prepare a fresh Supabase project for current Data Ops-owned runtime surfaces.

## SQL sources

Preferred path for a fresh empty project:

- [`sql/000_definitive_runtime_schema.sql`](/home/franciscosantos/finance-data-ops/sql/000_definitive_runtime_schema.sql)

This is the single-file bootstrap for empty projects: current tables, materialized views, refresh
functions, indexes, RLS enablement, and minimal runtime seed data.

## Surfaces created/owned by migrations

- Runtime schema:
  - `source_cache.market_price_daily`
  - `source_cache.fundamentals`
  - `source_cache.earnings`
  - `feature_store.technical_features_daily`
  - `feature_store.scorecard_daily`
  - `feature_store.ticker_page_summary`
  - `feature_store.entity_attributes_static`
  - `feature_store.ticker_readiness`
  - `data_source_runs`
  - `data_asset_status`
  - `symbol_data_coverage`
  - `ticker_registry`
  - `macro_series_catalog`
  - `macro_observations`
  - `macro_daily`
  - `economic_release_calendar`
  - `mv_latest_macro_observations` + refresh RPC
  - `mv_latest_economic_release_calendar` + refresh RPC
  - `async_job_runs` durable request-driven async job audit surface
  - `analysis_jobs`
  - `analysis_results`
- Runtime seed:
  - default `macro_series_catalog` rows required by daily macro/release refreshes

## Apply steps

1. Open Supabase SQL editor for a new project.
2. Execute `sql/000_definitive_runtime_schema.sql`.
3. Run dry flows:
   - `python scripts/run_market_daily.py --symbols SPY --no-publish`
   - `python scripts/run_fundamentals_daily.py --symbols SPY --no-publish`
   - `python scripts/run_earnings_daily.py --symbols SPY --no-publish`
   - `python scripts/run_macro_daily.py --start 2000-01-01 --end 2020-12-31 --no-publish`
   - `python scripts/run_release_calendar_daily.py --start-date 2000-01-01 --end-date 2020-12-31 --no-publish`
4. Verify required runtime relations exist:
   - `analysis_jobs`
   - `analysis_results`
   - `async_job_runs`
   - `ticker_registry`
   - `macro_series_catalog`
5. Verify status rows update in `data_source_runs` and `data_asset_status` for macro/release assets.

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
