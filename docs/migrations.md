# Migration runbook (Data Ops v1)

## Purpose

Prepare Supabase for Data Ops v1-owned status/metrics surfaces.

## SQL source

Apply:

- [`sql/001_data_ops_v1_surfaces.sql`](/home/franciscosantos/finance-data-ops/sql/001_data_ops_v1_surfaces.sql)

This script is idempotent (`create table if not exists`, `add column if not exists`, guarded compatibility renames/backfills).

## Surfaces that must already exist before first daily run

- `market_price_daily`
- `market_quotes`
- `market_quotes_history`
- RPC/function `refresh_mv_latest_prices` for materialized view maintenance

## New surfaces created by migration if missing

- `ticker_market_stats_snapshot`
- `data_source_runs`
- `data_asset_status`
- `symbol_data_coverage`

`ticker_market_stats_snapshot` is maintained as a latest snapshot table keyed by `ticker`.

## Apply steps

1. Open Supabase SQL editor (or run via your DB migration tool).
2. Execute `001_data_ops_v1_surfaces.sql`.
3. Verify tables exist and indexes are present.
4. Run one dry publish:
   - `python scripts/run_market_daily.py --symbols SPY --start 2026-01-01 --end 2026-04-10`
5. Validate rows appear in status tables.
