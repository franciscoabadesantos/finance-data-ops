# Data Ops v1 Architecture

## System position

`finance-data-ops` is the operational market-data backend.

Flow:

1. Fetch from external market providers (`providers/`)
2. Normalize and persist canonical cache tables (`refresh/`)
3. Validate freshness and coverage (`validation/`)
4. Compute price-derived product metrics (`derived/`)
5. Publish owned Supabase surfaces (`publish/`)
6. Publish operational status trail (`data_source_runs`, `data_asset_status`, `symbol_data_coverage`)

## Production boundary

Only this repo performs market-data fetching.

This repo owns:

- Fetch + retry + transient failure classification
- Price and quote publication
- Product metrics from prices
- Freshness and coverage publication

This repo does not own:

- Research/training/backtests
- Promotion/versioning
- Live inference
- `market_signals`

Those remain in the `Finance` repository.

## Owned Supabase surfaces

Existing:

- `market_price_daily` (minimal daily close publish surface)
- `market_quotes`
- `market_quotes_history`
- `mv_latest_prices` refresh RPC

New v1 surfaces:

- `ticker_market_stats_snapshot`
- `data_source_runs`
- `data_asset_status`
- `symbol_data_coverage`

`ticker_market_stats_snapshot` is a latest-only serving table keyed by `ticker`.
`as_of_date` is informational and not part of the write key.

## Operational shape

- CI workflow: `.github/workflows/ci.yml`
- Scheduler workflow: `.github/workflows/daily_market_refresh.yml`
- Main flow entrypoint: `flows/dataops_market_daily.py`
- Manual entrypoint: `scripts/run_market_daily.py`
