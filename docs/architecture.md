# Data Ops v2 Architecture

## System position

`finance-data-ops` is the operational product-data backend.

Flow per domain:

1. Fetch from external providers (`providers/`)
2. Normalize + persist canonical cache tables (`refresh/`)
3. Compute frontend-serving summaries (`derived/`)
4. Publish owned Supabase surfaces (`publish/`)
5. Publish operational status + coverage (`data_source_runs`, `data_asset_status`, `symbol_data_coverage`)

## Production boundary

Only this repo performs provider fetching for owned product-data domains.

This repo owns:

- Market data refresh + publish (v1)
- Fundamentals refresh + publish (v2)
- Earnings refresh + publish (v2)
- Frontend-serving product summaries derived from those domains
- Freshness, coverage, and run-status publication for all three domains

This repo does not own:

- Research/training/backtests
- Live inference/signal generation
- Signal publication workflows

Those remain in the `Finance` repository.

## Owned Supabase surfaces

### Market

- `market_price_daily`
- `market_quotes`
- `market_quotes_history`
- `mv_latest_prices` refresh RPC
- `ticker_market_stats_snapshot`

### Fundamentals

- `market_fundamentals_v2` (canonical normalized history)
- `mv_latest_fundamentals` (latest per `(ticker, metric)`)
- `ticker_fundamental_summary` (frontend-serving denormalized snapshot)

### Earnings

- `market_earnings_events` (upcoming/scheduled events)
- `market_earnings_history` (historical results)
- `mv_next_earnings` (next event per ticker)

### Operational

- `data_source_runs`
- `data_asset_status`
- `symbol_data_coverage`

## Operational entrypoints

- Market: `flows/dataops_market_daily.py` / `scripts/run_market_daily.py`
- Fundamentals: `flows/dataops_fundamentals_daily.py` / `scripts/run_fundamentals_daily.py`
- Earnings: `flows/dataops_earnings_daily.py` / `scripts/run_earnings_daily.py`
