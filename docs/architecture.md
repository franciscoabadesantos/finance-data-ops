# Data Ops v3 Architecture

## System position

`finance-data-ops` is the operational product-data backend and the canonical ingestion boundary.

Flow per domain:

1. Fetch from external providers (`providers/`)
2. Normalize + persist canonical cache tables (`refresh/`)
3. Compute frontend-serving summaries (`derived/`)
4. Publish owned Supabase surfaces (`publish/`)
5. Publish operational status + coverage (`data_source_runs`, `data_asset_status`, `symbol_data_coverage`)

## Production boundary

Only this repo performs provider fetching for owned external data domains.

This repo owns:

- Market data refresh + publish
- Fundamentals refresh + publish
- Earnings refresh + publish
- Macro refresh + publish
- Economic release calendar refresh + publish
- Frontend-serving summaries derived from those domains
- Freshness, coverage, and run-status publication for all owned domains

This repo does not own:

- Research/training/backtests
- Live inference/signal generation
- Signal publication workflows
- Model-specific feature engineering

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

### Macro

- `macro_series_catalog` (series metadata + policy)
- `macro_observations` (canonical observation-level history)
- `macro_daily` (business-day aligned canonical daily surface)
- `mv_latest_macro_observations` (latest per `series_key`)

### Economic release calendar

- `economic_release_calendar` (canonical release timestamps/events)
- `mv_latest_economic_release_calendar` (latest timestamp per release calendar source/series)

### Operational

- `data_source_runs`
- `data_asset_status`
- `symbol_data_coverage`
- `ticker_registry`

## Runtime source-of-truth contract

For macro and release-calendar domains, runtime source-of-truth is:

1. Canonical tables in `finance-data-ops`
2. (Temporary migration fallback only) legacy `Finance` loaders until removal phase

`Finance` CSV/config calendar artifacts are not part of post-cutover runtime logic.

## Operational entrypoints

- Market: `flows/dataops_market_daily.py` / `scripts/run_market_daily.py`
- Fundamentals: `flows/dataops_fundamentals_daily.py` / `scripts/run_fundamentals_daily.py`
- Earnings: `flows/dataops_earnings_daily.py` / `scripts/run_earnings_daily.py`
- Macro: `flows/dataops_macro_daily.py` / `scripts/run_macro_daily.py`
- Economic release calendar: `flows/dataops_release_calendar_daily.py` / `scripts/run_release_calendar_daily.py`
- Ticker backfill orchestration: `flows/prefect_dataops_daily.py:dataops_ticker_backfill_flow`
- Ticker validation orchestration: `flows/prefect_dataops_daily.py:dataops_ticker_validation_flow`
- Ticker onboarding orchestration: `flows/prefect_dataops_daily.py:dataops_ticker_onboarding_flow`

Production scheduler automation is domain-separated to match those entrypoints.
