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

Current market size is intentionally duplicated across market and fundamentals surfaces for
compatibility with existing consumers. See `docs/market_cap_contract.md`.

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

- `economic_release_calendar` (canonical schedule + observed availability semantics)
- `mv_latest_economic_release_calendar` (latest timestamp per release calendar source/series)

Release timing semantics:

- `scheduled_release_timestamp_utc` is the expected release schedule.
- `observed_first_available_at_utc` is first confirmed availability from authoritative source.
- downstream macro alignment uses observed availability when present, and scheduled time only as provisional fallback.

### Operational

- `data_source_runs`
- `data_asset_status`
- `symbol_data_coverage`
- `ticker_registry`
- `etf_holding_onboarding_identity`

Data Ops owns provider symbology for frontier/onboarding candidates. Backend services consume
`etf_holding_onboarding_identity` as a read model and pass through `onboard_symbol`; suffix and listing
venue rules live in `finance_data_ops.identity`.

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

Production scheduler automation is domain-separated to match those entrypoints.

## Request-driven jobs

Ticker lifecycle work is request-driven through Prefect deployments:

- Onboarding: `dataops_ticker_onboarding/ticker-onboarding`
- Removal/rejection: `dataops_ticker_remove/ticker-remove`
- Validation: `dataops_ticker_validation/ticker-validation`
- Backfill: `dataops_ticker_backfill/ticker-backfill`

Data Ops owns all `public.ticker_registry` lifecycle writes. The public backend
triggers Prefect deployments and reads state.
