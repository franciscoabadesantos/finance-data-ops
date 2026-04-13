# Prefect Cloud orchestration (Data Ops v3)

This repository uses Prefect Cloud as the primary orchestration layer for daily Data Ops runs.

## Flow wrappers

Prefect `@flow` wrappers are in:

- [`flows/prefect_dataops_daily.py`](/home/franciscosantos/finance-data-ops/flows/prefect_dataops_daily.py)

Wrapped domains:

- `dataops_market_daily`
- `dataops_fundamentals_daily`
- `dataops_earnings_daily`
- `dataops_macro_daily`
- `dataops_release_calendar_daily`
- `dataops_ticker_backfill`
- `dataops_ticker_validation`
- `dataops_ticker_onboarding`

## Deployments

Deployment definitions are version-controlled in:

- [`prefect.yaml`](/home/franciscosantos/finance-data-ops/prefect.yaml)

Base deployments:

- `market-daily`
- `fundamentals-daily`
- `earnings-daily`
- `macro-daily`
- `release-calendar-daily`
- `ticker-onboarding`
- `ticker-backfill`
- `ticker-validation`

Cadence strategy (weekday UTC):

- Market (`market-daily`): `06:30`, `14:30`, `22:30`
- Earnings (`earnings-daily`): `08:00`, `20:00`
- Fundamentals (`fundamentals-daily`): `03:00`
- Macro (`macro-daily`): `06:15`, `14:45`, `22:45`
- Economic release calendar (`release-calendar-daily`): `05:00`, `15:00`
- Ticker onboarding/validation/backfill: event-driven or on-demand

## Publish safety gates

Each domain validates payloads before publish. Publish fails before DB write on validation failure.

Macro gates:

- no NULL required values
- valid timestamps
- no forward-dated release timestamps

Release-calendar gates:

- valid `release_timestamp_utc`
- non-empty `release_timezone`
- no duplicate `(series_key, observation_period)` rows
