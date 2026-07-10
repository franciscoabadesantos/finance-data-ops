# Prefect Cloud orchestration (Data Ops v3)

This repository uses Prefect Cloud as the primary orchestration layer for daily Data Ops runs.

## Flow wrappers

Prefect `@flow` wrappers are in:

- [`flows/prefect_dataops_daily.py`](/home/franciscosantos/finance-data-ops/flows/prefect_dataops_daily.py)

Wrapped domains:

- `dataops_daily`
- `dataops_market_daily`
- `dataops_fundamentals_daily`
- `dataops_earnings_daily`
- `dataops_macro_daily`
- `dataops_release_calendar_daily`

## Deployments

Deployment definitions are version-controlled in:

- [`prefect.yaml`](/home/franciscosantos/finance-data-ops/prefect.yaml)

Base deployments:

- `dataops-daily`
- `market-daily`
- `fundamentals-daily`
- `earnings-daily`
- `macro-daily`
- `release-calendar-daily`

Cadence strategy (weekday UTC):

- Aggregate source handoff (`dataops-daily`): `23:10`
- Market (`market-daily`): `06:30`, `14:30`, `22:30`
- Earnings (`earnings-daily`): `08:00`, `20:00`
- Fundamentals (`fundamentals-daily`): `03:00`
- Macro (`macro-daily`): `06:15`, `14:45`, `22:45`
- Economic release calendar (`release-calendar-daily`): `05:00`, `15:00`

## Data Ops to feature-store handoff

Ownership boundary:

- Data Ops owns ingestion and publishing for source tables under `source_cache` and canonical operational status tables.
- Feature-store owns derived features, including `feature_store.scorecard_daily`.
- Data Ops may trigger feature-store deployments only through configured Prefect deployment names.

Deployment contract:

- `FEATURE_BUILD_DAILY_DEPLOYMENT` is the Prefect deployment name for the full daily feature-store build.
- Self-host default: `feature-build-daily/feature-build-daily`.
- `FEATURE_SCORECARD_BUILD_DEPLOYMENT` is the Prefect deployment name for targeted onboarding scorecard-only builds. Self-host default: `scorecard-daily/scorecard-daily`. Ticker onboarding schedules a targeted scorecard-only build after source data and technical feature backfill complete. If no scorecard deployment is configured, onboarding reports `scorecard_build.status=skipped` with `reason=deployment_not_configured`.

Orchestration order:

1. `dataops_daily` runs source-domain refreshes: release calendar, macro, earnings, fundamentals, then market.
2. The aggregate flow evaluates source watermarks for market, macro, calendars, fundamentals, and earnings.
3. If blocking watermarks are ready, Data Ops triggers `FEATURE_BUILD_DAILY_DEPLOYMENT` with `{"as_of_date": "<YYYY-MM-DD>"}`.
4. Macro daily no longer owns the default feature-store trigger. It can still opt in with `trigger_feature_build=true` for manual recovery runs.

Tracked universe contract:

- `ticker_registry` is pipeline state for validation/onboarding, not the tracked universe source of truth.
- Data Ops/Prefect owns all `ticker_registry` lifecycle writes. Backend services trigger Prefect deployments (`ticker-onboarding`, `ticker-remove`, and direct validation/backfill only when explicitly needed) and read state only.
- Daily tracked symbols come from deployment `symbols`, `DATA_OPS_SYMBOLS_<REGION>`, or `DATA_OPS_SYMBOLS`.
- The registry universe fallback is disabled by default and requires `DATA_OPS_ALLOW_TICKER_REGISTRY_UNIVERSE=true` for migration-only use.

Request-driven ticker validation/backfill are deployed without schedules and are invoked by onboarding/backend paths.

## Publish safety gates

Each domain validates payloads before publish. Publish fails before DB write on validation failure.

Macro gates:

- no NULL required values
- valid timestamps
- no forward-dated release timestamps

Release-calendar gates:

- valid `scheduled_release_timestamp_utc`
- valid `observed_first_available_at_utc` when present
- valid `availability_status` / `availability_source`
- valid `delay_vs_schedule_seconds` semantics
- consistent `is_schedule_based_only` vs observed availability
- non-empty `release_timezone`
- no duplicate `(series_key, observation_period)` rows
