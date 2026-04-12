# Prefect Cloud orchestration (Phase 1)

This repository uses Prefect Cloud as the primary orchestration layer for daily Data Ops runs.

Scope of this phase:

- Lift-and-shift orchestration only
- No provider, refresh, publish, or schema logic changes
- Keep domain-separated flows/deployments for targeted runs and debugging

## Flow wrappers

Prefect `@flow` wrappers are in:

- [`flows/prefect_dataops_daily.py`](/home/franciscosantos/finance-data-ops/flows/prefect_dataops_daily.py)

Wrapped domains:

- `dataops_market_daily`
- `dataops_fundamentals_daily`
- `dataops_earnings_daily`
- `dataops_ticker_backfill`

Default flow-level retries (orchestration only):

- Market: `retries=2`, `retry_delay_seconds=300`
- Fundamentals: `retries=1`, `retry_delay_seconds=900`
- Earnings: `retries=2`, `retry_delay_seconds=600`
- Ticker backfill: `retries=0` (step-level failures are surfaced immediately with alerts)

These wrappers delegate to existing orchestration functions:

- `run_dataops_market_daily`
- `run_dataops_fundamentals_daily`
- `run_dataops_earnings_daily`

Local execution examples:

```bash
python flows/prefect_dataops_daily.py market --region us --lookback-days 400
python flows/prefect_dataops_daily.py fundamentals --region eu
python flows/prefect_dataops_daily.py earnings --region apac --history-limit 12
python flows/prefect_dataops_daily.py ticker-backfill --ticker AAPL
```

## Deployments

Deployment definitions are version-controlled in:

- [`prefect.yaml`](/home/franciscosantos/finance-data-ops/prefect.yaml)

Base deployments:

- `market-daily` (args: `symbols`, `start`, `end`, `lookback_days`)
- `fundamentals-daily` (args: `symbols`)
- `earnings-daily` (args: `symbols`, `history_limit`)
- `ticker-backfill` (args: `ticker`, optional `start`, `end`, `history_limit`)

Region-aware deployments:

- Market: `market-us`, `market-eu`, `market-apac`
- Fundamentals: `fundamentals-us`, `fundamentals-eu`, `fundamentals-apac`
- Earnings: `earnings-us`, `earnings-eu`, `earnings-apac`

Each regional deployment uses a local timezone schedule and optional region symbol universe resolution.

## Ticker onboarding backfill

Ticker backfill flow behavior:

1. market backfill for one ticker (default window: last 5 years, override with `start`/`end`)
2. earnings backfill for one ticker (default `history_limit=24`)
3. fundamentals refresh for one ticker

This flow is idempotent by relying on existing domain dedupe/upsert logic.

Backfill status tracking:

- local cache table: `ticker_backfill_status.parquet` (latest row per ticker)
- fields include `ticker`, `status`, `failed_step`, `last_success_at`, and run metadata
- durability note: this is local worker storage for Phase 1; move to a shared
  persistent operational surface in a follow-up (for multi-worker HA visibility)

Trigger options when a ticker is added:

- Backend/API call that emits Prefect custom event `dataops.ticker.added`
- DB-triggered integration (for example Supabase webhook) that emits the same event

Helper script:

```bash
python scripts/emit_ticker_added_event.py AAPL --region us
```

The `ticker-backfill` deployment in `prefect.yaml` includes an event trigger:

- expects: `dataops.ticker.added`
- maps normalized payload values into parameters:
  - `ticker <- event.payload.ticker`
  - `region <- event.payload.region`
  - `start <- event.payload.start`
  - `end <- event.payload.end`
  - `history_limit <- event.payload.history_limit`
- `event.resource.id` is still emitted as plain uppercase ticker and used for event identity/tracing

Ticker normalization contract:

- accepted format: plain uppercase symbol (for example: `AAPL`, `BRK.B`, `RDS-A`)
- rejected: prefixed or mixed-format identifiers (for example: `ticker:AAPL`, `us/AAPL`)

## Batch-add behavior and queueing

High-volume ticker adds are buffered by Prefect queueing:

- deployment: `ticker-backfill`
- queue: `ticker-backfill`
- deployment concurrency limit: `4` with `ENQUEUE` collision strategy

When many `dataops.ticker.added` events arrive in a short interval, runs queue
instead of executing all at once, which limits provider load.

## Region symbol universes

Flow wrappers resolve symbols in this order:

1. Deployment parameter `symbols`
2. Region-specific env var (`DATA_OPS_SYMBOLS_US`, `DATA_OPS_SYMBOLS_EU`, `DATA_OPS_SYMBOLS_APAC`)
3. Default `DATA_OPS_SYMBOLS`

This enables separate symbol sets per region without changing domain logic.

## Work pool and worker

Default work pool name: `dataops-pool` (process worker).

Bootstrap:

```bash
pip install -e ".[dev,orchestration]"
./scripts/prefect_bootstrap.sh
```

Start worker in your own infra (required):

```bash
prefect worker start --pool dataops-pool
```

This design avoids Prefect-hosted execution and keeps secrets/network access in your environment.

## Environment and secrets

Configure on worker host and/or Prefect blocks:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `DATA_OPS_SYMBOLS`
- `DATA_OPS_MAX_ATTEMPTS`
- `DATA_OPS_ALERT_WEBHOOK_URL`

Optional region overrides:

- `DATA_OPS_SYMBOLS_US`
- `DATA_OPS_SYMBOLS_EU`
- `DATA_OPS_SYMBOLS_APAC`

## Automations

Automation templates are in:

- [`orchestration/prefect/automations.yaml`](/home/franciscosantos/finance-data-ops/orchestration/prefect/automations.yaml)

Included templates:

- flow failure alert
- repeated failures escalation
- missed schedule alert (`Late`)
- long-running/stuck flow alert

Apply:

```bash
prefect automation create --from-file orchestration/prefect/automations.yaml
```

If the Prefect workspace hosts non-Data Ops workflows, add resource filters
to the automation definitions before applying.

## GitHub Actions scope after migration

GitHub Actions daily workflows remain for manual backfills/debug only (`workflow_dispatch`):

- `.github/workflows/daily_market_refresh.yml`
- `.github/workflows/daily_fundamentals_refresh.yml`
- `.github/workflows/daily_earnings_refresh.yml`

CI remains in `.github/workflows/ci.yml`.

## Not included in this phase

- Per-symbol/provider granular retries
- Coverage-threshold alerting policy
- 5-year historical backfill expansions for fundamentals/earnings
