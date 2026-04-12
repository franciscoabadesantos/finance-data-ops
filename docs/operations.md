# Operations runbook (Data Ops v2)

## Daily jobs

Market:

```bash
python scripts/run_market_daily.py
```

Fundamentals:

```bash
python scripts/run_fundamentals_daily.py
```

Earnings:

```bash
python scripts/run_earnings_daily.py
```

Ticker onboarding orchestration (Prefect wrapper flow):

```bash
python flows/prefect_dataops_daily.py ticker-onboarding --input-symbol AAPL --region us
```

Targeted ticker backfill (manual/debug):

```bash
python flows/prefect_dataops_daily.py ticker-backfill --ticker AAPL
```

Ticker symbol validation (Prefect wrapper flow):

```bash
python flows/prefect_dataops_daily.py ticker-validation --input-symbol ANZ --region apac --instrument-type-hint equity --no-publish
```

Daily domain flows perform refresh, derived summary generation, Supabase publish, and status/coverage updates.
Ticker onboarding flow creates `pending_validation`, runs ticker validation, and triggers backfill only for promotable symbols.
Production universes are registry-driven; avoid direct manual edits to runtime region symbol lists except temporary fallback scenarios.

Primary scheduler/orchestrator: Prefect Cloud managed execution (`prefect.yaml` deployments + `dataops-managed-pool`).

Prefect deployments (6 total):

- Market:
  - `market-daily`
- Fundamentals:
  - `fundamentals-daily`
- Earnings:
  - `earnings-daily`
- Ticker onboarding:
  - `ticker-onboarding` (event-driven)
- Ticker validation:
  - `ticker-validation` (on-demand)
- Targeted backfill:
  - `ticker-backfill` (on-demand, invoked by onboarding after promotion)

Cadence (weekday UTC):

- Market `market-daily`: `06:30`, `14:30`, `22:30` (higher frequency for user-facing freshness)
- Earnings `earnings-daily`: `08:00`, `20:00` (medium frequency)
- Fundamentals `fundamentals-daily`: `03:00` (slow-moving domain)
- Ticker onboarding `ticker-onboarding`: event-driven only
- Ticker validation `ticker-validation`: webhook/API-invoked from onboarding path (no schedule)
- Ticker backfill `ticker-backfill`: no schedule (triggered by onboarding decision or manual debug run)

Manual backfills/debugging remain available in GitHub Actions via `workflow_dispatch`:

- [`.github/workflows/daily_market_refresh.yml`](/home/franciscosantos/finance-data-ops/.github/workflows/daily_market_refresh.yml)
- [`.github/workflows/daily_fundamentals_refresh.yml`](/home/franciscosantos/finance-data-ops/.github/workflows/daily_fundamentals_refresh.yml)
- [`.github/workflows/daily_earnings_refresh.yml`](/home/franciscosantos/finance-data-ops/.github/workflows/daily_earnings_refresh.yml)

Ticker-added event trigger helper:

```bash
python scripts/emit_ticker_added_event.py AAPL --region us
```

Direct deployment submitter helper (backend/API path):

```bash
python scripts/submit_ticker_onboarding.py AAPL --region us
```

Backfill queueing defaults:

- deployment `ticker-onboarding` concurrency limit is `4` (`ENQUEUE`) to absorb burst ticker adds
- deployment `ticker-backfill` concurrency limit is `4` (`ENQUEUE`) to protect provider load when onboarding bursts occur
- region is passed per-run via `region` parameter/event payload (not separate deployments)

Validation queueing defaults:

- deployment `ticker-validation` concurrency limit is `8` (`ENQUEUE`) to validate symbol candidates in parallel
- market validation includes quote publish-safety precheck (`price`/`change` required before promotion)
- validation rows are persisted to `ticker_registry` (Supabase when configured; local cache fallback table `ticker_registry.parquet`)

## Required environment

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `DATA_OPS_SYMBOLS`

Optional:

- `DATA_OPS_LOOKBACK_DAYS`
- `DATA_OPS_MAX_ATTEMPTS`
- `DATA_OPS_SYMBOL_BATCH_SIZE`
- `DATA_OPS_CACHE_ROOT`
- `DATA_OPS_ALERT_WEBHOOK_URL`
- `DATA_OPS_SYMBOLS_US` / `DATA_OPS_SYMBOLS_EU` / `DATA_OPS_SYMBOLS_APAC`

## Manual operations

Fundamentals dry run:

```bash
python scripts/run_fundamentals_daily.py --symbols SPY,QQQ --no-publish
```

Earnings dry run:

```bash
python scripts/run_earnings_daily.py --symbols SPY,QQQ --no-publish
```

Ticker validation dry run:

```bash
python scripts/run_ticker_validation.py ANZ --region apac --instrument-type-hint equity --no-publish
```

Market status check:

```bash
python scripts/validate_market_status.py
```

## Healthy run checks

Look for:

- flow exit code `0`
- `data_source_runs` rows for refresh + orchestration jobs
- `data_asset_status` rows for:
  - `market_fundamentals_v2`
  - `ticker_fundamental_summary`
  - `market_earnings_events`
  - `market_earnings_history`
  - `mv_next_earnings`
- `symbol_data_coverage` rows with populated:
  - `fundamentals_available`
  - `fundamentals_last_date`
  - `earnings_available`
  - `next_earnings_date`
- ticker onboarding status cache in `data_cache/ticker_backfill_status.parquet`:
  - `ticker`
  - `status`
  - `failed_step`
  - `last_success_at`
  - note: local durability for Phase 1; promote to shared persistent ops storage in follow-up

## Failure triage

1. Check Prefect deployment/flow-run logs (or manual GitHub workflow logs) for refresh or publish step failures.
2. Inspect `data_source_runs.error_messages` and `failure_classification`.
3. Inspect `data_asset_status.reason`, `freshness_status`, and `coverage_status`.
4. Inspect `symbol_data_coverage.reason` for missing domain components.
5. If configured, verify alert webhook delivery for unhealthy runs.
