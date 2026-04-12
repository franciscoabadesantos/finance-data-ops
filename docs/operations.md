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

Each flow performs refresh, derived summary generation, Supabase publish, and status/coverage updates.

GitHub Actions production automation (weekday schedule + manual dispatch):

- Market workflow: [`.github/workflows/daily_market_refresh.yml`](/home/franciscosantos/finance-data-ops/.github/workflows/daily_market_refresh.yml)
  - Price-window backfills and daily quote refresh.
  - Optional manual inputs: `symbols`, `lookback_days`, `start`, `end`.
- Fundamentals workflow: [`.github/workflows/daily_fundamentals_refresh.yml`](/home/franciscosantos/finance-data-ops/.github/workflows/daily_fundamentals_refresh.yml)
  - Provider-history fundamentals refresh.
  - Optional manual input: `symbols`.
- Earnings workflow: [`.github/workflows/daily_earnings_refresh.yml`](/home/franciscosantos/finance-data-ops/.github/workflows/daily_earnings_refresh.yml)
  - Next-event refresh plus bounded history via `history_limit`.
  - Optional manual inputs: `symbols`, `history_limit`.

Per-domain workflows are intentionally independent for targeted backfills and debugging.

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

## Manual operations

Fundamentals dry run:

```bash
python scripts/run_fundamentals_daily.py --symbols SPY,QQQ --no-publish
```

Earnings dry run:

```bash
python scripts/run_earnings_daily.py --symbols SPY,QQQ --no-publish
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

## Failure triage

1. Check workflow/script logs for refresh or publish step failures.
2. Inspect `data_source_runs.error_messages` and `failure_classification`.
3. Inspect `data_asset_status.reason`, `freshness_status`, and `coverage_status`.
4. Inspect `symbol_data_coverage.reason` for missing domain components.
5. If configured, verify alert webhook delivery for unhealthy runs.
