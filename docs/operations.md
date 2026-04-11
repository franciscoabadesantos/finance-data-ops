# Operations runbook (Data Ops v1)

## What runs daily

Scheduler runs:

```bash
python scripts/run_market_daily.py
```

This performs:

1. daily market price refresh
2. latest quote refresh
3. freshness/coverage evaluation
4. product metrics computation
5. Supabase publication for owned surfaces
6. status publication to `data_source_runs`, `data_asset_status`, `symbol_data_coverage`

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

Dry run:

```bash
python scripts/run_market_daily.py --symbols SPY,QQQ --no-publish
```

Backfill window:

```bash
python scripts/run_market_daily.py --symbols SPY,QQQ --start 2025-01-01 --end 2026-04-10
```

Status check:

```bash
python scripts/validate_market_status.py
```

## Healthy run checks

Look for:

- flow exit code `0`
- `data_source_runs` rows for `market_price_daily`, `market_quotes`, and `dataops_market_daily`
- `data_asset_status` rows showing `fresh` or acceptable tolerance
- `symbol_data_coverage` rows with expected symbols and non-empty reasons

## Failure triage

1. Check workflow logs (`daily_market_refresh.yml`) for publish/refresh step failure.
2. Inspect `data_source_runs.error_messages` and `failure_classification`.
3. Inspect `data_asset_status.details` and `symbol_data_coverage.reason`.
4. If configured, verify webhook alert delivery for the failure run.
