# Operations runbook (Data Ops v3)

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

Macro:

```bash
python scripts/run_macro_daily.py
```

Economic release calendar:

```bash
python scripts/run_release_calendar_daily.py
```

Historical backfill (idempotent):

```bash
python scripts/run_macro_backfill.py --start-date 2000-01-01 --end-date 2026-04-13 --force-recompute
python scripts/run_release_calendar_backfill.py --start-date 2000-01-01 --end-date 2026-04-13 --force-recompute
python scripts/run_macro_release_backfill.py --start-date 2000-01-01 --end-date 2026-04-13 --force-recompute
```

Parity gates:

```bash
python scripts/check_macro_parity.py --start-date 2020-01-01 --end-date 2026-04-13
python scripts/check_release_calendar_parity.py --start-date 2020-01-01 --end-date 2026-04-13
```

## Required environment

- `SUPABASE_URL`
- `SUPABASE_SECRET_KEY`
- `DATA_OPS_SYMBOLS`

Optional:

- `DATA_OPS_LOOKBACK_DAYS`
- `DATA_OPS_MAX_ATTEMPTS`
- `DATA_OPS_SYMBOL_BATCH_SIZE`
- `DATA_OPS_CACHE_ROOT`
- `DATA_OPS_ALERT_WEBHOOK_URL`
- `DATA_OPS_SYMBOLS_US` / `DATA_OPS_SYMBOLS_EU` / `DATA_OPS_SYMBOLS_APAC`

## Healthy run checks

Look for:

- flow exit code `0`
- `data_source_runs` rows for refresh + orchestration jobs
- `data_asset_status` rows for:
  - `macro_observations`
  - `macro_daily`
  - `economic_release_calendar`
  - existing market/fundamentals/earnings assets
- `symbol_data_coverage` rows remain populated for symbol-scoped domains

## Runtime source-of-truth policy

For macro and release calendar domains:

- canonical source: `finance-data-ops` tables (`macro_*`, `economic_release_calendar`)
- migration fallback only: legacy `Finance` read logic (feature-flag controlled)
- `Finance/configs/release_calendars/*` is not part of post-cutover runtime logic

## Request-driven async jobs

Ticker validation/backfill deployment and cutover (Cloud Tasks + Cloud Run) runbook:

- [`docs/cloud_tasks_cloud_run_deployment.md`](/home/franciscosantos/finance-data-ops/docs/cloud_tasks_cloud_run_deployment.md)

## Worker runtime floor

For the shared Cloud Run worker (`finance-jobs-worker`), keep:

- memory: `1Gi`
- timeout: `300s`

Operational reason:

- historical earnings region rebuilds OOM-kill the worker at `512Mi`
- the failure presents as Cloud Tasks retries plus worker shutdowns around
  batch 3, not as an explicit application error
- `1Gi` resolved the issue and is now the minimum safe production floor
