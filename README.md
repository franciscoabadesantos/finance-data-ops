# finance-data-ops

Data Ops Service v1 for the Finance platform. This repository is a standalone operational service.

## Ownership boundary

`finance-data-ops` owns:

- External market data fetching
- Daily market prices and latest quotes refresh
- Price-derived product metrics
- Freshness, coverage, and operational status publication
- Supabase publication for these surfaces:
  - `market_price_daily` (production shape: `ticker,date,close,source,fetched_at,created_at`)
  - `market_quotes` (production shape: `ticker,name,price,change,change_percent,market_cap_text,source,fetched_at,created_at,updated_at`)
  - `market_quotes_history` (production shape: `ticker,fetched_at,price,change,change_percent,market_cap,source`)
  - `mv_latest_prices` (refresh)
  - `ticker_market_stats_snapshot`
  - `data_source_runs`
  - `data_asset_status`
  - `symbol_data_coverage`

`ticker_market_stats_snapshot` is published as a latest snapshot table keyed by `ticker`
(`as_of_date` is informational).

`Finance` (research/signals repo) continues to own:

- Research
- Training
- Backtests
- Promotion
- Live inference
- Signal writes to `market_signals`

## Environment contract

Required for publish runs:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `DATA_OPS_SYMBOLS` (unless passed via `--symbols`)

Optional:

- `DATA_OPS_LOOKBACK_DAYS` (default `400`)
- `DATA_OPS_MAX_ATTEMPTS` (default `3`)
- `DATA_OPS_SYMBOL_BATCH_SIZE` (default `100`)
- `DATA_OPS_CACHE_ROOT` (default `./data_cache`)
- `DATA_OPS_ALERT_WEBHOOK_URL` (critical failure webhook)

See [`.env.example`](/home/franciscosantos/finance-data-ops/.env.example).

## Manual runs

Local dry run (no Supabase writes):

```bash
python scripts/run_market_daily.py \
  --symbols SPY,QQQ,IWM \
  --start 2025-01-01 \
  --end 2026-04-10 \
  --no-publish
```

Publish run (requires env vars):

```bash
python scripts/run_market_daily.py
```

The default run uses `DATA_OPS_SYMBOLS`, `DATA_OPS_LOOKBACK_DAYS`, and current UTC date.

## Daily scheduler

- Workflow: `.github/workflows/daily_market_refresh.yml`
- Trigger: weekdays via cron + manual dispatch
- Action: refresh prices/quotes, compute metrics, publish owned surfaces, publish status rows

## Validate status

```bash
python scripts/validate_market_status.py
```

Exit code is non-zero when core assets are unhealthy.

## Additional docs

- Architecture: [`docs/architecture.md`](/home/franciscosantos/finance-data-ops/docs/architecture.md)
- Schema contract: [`docs/schema_contract.md`](/home/franciscosantos/finance-data-ops/docs/schema_contract.md)
- Operations runbook: [`docs/operations.md`](/home/franciscosantos/finance-data-ops/docs/operations.md)
- Migrations runbook: [`docs/migrations.md`](/home/franciscosantos/finance-data-ops/docs/migrations.md)
