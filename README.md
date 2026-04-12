# finance-data-ops

Data Ops service for Finance product-data domains.

## Ownership boundary

`finance-data-ops` owns:

- External provider fetching
- Canonical refresh + normalization pipelines for:
  - market data
  - fundamentals
  - earnings
- Frontend-serving summary surfaces derived from those datasets
- Freshness, coverage, and operational run-status publication
- Supabase publication for owned surfaces

Owned Supabase surfaces:

- Market:
  - `market_price_daily`
  - `market_quotes`
  - `market_quotes_history`
  - `mv_latest_prices` (refresh RPC)
  - `ticker_market_stats_snapshot`
- Fundamentals:
  - `market_fundamentals_v2`
  - `mv_latest_fundamentals`
  - `ticker_fundamental_summary`
- Earnings:
  - `market_earnings_events`
  - `market_earnings_history`
  - `mv_next_earnings`
- Operational:
  - `data_source_runs`
  - `data_asset_status`
  - `symbol_data_coverage`

`Finance` (research/signals repo) owns:

- research
- training
- backtests
- live inference
- signal publication

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

Market:

```bash
python scripts/run_market_daily.py --no-publish
```

Fundamentals:

```bash
python scripts/run_fundamentals_daily.py --no-publish
```

Earnings:

```bash
python scripts/run_earnings_daily.py --no-publish
```

Status check:

```bash
python scripts/validate_market_status.py
```

## GitHub Actions automation

Production workflows now cover all three daily domains:

- Market: [`.github/workflows/daily_market_refresh.yml`](/home/franciscosantos/finance-data-ops/.github/workflows/daily_market_refresh.yml)
  - Purpose: price window backfills and daily quote refresh.
  - Triggers: weekday schedule + `workflow_dispatch`.
  - Inputs: optional `symbols`, `lookback_days`, `start`, `end`.
- Fundamentals: [`.github/workflows/daily_fundamentals_refresh.yml`](/home/franciscosantos/finance-data-ops/.github/workflows/daily_fundamentals_refresh.yml)
  - Purpose: provider-history fundamentals refresh/publish.
  - Triggers: weekday schedule + `workflow_dispatch`.
  - Inputs: optional `symbols`.
- Earnings: [`.github/workflows/daily_earnings_refresh.yml`](/home/franciscosantos/finance-data-ops/.github/workflows/daily_earnings_refresh.yml)
  - Purpose: next-event refresh plus bounded historical rows (`history_limit`).
  - Triggers: weekday schedule + `workflow_dispatch`.
  - Inputs: optional `symbols`, optional `history_limit`.

Each workflow remains independently runnable for targeted backfills and debugging.

Project aggregation (same pattern as `Finance` repo):

```bash
python scripts/run_project_aggregation.py --mode no-tests --ext .py .toml .md
```

## SQL migrations

- v1: [`sql/001_data_ops_v1_surfaces.sql`](/home/franciscosantos/finance-data-ops/sql/001_data_ops_v1_surfaces.sql)
- v2: [`sql/002_data_ops_v2_fundamentals_earnings.sql`](/home/franciscosantos/finance-data-ops/sql/002_data_ops_v2_fundamentals_earnings.sql)

## Additional docs

- Architecture: [`docs/architecture.md`](/home/franciscosantos/finance-data-ops/docs/architecture.md)
- Schema contract: [`docs/schema_contract.md`](/home/franciscosantos/finance-data-ops/docs/schema_contract.md)
- Operations runbook: [`docs/operations.md`](/home/franciscosantos/finance-data-ops/docs/operations.md)
- Migrations runbook: [`docs/migrations.md`](/home/franciscosantos/finance-data-ops/docs/migrations.md)
