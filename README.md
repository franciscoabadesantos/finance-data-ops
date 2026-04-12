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
- `DATA_OPS_SYMBOLS_US` / `DATA_OPS_SYMBOLS_EU` / `DATA_OPS_SYMBOLS_APAC` (region-parameterized runs)

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

## Prefect orchestration

Prefect Cloud is the primary scheduler/orchestrator for daily domain refreshes.

- Prefect flow wrappers (orchestration only):
  - [flows/prefect_dataops_daily.py](/home/franciscosantos/finance-data-ops/flows/prefect_dataops_daily.py)
  - `dataops_market_daily`
  - `dataops_fundamentals_daily`
  - `dataops_earnings_daily`
  - `dataops_ticker_backfill` (event-driven ticker onboarding backfill)
- Deployment definitions:
  - [prefect.yaml](/home/franciscosantos/finance-data-ops/prefect.yaml)
  - Includes exactly 4 deployments: `market-daily`, `fundamentals-daily`, `earnings-daily`, `ticker-backfill`
  - Region is handled via deployment parameters/flow logic (`region`) instead of per-region deployments
  - Cadence strategy (weekday UTC):
    - Market: `06:30`, `14:30`, `22:30` (higher freshness priority)
    - Earnings: `08:00`, `20:00` (medium freshness priority)
    - Fundamentals: `03:00` (low-change domain, daily is sufficient)
    - Ticker backfill: event-driven only (`dataops.ticker.added`)
- Prefect bootstrap script:
  - [scripts/prefect_bootstrap.sh](/home/franciscosantos/finance-data-ops/scripts/prefect_bootstrap.sh)
  - Creates `dataops-managed-pool` (Prefect-managed execution), deploys `prefect.yaml`, and applies automation templates
- Automation templates:
  - [orchestration/prefect/automations.yaml](/home/franciscosantos/finance-data-ops/orchestration/prefect/automations.yaml)

Install orchestration dependencies:

```bash
pip install -e ".[dev,orchestration]"
```

Deploy to Prefect Cloud:

```bash
./scripts/prefect_bootstrap.sh
```

Emit ticker-added event (triggers `ticker-backfill` deployment):

```bash
python scripts/emit_ticker_added_event.py AAPL --region us
```

Ticker backfill concurrency defaults to queued execution (`limit=4`) to protect providers during burst onboarding.
No always-on worker VM is required in this setup.

GitHub Actions remains available for CI and manual domain backfills/debugging via `workflow_dispatch`:

- [daily_market_refresh.yml](/home/franciscosantos/finance-data-ops/.github/workflows/daily_market_refresh.yml)
- [daily_fundamentals_refresh.yml](/home/franciscosantos/finance-data-ops/.github/workflows/daily_fundamentals_refresh.yml)
- [daily_earnings_refresh.yml](/home/franciscosantos/finance-data-ops/.github/workflows/daily_earnings_refresh.yml)

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
- Prefect orchestration: [`docs/prefect_orchestration.md`](/home/franciscosantos/finance-data-ops/docs/prefect_orchestration.md)
