# finance-data-ops

Data Ops service for Finance product-data domains.

## Ownership boundary

`finance-data-ops` owns:

- External provider fetching
- Canonical refresh + normalization pipelines for:
  - market data
  - fundamentals
  - earnings
- Raw/source provider cache publication
- Freshness, coverage, and operational run-status publication
- Supabase publication for owned surfaces

Owned Supabase surfaces:

- Market:
  - `source_cache.market_price_daily`
- Fundamentals:
  - `source_cache.fundamentals`
- Earnings:
  - `source_cache.earnings`
- Feature-store read models built after onboarding/backfill:
  - `feature_store.technical_features_daily`
  - `feature_store.scorecard_daily`
  - `feature_store.ticker_page_summary`
  - `feature_store.entity_attributes_static`
  - `feature_store.ticker_readiness`
- Entity identity V0 side-by-side tables:
  - `source_cache.openfigi_mapping_raw`
  - `source_cache.gleif_entity_raw`
  - `source_cache.listing_isin_raw`
  - `source_cache.gleif_isin_lei_raw`
  - `source_cache.gleif_lei_isin_raw`
  - `feature_store.entity_master`
  - `feature_store.entity_listing`
  - `feature_store.entity_identity_audit`
- Operational:
  - `data_source_runs`
  - `data_asset_status`
  - `symbol_data_coverage`
  - `ticker_registry` (validation + promotion status)

`Finance` (research/signals repo) owns:

- research
- training
- backtests
- live inference
- signal publication

## Environment contract

Required for publish runs:

- `DATA_OPS_DATABASE_URL`

Optional:

- `DATA_OPS_LOOKBACK_DAYS` (default `400`)
- `DATA_OPS_MAX_ATTEMPTS` (default `3`)
- `DATA_OPS_SYMBOL_BATCH_SIZE` (default `100`)
- `DATA_OPS_CACHE_ROOT` (default `./data_cache`)
- `DATA_OPS_ALERT_WEBHOOK_URL` (critical failure webhook)
- `DATA_OPS_SYMBOLS_OVERRIDE` for emergency/local source-refresh subsets
- `DATA_OPS_SYMBOLS_OVERRIDE_US` / `DATA_OPS_SYMBOLS_OVERRIDE_EU` / `DATA_OPS_SYMBOLS_OVERRIDE_APAC` for region-specific emergency/local subsets

See [`.env.example`](/home/franciscosantos/finance-data-ops/.env.example).

## Secret management

- GCP runtime secrets should come from Google Secret Manager (Cloud Run `--set-secrets`).
- Vercel-side shared secrets/config should use Vercel Shared Environment Variables.
- Do not commit `.env` or concrete worker env files. Use:
  - [`.env.example`](/home/franciscosantos/finance-data-ops/.env.example)
  - [`worker.env.template.yaml`](/home/franciscosantos/finance-data-ops/worker.env.template.yaml)
- Prefer Prefect deployment auth for ticker lifecycle operations; use `WORKER_SHARED_TOKEN` only for the analysis worker.

## Manual runs

Market:

```bash
python scripts/run_market_daily.py --region us --no-publish
```

Fundamentals:

```bash
python scripts/run_fundamentals_daily.py --region all --no-publish
```

Earnings:

```bash
python scripts/run_earnings_daily.py --region all --no-publish
```

Source-universe audit/reconciliation:

```bash
python scripts/reconcile_source_refresh_universe.py --fail-on-issues
python scripts/reconcile_source_refresh_universe.py --apply
```

Ticker validation (operator direct flow; backend lifecycle requests use Prefect deployments):

```bash
python scripts/run_ticker_validation.py ANZ --region apac --instrument-type-hint equity --no-publish
```

Status check:

```bash
python scripts/validate_market_status.py
```

Entity identity V0 dry-run:

```bash
python scripts/build_entity_identity.py --source fixtures
python scripts/build_entity_identity.py --source postgres --symbols SAP,SAP.DE --offline
python scripts/measure_entity_identity_chain.py --source fixtures
```

Entity Layer V0 is side-by-side only. OpenFIGI is the main listing/security identity source; GLEIF/LEI is optional enrichment and is not required for V0. `feature_store.entity_attributes_static` remains a metadata read model and must not be treated as entity master. No product/read path uses `feature_store.entity_master` or `feature_store.entity_listing` yet, no command autonomously onboards symbols, and no price series are merged across listings. Future consumers should migrate only after the entity layer has been validated.

OpenFIGI ticker mapping is not sufficient by itself for company/entity grouping. V0 treats ticker-mapping FIGIs as listing/security identity and emits audit rows when company-level identity is missing. V0.2 measures the concrete `anchor listing -> ISIN -> LEI -> expanded LEI ISIN set -> sibling listing attach` path using validated provider ISIN anchors, GLEIF ISIN-to-LEI lookup, and paginated GLEIF `GET /api/v1/lei-records/{LEI}/isins` expansion. GLEIF coverage is partial, and pairs with no valid sibling anchor remain blocked. `--apply-cache` on the measurement command may write raw cache rows only; entity tables remain unpublished until the acceptance set proves the chain.

## Prefect orchestration

Prefect Cloud is the primary scheduler/orchestrator for daily domain refreshes.

- Prefect flow wrappers (orchestration only):
  - [flows/prefect_dataops_daily.py](/home/franciscosantos/finance-data-ops/flows/prefect_dataops_daily.py)
  - `dataops_market_daily`
  - `dataops_fundamentals_daily`
  - `dataops_earnings_daily`
  - `dataops_macro_daily`
  - `dataops_release_calendar_daily`
  - `dataops_ticker_backfill` (targeted single-ticker backfill)
  - `dataops_ticker_validation` (on-demand symbol normalization + validation)
  - `dataops_ticker_onboarding` (event-driven validation gate + conditional backfill)
  - `dataops_ticker_remove` (on-demand lifecycle reject/remove)
  - `dataops_daily` (aggregate source refresh + feature-store handoff)
- Deployment definitions:
  - [prefect.yaml](/home/franciscosantos/finance-data-ops/prefect.yaml)
  - Includes source, aggregate, production, and onboarding deployments including `dataops-daily`, `market-daily`, `fundamentals-daily`, `earnings-daily`, `macro-daily`, `release-calendar-daily`, `ticker-validation`, `ticker-onboarding`, `ticker-backfill`, and `ticker-remove`
  - Region is handled via deployment parameters/flow logic (`region`) instead of per-region deployments
  - Scheduled source refresh symbols come from active, promoted, market-supported `ticker_registry` rows.
  - Deployment `symbols` parameters are manual one-off subsets and always win.
  - `DATA_OPS_SYMBOLS_OVERRIDE*` variables are emergency/local subset overrides, not the production universe.
  - `feature_store.ticker_readiness` is the product/search tracked universe; it is audited against `ticker_registry` but is not used directly as the source refresh scheduler universe.
  - Feature-store handoff is config-driven: `FEATURE_BUILD_DAILY_DEPLOYMENT` defaults to `feature-build-daily/feature-build-daily`; targeted onboarding scorecard builds use `FEATURE_SCORECARD_BUILD_DEPLOYMENT`, defaulting to `scorecard-daily/scorecard-daily`.
  - Cadence strategy (weekday UTC):
    - Aggregate source handoff: `23:10`
    - Market: `06:30`, `14:30`, `22:30` (higher freshness priority)
    - Earnings: `08:00`, `20:00` (medium freshness priority)
    - Fundamentals: `03:00` (low-change domain, daily is sufficient)
    - Macro: `06:15`, `14:45`, `22:45`
    - Release calendar: `05:00`, `15:00`
    - Ticker onboarding: event-driven only (`dataops.ticker.added`)
    - Ticker backfill: no schedule (invoked only after onboarding promotion)
    - Ticker validation: no schedule (invoked by Prefect onboarding or explicit operator deployment run)
    - Ticker remove: no schedule (backend/operator-triggered Prefect deployment)
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

Emit ticker-added event (triggers `ticker-onboarding` deployment):

```bash
python scripts/emit_ticker_added_event.py AAPL --region us
```

Or submit onboarding directly to the deployment (backend/API entrypoint):

```bash
python scripts/submit_ticker_onboarding.py AAPL --region us
```

Normalization config used by ticker validation:

- [config/symbol_normalization.yml](/home/franciscosantos/finance-data-ops/config/symbol_normalization.yml)

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

## SQL baseline

Fresh Supabase projects should use the definitive runtime baseline:

- Schema: [`sql/000_runtime_schema.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_schema.sql)
- Seed: [`sql/000_runtime_seed.sql`](/home/franciscosantos/finance-data-ops/sql/000_runtime_seed.sql)

## Additional docs

- Architecture: [`docs/architecture.md`](/home/franciscosantos/finance-data-ops/docs/architecture.md)
- Schema contract: [`docs/schema_contract.md`](/home/franciscosantos/finance-data-ops/docs/schema_contract.md)
- Operations runbook: [`docs/operations.md`](/home/franciscosantos/finance-data-ops/docs/operations.md)
- Migrations runbook: [`docs/migrations.md`](/home/franciscosantos/finance-data-ops/docs/migrations.md)
- Prefect orchestration: [`docs/prefect_orchestration.md`](/home/franciscosantos/finance-data-ops/docs/prefect_orchestration.md)
- Parity rules: [`docs/parity_rules.md`](/home/franciscosantos/finance-data-ops/docs/parity_rules.md)
- Thematic sources & relationship map: [`docs/thematic-sources-and-relationship-map.md`](/home/franciscosantos/finance-data-ops/docs/thematic-sources-and-relationship-map.md)
