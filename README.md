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
python scripts/publish_entity_identity_side_by_side.py --source fixtures
```

Entity Layer V0 is side-by-side only. OpenFIGI is the main listing/security identity source; GLEIF/LEI is optional enrichment and is not required for V0. `feature_store.entity_attributes_static` remains a metadata read model and must not be treated as entity master. No product/read path uses `feature_store.entity_master` or `feature_store.entity_listing` yet, no command autonomously onboards symbols, and no price series are merged across listings. Future consumers should migrate only after the entity layer has been validated.

OpenFIGI ticker mapping is not sufficient by itself for company/entity grouping. V0 treats ticker-mapping FIGIs as listing/security identity and emits audit rows when company-level identity is missing. V0.2 measures provider ISIN anchors through `ISIN -> LEI -> expanded LEI ISIN set`, using paginated GLEIF `GET /api/v1/lei-records/{LEI}/isins`. Prefix-compatible provider ISINs remain the main direct anchor path and attach as `isin_direct`/high. If a valid raw provider ISIN has a country prefix that differs from the listing country, the main pipeline still treats it as suspect so the established legalName anchor path is unchanged; a separate isolated candidate may attach it as `isin_direct_prefix_mismatch_name_confirmed`/high only after GLEIF returns a LEI and the listing/OpenFIGI/internal name matches the GLEIF legal name under conservative normalization. Failed prefix-mismatch candidates do not suppress legalName fallback. `name_anchor_confirmed` remains fail-closed and medium confidence because it requires exact conservative name normalization, compatible country/address context, one surviving LEI, acceptable GLEIF entity state, and compatible expanded ISINs. Missing geography or no compatible expanded ISIN goes to review or the provider/curated tail instead of accepting name-only grouping.

Publication readiness is gated by full audit output for every non-direct attach, including `lei_expansion`, `name_anchor_confirmed`, `foreign_issuer_name_anchor_confirmed`, `isin_direct_prefix_mismatch_name_confirmed`, and `curated_identity`. The audit records normalized listing/legal names, deterministic ISIN/LEI support, group symbols, conflict flags, confidence, provenance, and review status. CJK/APAC legal names preserve distinctive non-Latin tokens and only strip true legal-form suffixes; distinctive-token removal always requires review, while CJK/acronym collapse and short/acronym-only heuristic name matches are machine-safe only with bidirectional deterministic support: the name candidate points to a LEI and the listing's own or matched compatible ISIN forward-resolves to the same LEI. Heuristic attaches without bidirectional support are routed to review unless a version-controlled `reviewed_safe` decision is supplied. Side-by-side publication is allowed only when every heuristic attach is reviewed or machine-verifiably safe. Version-controlled curated identities live in `data/entity_identity_curated.json`; they support reusable symbol-to-LEI decisions with evidence metadata and publish as explicit `curated_identity` provenance, not as name anchors. Provisional single-listing candidates can be retained as low-confidence evidence under symbol-scoped provisional ids; they are not treated as confirmed entity merges and do not block publication. Re-evaluation is driven by cache/evidence changes such as new listings, OpenFIGI refreshes, provider ISIN changes, new GLEIF ISIN mappings, new LEI expansion ISINs, legal-name candidate changes, manual review decisions, or corroborating provider evidence. Measurement output classifies unattached listings into `fixable_free`, `requires_provider_or_curated_identity`, and `needs_manual_review`, includes listing-group profiling (`single_listing` vs `multi_listing_candidate`), caps expanded ISIN lists to samples, and includes precision/publication gate views. `--apply-cache` on the measurement command may write raw cache rows only; entity tables remain unpublished by the measurement command.

Controlled side-by-side entity publication is cache-first and dry-run by default:

```bash
python scripts/reconcile_entity_identity_schema.py
python scripts/reconcile_entity_identity_schema.py --apply
python scripts/publish_entity_identity_side_by_side.py --source postgres
python scripts/publish_entity_identity_side_by_side.py --source postgres --tracked-only --offline --use-raw-cache
python scripts/publish_entity_identity_side_by_side.py --source postgres --refresh-live --gleif-request-sleep-seconds 7
python scripts/publish_entity_identity_side_by_side.py --source postgres --tracked-only --use-raw-cache --refresh-live --refresh-cache-misses --gleif-request-sleep-seconds 7 --apply-caches
python scripts/publish_entity_identity_side_by_side.py --source postgres --refresh-live --gleif-request-sleep-seconds 7 --apply-caches
python scripts/publish_entity_identity_side_by_side.py --source postgres --refresh-live --gleif-request-sleep-seconds 7 --apply-caches --apply-entities --batch-id entity-wave-reviewed-YYYYMMDD
python scripts/run_post_onboard_entity_identity_refresh.py --source postgres --scope-key tracked
python scripts/run_post_onboard_entity_identity_refresh.py --source postgres --scope-key tracked --batch-id tracked-entity-refresh-YYYYMMDD-HHMMSS --apply-entities
python scripts/audit_frontier_entity_dedup.py --source postgres --scope-key tracked --symbols SAP.DE,NOVO-B.CO
```

The publish command defaults to no live API refresh; `--refresh-live` is required to call OpenFIGI/yfinance/GLEIF, and the GLEIF/OpenFIGI throttle flags should be set for broader scopes. `--offline --use-raw-cache` reads existing `source_cache.*_raw` facts and reports explicit `cache_miss` gaps without calling providers or writing data. `source_cache.gleif_entity_raw` is the legal-name search cache keyed by conservative `normalized_query_name`; it stores success, not-found, ambiguous, error, and rate-limited legal-name outcomes so cached full-universe dry-runs can exercise the name-anchor path. `--curated-identity-file` can point to reviewed symbol-to-LEI and reviewed-safe heuristic decisions; the default is `data/entity_identity_curated.json`. `--tracked-only` scopes Postgres candidates to `feature_store.ticker_readiness.is_tracked = true`, which is the intended full product universe for entity identity measurement. Cache fill for missing raw facts is explicit: use `--use-raw-cache --refresh-live --refresh-cache-misses --apply-caches`; this writes only raw cache tables and leaves entity tables untouched. Entity writes are blocked unless the publication gate is green, unresolved multi-listing entities are zero, group conflicts are zero, and heuristic review-required rows are zero for the measured scope. The command writes raw cache tables before side-by-side entity tables when both apply flags are present. Raw cache writes are idempotent upserts by natural cache key; if a later cache table fails after earlier cache upserts committed, rerun the same command after fixing the issue. Synthetic `cache_miss` diagnostic rows are not written as raw facts; cached negative GLEIF outcomes are raw facts and are reused by offline runs. Transient GLEIF 429/timeout/transport errors remain retryable `rate_limited`/`error` facts and are not converted to `not_found` negatives. Publication batches are idempotent by `batch_id`, entity rows upsert by `entity_id`/`symbol`, and the current pointer is replaceable by `scope_key`. This data-ops command does not change product path code; pointer-based consumers continue to follow their configured Entity Layer scope.
`run_post_onboard_entity_identity_refresh.py` is the post-onboarding wrapper for the growing `tracked` scope: it always measures the current `feature_store.ticker_readiness.is_tracked = true` universe in Postgres mode, reuses the same cache-first publisher/gate, and advances only the `tracked` current pointer after `--apply-entities` succeeds. The no-schedule Prefect deployment is `dataops_entity_identity_refresh/entity-identity-refresh`; Track B should trigger it explicitly after onboarding waves.
`audit_frontier_entity_dedup.py` is the read-only pre-onboard dedup audit: it resolves candidate symbols with the same cache-first identity path, reads the current `tracked` Entity Layer pointer, and classifies candidates as `already_tracked_entity`, `new_entity_candidate`, `provisional_or_unresolved`, `needs_review`, or `cache_miss` with a recommended action. It does not suppress or write anything by itself.

Published `entity_master.home_country` can be backfilled cache-only from existing GLEIF raw cache without changing mappings or current pointers:

```bash
python scripts/backfill_entity_home_country.py --batch-id tracked-675-first-publish
python scripts/backfill_entity_home_country.py --batch-id tracked-675-first-publish --apply
```

The command updates only null/blank `home_country` values for resolved rows in the selected publication batch and records source evidence in `metadata.home_country_backfill`.

Post-publish verification examples for the server operator:

```sql
select batch_id, status, is_current, planned_counts, actual_counts
from feature_store.entity_identity_publication_batch
order by created_at desc
limit 5;

select attach_method, review_state, count(*)
from feature_store.entity_listing
group by attach_method, review_state
order by attach_method, review_state;

select symbol, entity_id, attach_method, attach_confidence, review_state
from feature_store.entity_listing
where symbol in ('SAP', 'SAP.DE', 'CSL.AX');

select entity_id, array_agg(symbol order by symbol) as listings
from feature_store.entity_listing
group by entity_id
having count(*) > 1
order by entity_id;
```

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
  - `dataops_entity_identity_refresh` (explicit post-onboarding Entity Layer refresh)
  - `dataops_daily` (aggregate source refresh + feature-store handoff)
- Deployment definitions:
  - [prefect.yaml](/home/franciscosantos/finance-data-ops/prefect.yaml)
  - Includes source, aggregate, production, and onboarding deployments including `dataops-daily`, `market-daily`, `fundamentals-daily`, `earnings-daily`, `macro-daily`, `release-calendar-daily`, `ticker-validation`, `ticker-onboarding`, `ticker-backfill`, `ticker-remove`, and `entity-identity-refresh`
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
    - Entity identity refresh: no schedule (Track B/operator-triggered after onboarding waves)
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
