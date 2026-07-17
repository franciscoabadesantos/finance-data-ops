# Operations runbook (Data Ops v3)

## Daily jobs

Market:

```bash
python scripts/run_market_daily.py --region us
```

Fundamentals:

```bash
python scripts/run_fundamentals_daily.py --region all
```

Earnings:

```bash
python scripts/run_earnings_daily.py --region all
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

Entity attributes country/region repair:

```bash
python scripts/run_entity_attributes_static_backfill.py --write-cache --publish
```

This re-publishes `feature_store.entity_attributes_static` from the cached entity attributes table after applying the
canonical symbol normalization, ISO-2 `normalize_country`, `region_for_country`, and `home_country` mapping.
Region or aggregate tokens such as `EU`, `APAC`, `AMER`, and `EMEA` are not valid listing countries; bare symbols fall
back to the safe no-suffix listing-country policy, while suffixed symbols continue to use suffix-derived countries.

Provider symbol ownership:

- Data Ops owns provider/onboarding symbol resolution for ETF/frontier identities.
- Backend/frontier services should consume `public.etf_holding_onboarding_identity` and submit `onboard_symbol`
  only when `is_onboardable=true`.
- `public.etf_holding_onboarding_identity` is an operational onboarding identity read model only. Relationship-map
  holdings/theme consumers must use `source_cache.etf_holdings`, `source_cache.etf_themes`, and
  `source_cache.etf_theme_readiness`.
- Raw holdings symbols that cannot be resolved confidently remain `is_onboardable=false` with a
  `not_onboardable_reason`; backend must not add country/exchange suffixes.

Ensure the runtime baseline has been applied before publishing the country/home-country backfill.

Foreign ETF holding symbol/name repair:

```bash
python scripts/run_foreign_symbol_backfill.py --write-cache --publish
```

This rewrites cached ETF holdings with canonical foreign listing symbols, drops numeric placeholder identifiers, fills
`holding_country` from holdings source country with suffix fallback, fills `feature_store.entity_attributes_static.name`
from `holding_name`, and publishes the corrected holdings/entity rows. The summary reports existing US-classified rows
corrected to non-US listing country plus US-listed rows that gained a non-US `home_country`. It does not ingest prices for
newly discovered foreign constituents. When `--publish` is used, it also deletes old bare numeric or unpadded `.HK` entity
IDs only when their normalized replacement already exists in `feature_store.entity_attributes_static`, and removes dead
numeric-plus-letter placeholder entity IDs.

ETF canonical schema reconciliation:

```bash
python scripts/reconcile_etf_canonical_schema.py
python scripts/reconcile_etf_canonical_schema.py --apply
```

The command is dry-run by default. It is an admin-only one-shot for existing live databases where
`source_cache.etf_holdings` or `source_cache.etf_themes` still exist as old views. With `--apply`, it replaces those
views with writable `source_cache` tables, copies rows from the old `public.etf_holdings` and `public.etf_themes`
tables, rebuilds `source_cache.etf_theme_readiness`, and applies conditional grants. It does not drop the old public
tables unless `--drop-old-public` is passed.

Live verification after apply:

```bash
psql "$DATA_OPS_DATABASE_URL" -c "
select n.nspname as schema_name, c.relname, c.relkind
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where (n.nspname, c.relname) in (
  ('source_cache', 'etf_holdings'),
  ('source_cache', 'etf_themes'),
  ('source_cache', 'etf_theme_readiness'),
  ('public', 'etf_holdings'),
  ('public', 'etf_themes')
)
order by 1, 2;"

psql "$DATA_OPS_DATABASE_URL" -c "
select 'source_cache.etf_holdings' as object, count(*) from source_cache.etf_holdings
union all
select 'source_cache.etf_themes', count(*) from source_cache.etf_themes
union all
select 'source_cache.etf_theme_readiness', count(*) from source_cache.etf_theme_readiness;"

psql "$DATA_OPS_DATABASE_URL" -c "
set role finance_data_ops_worker;
select count(*) from source_cache.etf_holdings;
select count(*) from source_cache.etf_themes;
select count(*) from source_cache.etf_theme_readiness;
reset role;"
```

Entity identity side-by-side schema reconciliation:

```bash
python scripts/reconcile_entity_identity_schema.py
python scripts/reconcile_entity_identity_schema.py --apply
```

The command is dry-run by default and is admin/DDL-role only when `--apply` is used. It reconciles schema prerequisites
for Entity Layer cache-first and side-by-side publication by creating missing raw cache/review/publication tables,
adding missing publication columns to old `feature_store.entity_master` and `feature_store.entity_listing` tables,
creating indexes, and applying conditional worker/read grants. It does not write raw cache facts, does not publish
`entity_master`/`entity_listing` data, and does not change product/read paths.

Raw cache publication uses idempotent upserts. If a cache-only apply fails partway through, earlier cache table writes
may already be committed by the Postgres publisher and the same cache apply can be rerun after fixing the failing table.
Synthetic `cache_miss` rows emitted by offline cache-read measurement are diagnostics only and are not written as raw
facts.

Entity identity full-universe cache-read dry-run:

```bash
python scripts/measure_entity_identity_chain.py --source postgres --tracked-only --offline --use-raw-cache
python scripts/publish_entity_identity_side_by_side.py --source postgres --tracked-only --offline --use-raw-cache
python scripts/run_post_onboard_entity_identity_refresh.py --source postgres --scope-key tracked
```

`--tracked-only` restricts the Postgres universe to `feature_store.ticker_readiness.is_tracked = true`. In
`--offline --use-raw-cache` mode the commands read `source_cache.openfigi_mapping_raw`, `source_cache.listing_isin_raw`,
`source_cache.gleif_isin_lei_raw`, `source_cache.gleif_lei_isin_raw`, and cached GLEIF entity rows if present. No
OpenFIGI, yfinance, or GLEIF live calls are made, and no data is written without the existing apply flags. The summary
includes raw-cache coverage and samples of missing OpenFIGI, listing ISIN, GLEIF ISIN-to-LEI, GLEIF LEI-to-ISIN, and
legal-name cache facts.
`source_cache.gleif_entity_raw` stores GLEIF legal-name search by conservative `normalized_query_name`, not by LEI. It
persists the original query, candidates payload, response payload, status, and error message. Cached `not_found` rows
are reused as known negatives; missing rows are reported as `cache_miss` and are not written as raw facts.

Post-onboarding Entity Layer refresh:

```bash
# Dry-run current tracked universe, cache-first, no live providers and no writes.
python scripts/run_post_onboard_entity_identity_refresh.py --source postgres --scope-key tracked

# Fill raw-cache misses only, with provider throttles, without publishing entity tables.
python scripts/run_post_onboard_entity_identity_refresh.py \
  --source postgres \
  --scope-key tracked \
  --refresh-live \
  --refresh-cache-misses \
  --gleif-request-sleep-seconds 7 \
  --apply-caches

# Publish side-by-side entity tables only after the gate is green.
python scripts/run_post_onboard_entity_identity_refresh.py \
  --source postgres \
  --scope-key tracked \
  --batch-id tracked-entity-refresh-YYYYMMDD-HHMMSS \
  --apply-entities
```

The post-onboarding command is a thin wrapper over `publish_entity_identity_side_by_side.py`: it reuses the same resolver,
raw-cache read-through, publication gate, review routing, side-by-side batch rows, and current pointer update. Postgres
mode always targets the current tracked universe from `feature_store.ticker_readiness.is_tracked = true`; it does not use
a fixed symbol file. `tracked` is the growing scope key for future onboarding waves and is independent of the historical
`tracked-675` pointer. The command advances `feature_store.entity_identity_publication_current(scope_key='tracked')` only
when `--apply-entities` is supplied and the publication gate passes.

Track B can trigger the no-schedule Prefect deployment after an onboarding wave completes:

```bash
prefect deployment run dataops_entity_identity_refresh/entity-identity-refresh \
  --param scope_key=tracked \
  --param apply_caches=false \
  --param apply_entities=false \
  --param refresh_live=false
```

For the apply run, set `apply_entities=true` and provide a reviewed `batch_id` after the dry-run summary is accepted.
The JSON summary reports `scope_key`, `batch_id`, tracked count, planned entity/review rows, resolved/provisional/review
counts, unresolved multi-listing count, publication gate status, cache-refresh stats, whether the pointer advanced, and
the previous batch id.

Entity master home-country cache-only backfill:

```bash
python scripts/backfill_entity_home_country.py --batch-id tracked-675-first-publish
python scripts/backfill_entity_home_country.py --batch-id tracked-675-first-publish --apply
python scripts/backfill_entity_home_country.py --batch-id tracked-675-first-publish
```

This reads the published batch from `feature_store.entity_master` and derives missing `home_country` values only from
already persisted GLEIF raw cache rows (`source_cache.gleif_entity_raw` and `source_cache.gleif_isin_lei_raw`). It does
not call APIs, does not change `entity_listing`, does not change entity ids or attach decisions, and does not update the
publication current pointer. `--apply` updates only null/blank `entity_master.home_country` values for resolved entities
in the selected batch and stores the GLEIF source table/field in `metadata.home_country_backfill`. Non-empty conflicting
countries are reported and not overwritten.

Entity identity cache-fill for missing raw facts remains separate from entity publication:

```bash
python scripts/publish_entity_identity_side_by_side.py \
  --source postgres \
  --tracked-only \
  --use-raw-cache \
  --refresh-live \
  --refresh-cache-misses \
  --gleif-request-sleep-seconds 7 \
  --apply-caches
```

This reads cache first, refreshes only cache misses through the live providers, and upserts only raw cache tables.
`--apply-entities` is a separate operation and must not be combined until the publication gate for the measured scope is
green.

Read-only verification after schema apply:

```bash
psql "$DATA_OPS_DATABASE_URL" -c "
select table_schema, table_name
from information_schema.tables
where (table_schema, table_name) in (
  ('source_cache', 'listing_isin_raw'),
  ('source_cache', 'gleif_isin_lei_raw'),
  ('source_cache', 'gleif_lei_isin_raw'),
  ('feature_store', 'entity_master'),
  ('feature_store', 'entity_listing'),
  ('feature_store', 'entity_identity_review'),
  ('feature_store', 'entity_identity_review_audit'),
  ('feature_store', 'entity_identity_publication_batch'),
  ('feature_store', 'entity_identity_publication_current')
)
order by 1, 2;"

psql "$DATA_OPS_DATABASE_URL" -c "
select table_schema, table_name, column_name, data_type, is_nullable, column_default
from information_schema.columns
where (table_schema, table_name, column_name) in (
  ('feature_store', 'entity_master', 'publication_batch_id'),
  ('feature_store', 'entity_listing', 'attach_method'),
  ('feature_store', 'entity_listing', 'attach_confidence'),
  ('feature_store', 'entity_listing', 'review_state'),
  ('feature_store', 'entity_listing', 'evidence_payload'),
  ('feature_store', 'entity_listing', 'source_freshness'),
  ('feature_store', 'entity_listing', 'publication_batch_id')
)
order by 1, 2, 3;"

psql "$DATA_OPS_DATABASE_URL" -c "
select schemaname, indexname
from pg_indexes
where indexname in (
  'idx_listing_isin_raw_isin',
  'idx_gleif_isin_lei_raw_lei',
  'idx_entity_master_publication_batch_id',
  'idx_entity_listing_publication_batch_id',
  'idx_entity_identity_review_publication_batch_id',
  'idx_entity_identity_publication_batch_scope_current'
)
order by 1, 2;"
```

Wave A onboarding (ITB homebuilders + US-listed GDX gold miners):

```bash
python scripts/run_wave_a_itb_gdx_onboarding.py --write-cache --publish --run-backfill
```

This derives the bounded Wave A universe from cached `etf_holdings`, skips already-active registry symbols, publishes
names from `holding_name`, and fetches full-history market prices from `1900-01-01` plus fundamentals, earnings history,
and exchange calendars. Non-US GDX listings remain out of scope for the later international wave.

Parity gates:

```bash
python scripts/check_macro_parity.py --start-date 2020-01-01 --end-date 2026-04-13
python scripts/check_release_calendar_parity.py --start-date 2020-01-01 --end-date 2026-04-13
```

Market history repair:

```bash
python scripts/run_market_history_repair.py --region all --min-rows 500 --lookback-days 3650 --dry-run
python scripts/run_market_history_repair.py --region all --min-rows 500 --lookback-days 3650 --chunk-size 50
```

The repair command resolves its universe from `ticker_registry` unless `--symbols` or `DATA_OPS_SYMBOLS_OVERRIDE*`
is provided. It writes idempotently to `source_cache.market_price_daily`, scopes status as `run_subset`, and does not
trigger relationship-map or feature-store rebuilds.

Source refresh universe audit/reconciliation:

```bash
python scripts/reconcile_source_refresh_universe.py --fail-on-issues
python scripts/reconcile_source_refresh_universe.py --apply
```

The audit compares active/promoted/market-supported `ticker_registry` rows with tracked/materialized product symbols
from `feature_store.ticker_readiness`, `source_cache.market_price_daily`, `feature_store.technical_features_daily`,
and `feature_store.ticker_page_summary`. Dry-run is the default; `--apply` is required before any registry write.

## Required environment

- `DATA_OPS_DATABASE_URL`

Optional:

- `DATA_OPS_LOOKBACK_DAYS`
- `DATA_OPS_MAX_ATTEMPTS`
- `DATA_OPS_SYMBOL_BATCH_SIZE`
- `DATA_OPS_CACHE_ROOT`
- `DATA_OPS_ALERT_WEBHOOK_URL`
- `DATA_OPS_SYMBOLS_OVERRIDE` for manual/local source-refresh subsets
- `DATA_OPS_SYMBOLS_OVERRIDE_US` / `DATA_OPS_SYMBOLS_OVERRIDE_EU` / `DATA_OPS_SYMBOLS_OVERRIDE_APAC` for region-specific manual/local subsets
- `FEATURE_BUILD_DAILY_DEPLOYMENT` (default `feature-build-daily/feature-build-daily`)
- `FEATURE_SCORECARD_BUILD_DEPLOYMENT` (default `scorecard-daily/scorecard-daily`; targeted onboarding scorecard build)

## Healthy run checks

Look for:

- flow exit code `0`
- `data_source_runs` rows for refresh + orchestration jobs
- `data_asset_status` rows for:
  - `macro_observations`
  - `macro_daily`
  - `economic_release_calendar`
  - existing market/fundamentals/earnings assets
- `symbol_data_coverage` is complete after a diagnostic rebuild, with one row per symbol
  currently materialized in source market/fundamentals/earnings tables

## Runtime source-of-truth policy

- Scheduled source refresh universes come from active, promoted, market-supported `ticker_registry` rows.
- `feature_store.ticker_readiness` remains the product/search tracked universe.
- Prefect `symbols` parameters are manual one-off subsets and always win.
- `DATA_OPS_SYMBOLS_OVERRIDE*` variables are emergency/local subset overrides, not the production universe.
- Region schedules exist because US, EU, and APAC market-close times differ.

For macro and release calendar domains:

- canonical source: `finance-data-ops` tables (`macro_*`, `economic_release_calendar`)
- migration fallback only: legacy `Finance` read logic (feature-flag controlled)
- `Finance/configs/release_calendars/*` is not part of post-cutover runtime logic

## Diagnostic Coverage Rebuild

`symbol_data_coverage` is diagnostic only. Rebuild it from current materialized source rows when
coverage counts drift from readiness:

```bash
python scripts/rebuild_symbol_data_coverage.py --source postgres --summary
python scripts/rebuild_symbol_data_coverage.py --source postgres --apply --summary
```

The rebuild deletes and reinserts `public.symbol_data_coverage` rows derived from
`source_cache.market_price_daily`, `source_cache.fundamentals`, and `source_cache.earnings`.
`market_data_available=true` requires materialized price rows. Cleaned placeholder identifiers
and known superseded aliases such as `700.HK` are excluded unless they have materialized price rows.

## Request-driven async jobs

Ticker lifecycle requests run through Prefect deployments. Backend services should
trigger `ticker-onboarding`, `ticker-remove`, `ticker-validation`, or
`ticker-backfill` directly and must not write `public.ticker_registry`.

## Worker runtime floor

For the shared analysis worker (`finance-jobs-worker`), keep:

- memory: `1Gi`
- timeout: `300s`

Operational reason:

- historical earnings region rebuilds OOM-kill the worker at `512Mi`
- the failure historically presented as worker shutdowns around batch 3, not
  as an explicit application error
- `1Gi` resolved the issue and is now the minimum safe production floor
