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
