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
- Raw holdings symbols that cannot be resolved confidently remain `is_onboardable=false` with a
  `not_onboardable_reason`; backend must not add country/exchange suffixes.

Apply `sql/017_symbology_country_home_country.sql` before publishing the country/home-country backfill.

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

## Required environment

- `DATA_OPS_DATABASE_URL`
- `DATA_OPS_SYMBOLS`

Optional:

- `DATA_OPS_LOOKBACK_DAYS`
- `DATA_OPS_MAX_ATTEMPTS`
- `DATA_OPS_SYMBOL_BATCH_SIZE`
- `DATA_OPS_CACHE_ROOT`
- `DATA_OPS_ALERT_WEBHOOK_URL`
- `DATA_OPS_SYMBOLS_US` / `DATA_OPS_SYMBOLS_EU` / `DATA_OPS_SYMBOLS_APAC`
- `FEATURE_BUILD_DAILY_DEPLOYMENT` (default `feature-build-daily/feature-build-daily`)
- `FEATURE_SCORECARD_BUILD_DEPLOYMENT` (default `scorecard-daily/scorecard-daily`; targeted onboarding scorecard build)
- `DATA_OPS_ALLOW_TICKER_REGISTRY_UNIVERSE` (default `false`; migration-only fallback)

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
