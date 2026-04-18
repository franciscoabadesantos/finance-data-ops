# Final Semantic Parity Checkpoint (2026-04-16)

## 1) CPI release timing fix result

Code path updated:
- `src/finance_data_ops/providers/release_calendar.py`

Applied CPI Jan-period explicit release overrides:
- 2020-01 -> 2020-02-13 13:30:00 UTC
- 2021-01 -> 2021-02-10 13:30:00 UTC
- 2022-01 -> 2022-02-10 13:30:00 UTC
- 2023-01 -> 2023-02-14 13:30:00 UTC
- 2024-01 -> 2024-02-13 13:30:00 UTC

Validation outcome after backfill:
- Canonical CPI release timestamps match legacy for all overridden periods.
- Macro parity availability timing mismatches: `0`.

## 2) Finalized parity contract (enforced)

Macro:
- value tolerance: `1e-5`
- compared fields:
  - canonical: `macro_daily.value`
  - legacy: `macro_data_processed[series_key]`
- timing comparison:
  - canonical `available_at_utc` vs legacy `first_known_at_timestamp_utc`
  - exact UTC match on `(series_key, observation_period)`
- staleness comparison:
  - canonical `is_stale`
  - legacy replayed `legacy_is_stale_true` from true legacy method
- parity gate scope:
  - value/timing/staleness contract on shared canonical migration series
  - legacy-only extra columns are reported but do not fail macro gate

Release calendar:
- compared fields:
  - `release_timestamp_utc` exact UTC equality on `(series_key, observation_period)`
  - exact key/count parity after placeholder filtering
- placeholder decision:
  - rows with missing timestamps are non-canonical (publish contract violation)
  - forward-dated rows may exist in base canonical table and are excluded from in-window strict parity

Contract docs/scripts:
- `docs/parity_rules.md`
- `scripts/check_macro_parity.py`
- `scripts/check_release_calendar_parity.py`

## 3) Rerun execution results

Release backfill (publish-enabled):
- command window: `2020-01-01` to `2026-04-13`
- artifact: `reports/migration/release_backfill_publish_v6.log`
- result: success
  - `economic_release_calendar` rows: `698`
  - publish failures: `0`
  - `data_asset_status`: fresh for release assets
  - reliability note:
    - initial failed attempt in sandboxed network mode showed DNS resolution failure to `fred.stlouisfed.org`
    - rerun with unrestricted network completed with no failed symbols and no retry-exhausted symbols

Macro backfill (publish-enabled):
- command window: `2020-01-01` to `2026-04-13`
- artifact: `reports/migration/macro_backfill_publish_v5.log`
- result: success
  - `macro_series_catalog`: `17`
  - `macro_observations`: `15753`
  - `macro_daily`: `27863` (published payload rows: `27701`)
  - publish failures: `0`
  - `data_asset_status`: fresh/within-tolerance as expected for macro assets

Macro parity rerun:
- artifacts:
  - `reports/migration/macro_parity_report_v6.json`
  - `reports/migration/macro_parity_v6.log`
- result: `fail`
  - value mismatch rows: `4`
  - canonical-present/legacy-missing: `2137`
  - legacy-present/canonical-missing: `1238`
  - timing mismatches: `0`
  - staleness mismatches: `788`

Release parity rerun:
- artifacts:
  - `reports/migration/release_parity_report_v3.json`
  - `reports/migration/release_parity_v3.log`
- result: `fail`
  - timestamp mismatches: `0`
  - missing keys in legacy: `5`
  - missing keys in canonical: `0`
  - all 5 missing keys are `2026-03` monthly events present in canonical and absent in legacy

## 4) Root-cause analysis for requested high-priority macro series

Series: `CPI_Core`, `CPI_Headline`
- availability timing: fixed (no mismatches)
- value mismatches: `0`
- staleness mismatches: boundary-window artifacts (`2020-01-01` to `2020-02-03`)
- classification: semantics/window-seeding issue (not CPI release-timing bug)

Series: `U6RATE`, `CIVPART`, `UNRATE`
- availability timing: `0` mismatches
- value mismatches: `0`
- staleness mismatches concentrated at early-window boundary (`2020-01-01` to `2020-02-03`)
- classification: semantics/window-seeding issue

Series: `ICSA`
- availability timing: `0` mismatches
- staleness mismatches:
  - early-window boundary (`2020-01-01` onward)
  - late-2025 divergence where canonical has newer weekly source-observation dates while legacy remains on older source dates
- classification: mixed
  - boundary artifact + canonical fresher than legacy in late-2025

Series: `DBC`
- value mismatch: single row at `2025-12-31`
- staleness mismatch cluster in 2026 tail where canonical has fresh daily updates and legacy is stale
- classification: legacy appears stale/outdated; canonical behavior is preferred

## 5) Cutover recommendation

Current recommendation: **not yet safe to cut over**.

Blocking reasons:
- macro parity gate still failing materially (`value presence` + `staleness` deltas)
- release parity gate still failing on key-count differences (canonical has newer monthly `2026-03` rows missing in legacy)

Required before cutover:
1. Resolve macro boundary/staleness semantics explicitly:
   - either seed backfill from pre-2020 history (recommended) and rerun parity
   - or formally accept canonical stale-null semantics and adjust parity acceptance for value-presence deltas tied to stale policy
2. Decide acceptance for canonical-newer release rows (e.g., `2026-03` monthly releases) and encode that as parity expectation.
3. Rerun both parity gates after decisions; keep production flags unchanged until gates pass under accepted contract.
