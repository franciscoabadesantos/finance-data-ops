# Macro + Release Parity Rules (Cutover Gate)

This document defines strict parity checks before Finance runtime cutover.

## Macro parity

Source comparison:

- canonical: `finance-data-ops` `macro_daily`
- legacy: `Finance` `macro_data_processed` + `macro_release_timing_audit`

Rules:

- value equality on shared `(as_of_date, series_key)` rows with absolute tolerance `1e-5`
- value fields compared:
  - canonical: `macro_daily.value`
  - legacy: `macro_data_processed[series_key]`
- availability timing equality for release-timed series using exact UTC timestamp match:
  - canonical: `macro_daily.available_at_utc`
  - legacy: `macro_release_timing_audit.first_known_at_timestamp_utc`
  - key: `(series_key, source_observation_period == observation_period)`
- staleness parity:
  - canonical: `macro_daily.is_stale`
  - legacy: computed `legacy_is_stale_true` (not null-proxy), replayed by:
    - release-timed series: `first_eligible_trading_date -> observation_date` from `macro_release_timing_audit`
    - non-release series: last non-null legacy observation date forward-filled
    - stale rule: `source_observation_date` missing OR `busday_count(source_observation_date, as_of_date) > staleness_max_bdays`

Failure conditions:

- any value mismatch above tolerance
- any canonical-only/legacy-only value presence mismatch
- any availability timing mismatch
- any staleness mismatch

Notes:

- macro parity scope is the shared canonical migration series; legacy-only extra columns are reported but do not fail the gate.

## Release calendar parity

Source comparison:

- canonical: `finance-data-ops` `economic_release_calendar`
- legacy: `Finance/configs/release_calendars/*` active tables from `registry.json`

Rules:

- placeholder policy before strict parity:
  - rows with missing scheduled release timestamp are treated as non-canonical placeholders
  - rows with scheduled release timestamp beyond parity window end are treated as forward-unrealized placeholders
  - rows with `is_schedule_based_only=true` are treated as non-canonical placeholders until observed availability is confirmed
  - placeholders are excluded from strict key/count/timestamp comparisons
- strict parity after placeholder filtering:
  - identical scheduled timestamp for shared `(series_key, observation_period)` keys (exact UTC equality):
    - canonical: `scheduled_release_timestamp_utc`
    - legacy: `release_timestamp_utc`
  - identical event counts (total + per-series)
  - no missing series in either source
  - no missing keys in either source
- canonical decision for placeholders:
  - canceled/missing rows with null `scheduled_release_timestamp_utc` are non-canonical and must fail publish validation
  - forward-dated rows may appear in base `economic_release_calendar` and are excluded from in-window strict parity

Tolerance:

- timestamp tolerance: exact UTC timestamp match (`0s`)
- count tolerance: exact match (`0` delta)

## Scripts

- `python scripts/check_macro_parity.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD`
- `python scripts/check_release_calendar_parity.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD`

Both scripts return non-zero on parity failure and can be used as CI/cutover gates.
