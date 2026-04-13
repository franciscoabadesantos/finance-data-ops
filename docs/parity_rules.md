# Macro + Release Parity Rules (Cutover Gate)

This document defines strict parity checks before Finance runtime cutover.

## Macro parity

Source comparison:

- canonical: `finance-data-ops` `macro_daily`
- legacy: `Finance` `macro_data_processed` + `macro_release_timing_audit`

Rules:

- value equality on shared `(as_of_date, series_key)` rows with absolute tolerance `1e-8`
- availability timing equality for release-timed series using:
  - canonical `available_at_utc`
  - legacy `first_known_at_timestamp_utc` keyed by `(series_key, observation_period)`
- staleness parity:
  - canonical `is_stale`
  - legacy proxy `legacy_value IS NULL` on the same `(as_of_date, series_key)` row

Failure conditions:

- any missing series in either source
- any value mismatch above tolerance
- any canonical-only/legacy-only value presence mismatch
- any availability timing mismatch
- any staleness mismatch

## Release calendar parity

Source comparison:

- canonical: `finance-data-ops` `economic_release_calendar`
- legacy: `Finance/configs/release_calendars/*` active tables from `registry.json`

Rules:

- identical `release_timestamp_utc` for shared `(series_key, observation_period)` keys
- identical event counts (total + per-series)
- no missing series in either source
- no missing keys in either source

Tolerance:

- timestamp tolerance: exact UTC timestamp match (`0s`)
- count tolerance: exact match (`0` delta)

## Scripts

- `python scripts/check_macro_parity.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD`
- `python scripts/check_release_calendar_parity.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD`

Both scripts return non-zero on parity failure and can be used as CI/cutover gates.
