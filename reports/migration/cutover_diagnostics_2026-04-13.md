# Cutover Diagnostics (2026-04-13)

## 1) Schema + publish-enabled macro backfill

- Supabase table/view checks: `200` for
  - `macro_series_catalog`
  - `macro_observations`
  - `macro_daily`
  - `economic_release_calendar`
  - `mv_latest_macro_observations`
  - `mv_latest_economic_release_calendar`

- Publish-enabled macro backfill succeeded end-to-end.
  - Summary: `reports/migration/macro_backfill_publish_summary_v4.json`
  - Log: `reports/migration/macro_backfill_publish_v4.log`
  - Key outputs:
    - status: `fresh`
    - symbols_succeeded: 17/17
    - `macro_series_catalog` publish rows: 17
    - `macro_observations` publish rows: 15738
    - `macro_daily` publish rows: 27705
    - publish_failures: none

## 2) Release backfill reliability diagnosis + stabilization

### Root causes found

1. ALFRED/FRED fetch transport instability was amplified by forced custom `User-Agent` and long per-call timeout windows.
2. Python-side network path could stall long enough to make run-level retries ineffective.
3. Weekly ICSA frontier gap: official release dates only overrode existing weekly rows and did not insert missing frontier observation weeks.
4. DOL weekly source can lag latest observation week.

### Stabilization changes applied

- `src/finance_data_ops/providers/release_calendar.py`
  - switched to curl-bounded transport with explicit subprocess timeout
  - reduced default timeout/retry envelope (`DEFAULT_TIMEOUT_SECONDS=15`, `DEFAULT_MAX_RETRIES=2`)
  - removed custom `User-Agent`
  - switched ALFRED-mode calls to FRED host (`fred.stlouisfed.org`) and robust CSV column parsing
  - ensured official ICSA release dates can insert missing weekly rows
  - unioned DOL weekly observation dates with latest ICSA observation presence to cover frontier weeks

- `src/finance_data_ops/ops/incidents.py`
  - classified additional transient network/JSON failure classes as retryable

### Reliability result after changes

- Publish-enabled release backfill succeeded end-to-end.
  - Summary: `reports/migration/release_backfill_publish_summary_v4.json`
  - Log: `reports/migration/release_backfill_publish_v4.log`
  - Key outputs:
    - status: `fresh`
    - symbols_succeeded: 6/6
    - `economic_release_calendar` publish rows: 696
    - publish_failures: none

## 3) Macro parity (target mismatch classes)

- Latest macro parity report:
  - `reports/migration/macro_parity_report_v4.json`
  - `reports/migration/macro_parity_v4.log`

- Aggregate parity counters (v4):
  - value_mismatch_rows: 263
  - timing_mismatch_rows: 218
  - staleness_mismatch_rows: 3359

- Target-series breakdown:
  - `reports/migration/macro_mismatch_series_breakdown_v4.json`

### Series-level RCA

- `CPI_Core`, `CPI_Headline`
  - value mismatches: 10 each
  - availability mismatches: 109 each
  - staleness mismatches: 494 each
  - root cause: canonical CPI release timestamps are earlier than legacy for observation periods `2020-01`, `2021-01`, `2022-01`, `2023-01`, `2024-01` (2-4 days earlier).
  - interpretation: canonical CPI release timing logic is likely wrong for these periods; legacy aligns with explicit BLS calendar entries.

- `DBC`
  - value mismatches: 240 at tolerance `1e-9`; only 1 at tolerance `1e-5`
  - staleness mismatches: 138
  - root cause split:
    - most mismatches are float precision noise
    - one material mismatch (`2025-12-31`) tracks legacy stale carry-forward behavior; current yfinance matches canonical value.

- `U6RATE`, `CIVPART`, `UNRATE`
  - value mismatches: 0
  - availability mismatches: 0
  - staleness mismatches: 265 each
  - root cause: staleness contract mismatch (canonical explicit staleness policy vs legacy null-proxy parity heuristic).

- `ICSA`
  - value mismatches: 0
  - availability mismatches: 0 (fixed)
  - staleness mismatches: 148
  - root cause: staleness contract mismatch, not release timing after fixes.

## 4) Release-calendar parity status

- Latest release parity report:
  - `reports/migration/release_parity_report_v2.json`
  - `reports/migration/release_parity_v2.log`

- Current gaps:
  - timestamp mismatches: 10 (all CPI Jan periods listed above)
  - key-count deltas exist due legacy registry semantics:
    - legacy includes canceled placeholders (`release_timestamp_utc` empty) and forward scheduled keys
    - canonical currently materializes observed/supported release keys

## 5) Recommendation before rerunning parity for cutover decision

1. Fix CPI release timing in canonical release-calendar generation:
   - source CPI from explicit official release calendar semantics (BLS/FRED calendar mapping) for the affected Jan observation periods.
2. Update parity tolerance contract:
   - value tolerance: `1e-5` (removes precision-only noise).
3. Update staleness parity contract:
   - compare canonical `is_stale` against a legacy staleness computation, not `legacy_value IS NULL` proxy.
4. Clarify release-calendar contract for canceled/forward placeholders:
   - decide whether canonical must include legacy-style placeholder rows with empty release timestamps.
5. After 1-4 are codified, rerun:
   - release backfill publish-enabled
   - macro backfill publish-enabled
   - macro parity + release parity

No production flags were flipped.
No legacy files were deleted.
