# Controlled Migration Report (Macro + Release)

Generated: 2026-04-13
Scope window: 2020-01-01 to 2026-04-13

## 1) Backfill execution summary

### Macro backfill
- Publish-enabled run (`scripts/run_macro_backfill.py --start-date 2004-01-01 --end-date 2026-04-13 --force-recompute`) failed at publish phase.
- Root cause: canonical macro tables do not exist in Supabase yet.
  - Verified 404 on `macro_series_catalog`, `macro_observations`, `macro_daily`.
  - Error: `PGRST205 Could not find the table 'public.macro_series_catalog' in the schema cache`.
- No-publish run completed successfully for parity window:
  - Command: `scripts/run_macro_backfill.py --start-date 2020-01-01 --end-date 2026-04-13 --force-recompute --no-publish`
  - Status: `fresh`
  - Rows written: `macro_observations=15738`, `macro_daily=27863`, `macro_series_catalog=17`.

### Release calendar backfill
- Publish/no-publish runs with external provider calls were blocked by upstream endpoint timeouts from this environment (ALFRED/FRED).
  - `https://alfred.stlouisfed.org/graph/api/series/...` timed out
  - `https://fred.stlouisfed.org/releases/calendar...` timed out
- Fast-fail no-publish run (single-attempt timeout probe mode) completed with failure summary:
  - Status: `failed_retrying`
  - Rows written: `economic_release_calendar=0`
  - All requested series failed (`CPI_Headline`, `CPI_Core`, `UNRATE`, `U6RATE`, `CIVPART`, `ICSA`)
  - Primary error: `The read operation timed out`

## 2) Parity results

### Macro parity (`scripts/check_macro_parity.py`)
- Command exit code: `1` (`status=fail`)
- Window: `2020-01-01` to `2026-04-13`
- Result highlights:
  - Shared series compared: `17`
  - Value mismatch rows: `8764`
  - Canonical present / legacy missing rows: `2307`
  - Legacy present / canonical missing rows: `4`
  - Availability timing mismatch rows: `9831` (of 9834 compared)
  - Staleness mismatch rows: `2311`
- Top value-mismatch series:
  - `CPI_Core=1494`, `CPI_Headline=1494`, `DBC=1494`, `U6RATE=1207`, `CIVPART=1128`, `UNRATE=1120`, `ICSA=824`
- Top availability mismatch series:
  - `CIVPART=1639`, `CPI_Core=1639`, `CPI_Headline=1639`, `U6RATE=1639`, `UNRATE=1639`, `ICSA=1636`

### Release calendar parity (`scripts/check_release_calendar_parity.py`)
- Command exit code: `1`
- Blocked: canonical release parquet missing (`data_cache/economic_release_calendar.parquet`).
- Cause: release backfill did not produce rows due upstream timeout failures.

## 3) Contract issues / blockers found

1. **Schema not applied in Supabase for macro/release canonical tables**
- Missing: `macro_series_catalog`, `macro_observations`, `macro_daily`, `economic_release_calendar`.
- Required action: apply `sql/004_data_ops_v3_macro_release.sql` in target Supabase before publish-enabled backfill.

2. **Release provider reachability instability from current runtime**
- ALFRED and FRED calendar endpoints timed out repeatedly.
- Required action: stabilize connectivity (egress/allowlist/proxy/retry policy) before release backfill can complete.

3. **Macro parity currently fails strict gate**
- Differences are material across value, availability timing, and staleness dimensions.
- Required action: resolve semantic deltas (release-time assignment, series normalization, and stale semantics) before cutover.

## 4) Cutover recommendation

`NOT SAFE` to cut over canonical reads yet.

Rationale:
- Release calendar canonical dataset is not successfully backfilled.
- Macro parity fails strict requirements by large margins.
- Supabase canonical schema migration for macro/release is not applied in target environment.

## 5) Explicitly not performed

- Production feature flag flip was **not** performed.
- Legacy file/module deletion was **not** performed.

