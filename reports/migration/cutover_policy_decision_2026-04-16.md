# Cutover Policy Decision Memo (2026-04-16)

## Scope

Decision pass after reruns with observed-first release timing semantics.

Inputs:

- `reports/migration/macro_parity_report_v7.json`
- `reports/migration/release_parity_report_v4.json`
- `reports/migration/macro_presence_mismatch_canonical_only_v7.csv`
- `reports/migration/macro_presence_mismatch_legacy_only_v7.csv`
- `reports/migration/macro_value_mismatches_v7.csv`

## 1) Remaining 4 macro value mismatches (row-by-row)

All 4 are on `2025-12-31`:

| as_of_date | series_key | canonical_value | legacy_value | abs_diff | classification |
|---|---|---:|---:|---:|---|
| 2025-12-31 | DBC | 22.360001 | 22.639999 | 0.279999 | legacy_stale_carry_forward |
| 2025-12-31 | VIX | 14.950000 | 14.330000 | 0.620000 | legacy_stale_carry_forward |
| 2025-12-31 | VIX3M | 18.180000 | 17.770000 | 0.410000 | legacy_stale_carry_forward |
| 2025-12-31 | VVIX | 92.669998 | 87.129997 | 5.540001 | legacy_stale_carry_forward |

Observed pattern:

- Legacy values on `2025-12-31` equal legacy `2025-12-30` for all 4 series.
- Canonical has fresh `2025-12-31` source-observation values.

Decision:

- Treat as acceptable drift from legacy staleness, not canonical correctness defects.

## 2) Macro presence/absence mismatch classification

From latest parity:

- canonical present / legacy missing: `2137`
- legacy present / canonical missing: `1238`

Classified mismatch classes:

### Canonical present / legacy missing (`2137`)

- `1103` (`51.6%`): `boundary_end_legacy_tail_missing`
  - canonical has newer tail coverage after legacy effectively stops updating.
- `1034` (`48.4%`): `in_window_legacy_missing`
  - dominated by calendar semantics; `926/1034` (`89.6%`) are lagged-source rows (canonical forward-fill on weekday calendar while legacy is missing on those dates).

### Legacy present / canonical missing (`1238`)

- `1098` (`88.7%`): `canonical_stale_null_policy`
  - canonical intentionally nulls stale data where legacy continues carrying values.
- `140` (`11.3%`): `boundary_start_pre_canonical_coverage`
  - early-window seeding before canonical first available coverage.

Decision:

- These are policy/semantic differences, not ingestion/publish failures.

## 3) Release parity residuals

Current state:

- release timestamp mismatches: `0`
- key mismatches: `5` (all missing in legacy, none missing in canonical)
- mismatch keys: monthly `2026-03` rows for `CIVPART`, `CPI_Core`, `CPI_Headline`, `U6RATE`, `UNRATE`

Decision:

- Canonical-newer release rows are accepted as correct.

## 4) Explicit policy recommendations

### A) Should canonical-newer release rows be accepted as correct?

**Recommendation: YES**

Reason:

- Canonical timestamps match on shared keys (`0` timestamp mismatches).
- Extra rows are newer canonical coverage absent from legacy, not contradictory data.

### B) Should canonical stale/null semantics replace legacy semantics?

**Recommendation: YES**

Reason:

- This aligns with correctness goal: avoid treating stale carried-forward values as current truth.
- `1098` legacy-only rows are exactly this semantic difference.

### C) Should boundary-window presence mismatches be excluded from cutover parity?

**Recommendation: YES**, with explicit rule set:

1. Exclude `boundary_start_pre_canonical_coverage`.
2. Exclude `boundary_end_legacy_tail_missing`.
3. Exclude calendar-semantic rows where canonical is carrying prior observed value for weekday alignment and legacy is missing (in-window lagged-source category).

## 5) Remaining mismatch classes: accepted vs blockers

Accepted:

- 4 value mismatches on `2025-12-31` (`legacy_stale_carry_forward`)
- canonical-newer release keys (`2026-03` monthly rows)
- stale/null semantic differences
- boundary-window/calendar presence differences

True blockers:

- **None identified** after applying the policy decisions above.

## 6) Final cutover decision status

Technical correctness status:

- backfills succeed
- publish succeeds
- macro timing mismatches are `0`
- release timestamp mismatches are `0`

Policy decision outcome:

- Cutover is **safe contingent on formal approval of the policy recommendations above**.
- Do not flip flags until this memo is approved.

