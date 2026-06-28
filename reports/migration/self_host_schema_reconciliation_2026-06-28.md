# Self-Host Schema Reconciliation - 2026-06-28

Scope: compare the feature-store canonical schema from commit `cc9bc82` with the real Supabase public schema exposed by PostgREST.

## Feature-Store Contract

The merged feature-store schema defines:

- `source_cache.fundamentals`: `symbol`, `report_date`, `metric`, `value`, `value_text`, `period_end`, `period_type`, `fiscal_year`, `fiscal_quarter`, `currency`, `source`, `source_updated_at`, `ingested_at`; primary key `(symbol, metric, period_end, period_type, report_date)`.
- `source_cache.earnings`: `symbol`, `report_date`, `earnings_date`, `fiscal_period`, `earnings_time`, EPS/revenue fields, `currency`, `source`, `source_updated_at`, `ingested_at`; primary key `(symbol, report_date, earnings_date, fiscal_period)`.
- `source_cache.calendars`: `exchange_mic`, `session_date`, `is_trading_day`, `is_half_day`, `currency`, `source`, `source_updated_at`, `ingested_at`; primary key `(exchange_mic, session_date)`.
- `feature_store.entity_attributes_static`: now includes `exchange`, `exchange_mic`, `currency`.

## Supabase Real Schema

Supabase PostgREST currently exposes only the legacy public surfaces for this scope:

- `market_fundamentals_v2`: `ticker`, `period`, `period_end`, `metric`, `value`, `value_text`, `source`, `fetched_at`.
- `market_earnings_events`: `ticker`, `earnings_date`, `earnings_time`, `fiscal_period`, `estimate_eps`, `estimate_revenue`, `source`, `fetched_at`, `created_at`, `updated_at`.
- `market_earnings_history`: `ticker`, `earnings_date`, `fiscal_period`, `actual_eps`, `estimate_eps`, `surprise_eps`, `actual_revenue`, `estimate_revenue`, `surprise_revenue`, `source`, `fetched_at`, `created_at`, `updated_at`.
- `exchange_trading_calendar`: `exchange_mic`, `session_date`, `is_half_day`, `ingested_at`.
- `ticker_registry`: already includes `exchange`, `exchange_mic`, `currency`.

`source_cache.*` is not present in the current Supabase schema, so this repo now writes those tables directly in the local Postgres schema.

## Invariants

- Fundamentals are written long-format to `source_cache.fundamentals` with explicit `report_date` and `currency`. Existing Supabase rows do not have those fields, so migration/backfill maps `report_date = fetched_at::date` when available, otherwise `period_end`, and defaults `currency = 'USD'` unless upstream rows provide currency.
- Earnings are written to `source_cache.earnings` with `report_date`; events and history are merged into the same PIT table.
- Calendars are written by `(exchange_mic, session_date)`, matching the feature-store schema. The request phrasing `(exchange, date)` maps to the MIC-backed key in the merged schema.
- `entity_attributes_static` is populated from ticker registry rows with `exchange`, `exchange_mic`, and `currency`; country/region are inferred by symbol suffix when absent.

## Follow-Up

No Supabase-only extra fields need a feature-store follow-up migration. The only divergence is expected: Supabase has legacy public tables, while the self-host target receives the new `source_cache` tables from `sql/015_self_host_source_cache_and_signals.sql`.
