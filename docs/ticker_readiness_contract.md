# Ticker Readiness Contract

## Ownership

- `ticker_registry` is onboarding/request pipeline state only. `active`, `validated_market_only`, and `validated_full` do not mean product-ready or tracked.
- `symbol_data_coverage` is a diagnostic operational table. It can be rebuilt from current materialized source tables and must not be the sole source of truth for tracked search.
- Source tables such as `source_cache.market_price_daily` prove materialized source availability.
- Feature-store derived tables prove feature readiness. The canonical product-readiness DB object should live in feature-store because it depends on derived feature tables such as technicals and scorecards.
- Backend reads and orchestrates; it must not mutate or own readiness.

## Data Ops Side

Data Ops owns deterministic inputs and audits:

- `finance_data_ops.validation.readiness.build_ticker_readiness_rows`
- `finance_data_ops.validation.readiness.build_readiness_audit`
- `scripts/audit_ticker_readiness.py`

Tracked search readiness is derived from materialized prices plus technical feature rows. Scorecard availability is reported separately because scorecard is a feature-store product surface that can lag technical readiness.

## Residue Audit

The audit reports, but does not delete:

- active/validated registry rows with no prices
- prices without technicals
- technicals without scorecard
- rejected registry rows with materialized data
- placeholder-like symbols such as `2200963D`
- `symbol_data_coverage` rows that disagree with materialized price rows
