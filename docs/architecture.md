# Data Ops v3 Architecture

## System position

`finance-data-ops` is the operational product-data backend and the canonical ingestion boundary.

Flow per domain:

1. Fetch from external providers (`providers/`)
2. Normalize + persist canonical cache tables (`refresh/`)
3. Compute frontend-serving summaries (`derived/`)
4. Publish owned Supabase surfaces (`publish/`)
5. Publish operational status + coverage (`data_source_runs`, `data_asset_status`, `symbol_data_coverage`)

## Production boundary

Only this repo performs provider fetching for owned external data domains.

This repo owns:

- Market data refresh + publish
- Fundamentals refresh + publish
- Earnings refresh + publish
- Macro refresh + publish
- Economic release calendar refresh + publish
- Frontend-serving summaries derived from those domains
- Freshness, coverage, and run-status publication for all owned domains

This repo does not own:

- Research/training/backtests
- Live inference/signal generation
- Signal publication workflows
- Model-specific feature engineering

Those remain in the `Finance` repository.

## Owned Supabase surfaces

### Market

- `source_cache.market_price_daily`
- `feature_store.technical_features_daily`
- `feature_store.scorecard_daily`
- `feature_store.ticker_page_summary`

### Fundamentals

- `source_cache.fundamentals`
- `feature_store.ticker_page_summary`
- `feature_store.entity_attributes_static`

Feature-store owns product read models and schema lifecycle.

### Earnings

- `source_cache.earnings`
- `feature_store.ticker_page_summary`

### Macro

- `macro_series_catalog` (series metadata + policy)
- `macro_observations` (canonical observation-level history)
- `macro_daily` (business-day aligned canonical daily surface)
- `mv_latest_macro_observations` (latest per `series_key`)

### Economic release calendar

- `economic_release_calendar` (canonical schedule + observed availability semantics)
- `mv_latest_economic_release_calendar` (latest timestamp per release calendar source/series)

Release timing semantics:

- `scheduled_release_timestamp_utc` is the expected release schedule.
- `observed_first_available_at_utc` is first confirmed availability from authoritative source.
- downstream macro alignment uses observed availability when present, and scheduled time only as provisional fallback.

### Operational

- `data_source_runs`
- `data_asset_status`
- `symbol_data_coverage`
- `ticker_registry`
- `etf_holding_onboarding_identity`
- `source_cache.openfigi_mapping_raw`
- `source_cache.gleif_entity_raw`
- `source_cache.listing_isin_raw`
- `source_cache.gleif_isin_lei_raw`
- `source_cache.gleif_lei_isin_raw`
- `feature_store.entity_master`
- `feature_store.entity_listing`
- `feature_store.entity_identity_audit`

Data Ops owns provider symbology for frontier/onboarding candidates. Backend services consume
`etf_holding_onboarding_identity` as a read model and pass through `onboard_symbol`; suffix and listing
venue rules live in `finance_data_ops.identity`.

Entity identity V0 is an auditable side-by-side layer. A company/entity is not a ticker: one entity can have many listings, and listings retain their own symbol, venue, MIC, currency, country, provider identity, calendar, and price series. OpenFIGI ticker mapping provides listing/security identity; it does not by itself solve company grouping. V0.2 measures `anchor listing -> ISIN -> LEI -> expanded LEI ISIN set -> sibling listing attach`: one valid provider ISIN can identify a company LEI, and GLEIF LEI expansion uses paginated `GET /api/v1/lei-records/{LEI}/isins` to conservatively attach sibling listings. ISIN country-prefix mismatch is diagnostic, not an automatic blocker, only on the direct `ISIN -> LEI` path: the listing may attach as `isin_direct_prefix_mismatch_name_confirmed` with high confidence when the ISIN is valid, GLEIF returns a LEI, and OpenFIGI/internal listing name matches the GLEIF legal name under conservative normalization. Prefix-compatible direct anchors remain `isin_direct` with high confidence. The GLEIF legalName path remains fail-closed: name search creates only a candidate LEI, and `name_anchor_confirmed` requires exact conservative normalized name, compatible country/address context, one surviving LEI, acceptable GLEIF entity state, and compatible expanded ISINs. Name-only grouping is never allowed. Measurement output separates unattached tails into `fixable_free`, `requires_provider_or_curated_identity`, and `needs_manual_review`, profiles each row as `single_listing` or `multi_listing_candidate`, keeps detailed reject reasons, caps expanded ISIN samples, and emits a `name_anchor_precision_audit` for medium-confidence attachments. Attachments carry provenance/confidence: `isin_direct` and `isin_direct_prefix_mismatch_name_confirmed` are high, `lei_expansion` is high, `name_anchor_confirmed` is medium, and unresolved/ambiguous cases are `needs_review`. No product read path uses the entity layer yet, no entity tables are published from the measurement command, and no autonomous onboarding or cross-listing price blending is allowed.

## Runtime source-of-truth contract

For macro and release-calendar domains, runtime source-of-truth is:

1. Canonical tables in `finance-data-ops`
2. (Temporary migration fallback only) legacy `Finance` loaders until removal phase

`Finance` CSV/config calendar artifacts are not part of post-cutover runtime logic.

## Operational entrypoints

- Market: `flows/dataops_market_daily.py` / `scripts/run_market_daily.py`
- Fundamentals: `flows/dataops_fundamentals_daily.py` / `scripts/run_fundamentals_daily.py`
- Earnings: `flows/dataops_earnings_daily.py` / `scripts/run_earnings_daily.py`
- Macro: `flows/dataops_macro_daily.py` / `scripts/run_macro_daily.py`
- Economic release calendar: `flows/dataops_release_calendar_daily.py` / `scripts/run_release_calendar_daily.py`

Production scheduler automation is domain-separated to match those entrypoints.

## Request-driven jobs

Ticker lifecycle work is request-driven through Prefect deployments:

- Onboarding: `dataops_ticker_onboarding/ticker-onboarding`
- Removal/rejection: `dataops_ticker_remove/ticker-remove`
- Validation: `dataops_ticker_validation/ticker-validation`
- Backfill: `dataops_ticker_backfill/ticker-backfill`

Data Ops owns all `public.ticker_registry` lifecycle writes. The public backend
triggers Prefect deployments and reads state.
