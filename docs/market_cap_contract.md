# Market Cap Contract

## Current Decision

Data Ops publishes provider market-cap and fund-size observations only as raw/source data:

- `source_cache.fundamentals`

Feature-store owns the canonical product read models that expose current size, valuation ratios,
scorecards, and ticker page summaries.

`feature_store.entity_attributes_static` is a latest descriptive-metadata surface only. Data Ops
publishes identity fields there (name, country/home country, region, exchange/MIC, currency,
sector/industry, and description) and deliberately does not publish `market_cap`, `beta`, or
`beta_3y`. Those mutable numerics must come from point-in-time-safe sources, such as
report-dated `source_cache.fundamentals` rows filtered to `report_date <= as_of_date`.

Do not backdate or future-fill static metadata numerics into a historical scorecard or model
snapshot. A richer daily market-metrics series may be introduced separately; it is not supplied
by `entity_attributes_static`.

## ETF And Fund Semantics

For equities, `market_cap` means company market capitalization.

For ETFs and funds, Yahoo can return `marketCap = null`. In that case Data Ops may use fund AUM from
`.info['totalAssets']` as the size fallback in normalized provider fundamentals. This fallback must
only apply to ETFs and funds. For equities, `totalAssets` is a balance-sheet metric and must not be
used as market cap.

## Flow Ownership

`dataops_fundamentals_daily` writes normalized provider observations to `source_cache.fundamentals`.
It does not publish legacy public quote, fundamentals summary, or materialized-view market-cap
surfaces.
