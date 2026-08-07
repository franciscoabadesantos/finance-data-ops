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

## Currency

`source_cache.fundamentals` is **multi-currency and labelled**: each row carries the currency of its own listing, and
nothing in the stack converts between them. 9984.T is reported in JPY, HSBA.L in USD.

Consumers must filter or convert. Reading `value` without looking at `currency` produces a number with no unit, which is
harmless for a page showing one ticker at a time and wrong the moment anything sums or ranks. It did: one relationship-map
community totalled USD 4,882 bn plus non-USD 2,879,982 bn as a single figure, and 005930.KS alone read roughly 1,300×
the largest US name. The feature store now restricts to a configured `base_currency`, at the cost of ~18% of nodes
carrying a null cap instead of a wrong one.

There is no FX source anywhere in the stack today — no rates table in any schema, no currency symbols in
`market_price_daily`, and no entity with caps in two currencies to derive a rate from. The `fx` factor in
`residualize_return_matrix` is not one: it averages peers sharing a currency, which is useful for residualising and
useless for converting.

When conversion arrives, store the converted value **together with the rate used**, rather than converting at read time.
Converting on read makes a historical cap change whenever rates move, which destroys the point-in-time property.

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
