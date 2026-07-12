# Market Cap Contract

## Current Decision

Data Ops publishes provider market-cap and fund-size observations only as raw/source data:

- `source_cache.fundamentals`

Feature-store owns the canonical product read models that expose current size, valuation ratios,
scorecards, and ticker page summaries.

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
