# Market Cap Contract

## Current decision

`market_cap` is intentionally published in two runtime surfaces for now:

- `market_quotes.market_cap_text`
- `ticker_fundamental_point_in_time.metric = 'market_cap'`, surfaced through
  `mv_latest_fundamentals` and `ticker_fundamental_summary.market_cap`

This is a pragmatic compatibility decision. The clean long-term design is a single canonical
current-size metric, but existing consumers read from different surfaces.

## Why there are two surfaces

`market_quotes` is the current quote/display surface. It feeds quote headers, latest quote payloads,
and downstream entity-size consumers such as correlation-network node sizing.

Fundamentals surfaces feed valuation math and scorecard/profile consumers. Valuation ratios need a
current market size next to fiscal statement values:

- `price_to_book = market_cap / equity`
- `price_to_sales = market_cap / revenue`
- `price_to_fcf = market_cap / free_cash_flow`

Keeping `market_cap` available in fundamentals avoids forcing those consumers to join quotes during
scorecard/profile assembly.

## ETF and fund semantics

For equities, `market_cap` means company market capitalization.

For ETFs/funds, Yahoo can return `marketCap = null`. In that case Data Ops uses fund AUM from
`.info['totalAssets']` as the size fallback. This fallback must only apply to ETFs/funds. For equities,
`totalAssets` is a balance-sheet metric and must not be used as market cap.

The ETF/fund fallback must stay aligned in both providers while the two-surface design exists:

- `FundamentalsDataProvider`: fills point-in-time `market_cap` for fundamentals/profile/scorecard.
- `MarketDataProvider`: fills quote `market_cap` so `market_quotes.market_cap_text` can be populated.

## Flow ownership

`dataops_fundamentals_daily` updates:

- `ticker_fundamental_point_in_time`
- `mv_latest_fundamentals`
- `ticker_fundamental_summary`

It reads cached `market_quotes` for coverage/status context, but it does not recalculate or publish
`market_quotes`.

`dataops_market_daily` updates:

- `market_price_daily`
- `market_quotes`
- `market_quotes_history`
- `mv_latest_prices`
- `ticker_market_stats_snapshot`

Any fix in `MarketDataProvider` only reaches Supabase after `dataops_market_daily` runs.

## Backend read order

Product-facing reads should resolve current market size in this order:

1. Parsed quote market cap from `market_quotes.market_cap_text`
2. `ticker_fundamental_summary.market_cap`
3. `ticker_fundamental_point_in_time` / `mv_latest_fundamentals` metric `market_cap`

This keeps quote consumers working while allowing fundamentals to rescue ETFs whose quote payload
does not include market cap.

## Known wart

The current design can temporarily diverge because market and fundamentals flows refresh on different
paths. This is acceptable while stabilizing the product data pipeline, but it is not the final model.

## Future cleanup

The durable design is to introduce one canonical current-size source and make consumers join it
explicitly:

1. Add a numeric canonical `market_cap` or `current_size` field to the quote/current-size surface.
2. Move scorecard/profile assembly to read that canonical current-size surface when computing value
   ratios.
3. Deprecate market-cap duplication in fundamentals summaries once all consumers have migrated.
4. Keep ETF/fund AUM fallback in exactly one provider path after migration.

Do not remove either current surface until backend, scorecard, profile, and network/entity-attribute
consumers have been migrated and verified.
