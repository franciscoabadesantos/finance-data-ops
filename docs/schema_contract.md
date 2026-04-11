# Supabase schema contract (Data Ops v2)

Data Ops now writes three product-data domains: market, fundamentals, earnings.

## Existing market surfaces (v1)

- `market_price_daily`
- `market_quotes`
- `market_quotes_history`
- `mv_latest_prices` (RPC refresh path: `refresh_mv_latest_prices`)
- `ticker_market_stats_snapshot`

## Fundamentals surfaces (v2)

- `market_fundamentals_v2` (canonical normalized history)
- `mv_latest_fundamentals` (latest per `(ticker, metric)`)
- `ticker_fundamental_summary` (frontend snapshot)

`market_fundamentals_v2` write contract:

- `ticker`
- `period`
- `period_end`
- `metric`
- `value`
- `value_text`
- `source`
- `fetched_at`
- upsert key: `ticker,period,period_end,metric`

`ticker_fundamental_summary` write contract:

- `ticker` (upsert key)
- `latest_revenue`
- `latest_eps`
- `trailing_pe`
- `market_cap`
- `revenue_growth_yoy`
- `earnings_growth_yoy`
- `latest_period_end`
- `source`
- `updated_at`

## Earnings surfaces (v2)

- `market_earnings_events` (upcoming/scheduled)
- `market_earnings_history` (historical results)
- `mv_next_earnings` (next event per ticker)

`market_earnings_events` write contract:

- `ticker`
- `earnings_date`
- `earnings_time`
- `fiscal_period`
- `estimate_eps`
- `estimate_revenue`
- `source`
- `fetched_at`
- `created_at`
- `updated_at`
- upsert key: `ticker,earnings_date`

`market_earnings_history` write contract:

- `ticker`
- `earnings_date`
- `fiscal_period`
- `actual_eps`
- `estimate_eps`
- `surprise_eps`
- `actual_revenue`
- `estimate_revenue`
- `surprise_revenue`
- `source`
- `fetched_at`
- `created_at`
- `updated_at`
- upsert key: `ticker,earnings_date,fiscal_period`

## Operational surfaces

- `data_source_runs` (run trail for market/fundamentals/earnings refresh + publish)
- `data_asset_status` (freshness/coverage per owned asset)
- `symbol_data_coverage` (ticker coverage state)

`symbol_data_coverage` required fields:

- `ticker`
- `market_data_available`
- `fundamentals_available`
- `earnings_available`
- `signal_available`
- `market_data_last_date`
- `fundamentals_last_date`
- `next_earnings_date`
- `coverage_status`
- `reason`
- `updated_at`

Migration file for v2 surfaces: [`sql/002_data_ops_v2_fundamentals_earnings.sql`](/home/franciscosantos/finance-data-ops/sql/002_data_ops_v2_fundamentals_earnings.sql)
