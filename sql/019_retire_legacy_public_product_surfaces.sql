-- Manual one-way cleanup for existing databases after product reads/writes
-- have moved to source_cache and feature_store.
--
-- Do not include this file in normal refresh, onboarding, or fresh-install
-- execution. Run it only as an explicit operator action after validating that
-- source_cache.market_price_daily, source_cache.fundamentals, source_cache.earnings,
-- and feature-store read models are populated.

begin;

-- Drop retired product materialized views first.
drop materialized view if exists public.mv_latest_prices;
drop materialized view if exists public.mv_latest_fundamentals;
drop materialized view if exists public.mv_next_earnings;

-- Drop retired product materialized-view refresh functions.
drop function if exists public.refresh_mv_latest_prices();
drop function if exists public.refresh_mv_latest_fundamentals();
drop function if exists public.refresh_mv_next_earnings();

-- Drop retired public product tables. Operational/control, macro,
-- release-calendar, ETF/theme, registry, analysis, and production tables are
-- intentionally not listed here.
drop table if exists public.ticker_market_stats_snapshot;
drop table if exists public.market_quotes_history;
drop table if exists public.market_quotes;
drop table if exists public.market_price_daily;
drop table if exists public.ticker_fundamental_summary;
drop table if exists public.ticker_fundamental_point_in_time;
drop table if exists public.market_fundamentals_v2;
drop table if exists public.market_earnings_events;
drop table if exists public.market_earnings_history;

commit;
