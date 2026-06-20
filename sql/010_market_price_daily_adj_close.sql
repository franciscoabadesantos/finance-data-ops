alter table if exists public.market_price_daily
  add column if not exists adj_close double precision;
