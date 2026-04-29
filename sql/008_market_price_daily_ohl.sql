alter table if exists public.market_price_daily
  add column if not exists open double precision;

alter table if exists public.market_price_daily
  add column if not exists high double precision;

alter table if exists public.market_price_daily
  add column if not exists low double precision;
