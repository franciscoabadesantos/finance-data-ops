-- Capture vendor sector/industry profile attributes on latest market quotes.
alter table if exists public.market_quotes
  add column if not exists sector text;

alter table if exists public.market_quotes
  add column if not exists industry text;

-- After the first refresh/backfill, inspect the raw vendor vocabulary with:
-- select sector, count(*) as tickers
-- from public.market_quotes
-- group by sector
-- order by sector nulls last;
