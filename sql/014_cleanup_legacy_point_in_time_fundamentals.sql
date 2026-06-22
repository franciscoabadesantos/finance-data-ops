-- DESTRUCTIVE, TARGETED CLEANUP.
-- Removes legacy point-in-time metrics from market_fundamentals_v2 after
-- sql/013_fundamentals_point_in_time_snapshot.sql has populated
-- ticker_fundamental_point_in_time.
--
-- This intentionally excludes ambiguous metrics that can also be fiscal
-- statement metrics, such as eps, ebitda, and free_cash_flow.

begin;

do $$
begin
  if to_regclass('public.ticker_fundamental_point_in_time') is null then
    raise exception 'ticker_fundamental_point_in_time is missing; run sql/013 first';
  end if;
end $$;

with cleanup_metrics(metric) as (
  values
    ('market_cap'),
    ('shares_outstanding'),
    ('trailing_pe'),
    ('dividend_yield'),
    ('dividend_rate'),
    ('trailing_annual_dividend_yield'),
    ('trailing_annual_dividend_rate'),
    ('payout_ratio'),
    ('beta'),
    ('beta_3y'),
    ('ytd_return'),
    ('three_year_avg_return'),
    ('five_year_avg_return'),
    ('ex_dividend_date'),
    ('payout_frequency')
)
select
  'pre_delete_candidates' as check_name,
  count(*) as rows
from public.market_fundamentals_v2 f
join cleanup_metrics m
  on m.metric = f.metric
where exists (
  select 1
  from public.ticker_fundamental_point_in_time p
  where p.ticker = f.ticker
    and p.metric = f.metric
);

with cleanup_metrics(metric) as (
  values
    ('market_cap'),
    ('shares_outstanding'),
    ('trailing_pe'),
    ('dividend_yield'),
    ('dividend_rate'),
    ('trailing_annual_dividend_yield'),
    ('trailing_annual_dividend_rate'),
    ('payout_ratio'),
    ('beta'),
    ('beta_3y'),
    ('ytd_return'),
    ('three_year_avg_return'),
    ('five_year_avg_return'),
    ('ex_dividend_date'),
    ('payout_frequency')
),
deleted as (
  delete from public.market_fundamentals_v2 f
  using cleanup_metrics m
  where m.metric = f.metric
    and exists (
      select 1
      from public.ticker_fundamental_point_in_time p
      where p.ticker = f.ticker
        and p.metric = f.metric
    )
  returning f.ticker, f.metric, f.period_end
)
select
  'deleted_rows' as check_name,
  count(*) as rows
from deleted;

refresh materialized view public.mv_latest_fundamentals;

with cleanup_metrics(metric) as (
  values
    ('market_cap'),
    ('shares_outstanding'),
    ('trailing_pe'),
    ('dividend_yield'),
    ('dividend_rate'),
    ('trailing_annual_dividend_yield'),
    ('trailing_annual_dividend_rate'),
    ('payout_ratio'),
    ('beta'),
    ('beta_3y'),
    ('ytd_return'),
    ('three_year_avg_return'),
    ('five_year_avg_return'),
    ('ex_dividend_date'),
    ('payout_frequency')
)
select
  'post_delete_remaining_candidates' as check_name,
  count(*) as rows
from public.market_fundamentals_v2 f
join cleanup_metrics m
  on m.metric = f.metric;

select
  'point_in_time_snapshot_rows' as check_name,
  count(*) as rows
from public.ticker_fundamental_point_in_time;

commit;
