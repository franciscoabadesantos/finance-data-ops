-- Fundamentals point-in-time snapshot surface.
-- Non-destructive migration: creates the snapshot table, backfills the latest
-- known point-in-time-style metrics, and keeps historical rows untouched.

create table if not exists public.ticker_fundamental_point_in_time (
  ticker text not null,
  metric text not null,
  value double precision,
  value_text text,
  as_of_date date not null,
  source text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (ticker, metric)
);

create index if not exists idx_ticker_fundamental_point_in_time_as_of
  on public.ticker_fundamental_point_in_time (as_of_date desc);

create index if not exists idx_ticker_fundamental_point_in_time_ticker_as_of
  on public.ticker_fundamental_point_in_time (ticker, as_of_date desc);

insert into public.ticker_fundamental_point_in_time (
  ticker,
  metric,
  value,
  value_text,
  as_of_date,
  source,
  fetched_at,
  updated_at
)
select distinct on (ticker, metric)
  ticker,
  metric,
  value,
  value_text,
  period_end as as_of_date,
  source,
  fetched_at,
  fetched_at as updated_at
from public.market_fundamentals_v2
where metric in (
  'market_cap',
  'shares_outstanding',
  'trailing_pe',
  'eps',
  'ebitda',
  'free_cash_flow',
  'dividend_yield',
  'dividend_rate',
  'trailing_annual_dividend_yield',
  'trailing_annual_dividend_rate',
  'payout_ratio',
  'beta',
  'beta_3y',
  'ytd_return',
  'three_year_avg_return',
  'five_year_avg_return',
  'ex_dividend_date',
  'payout_frequency'
)
order by ticker, metric, period_end desc, fetched_at desc, period desc
on conflict (ticker, metric) do update set
  value = excluded.value,
  value_text = excluded.value_text,
  as_of_date = excluded.as_of_date,
  source = excluded.source,
  fetched_at = excluded.fetched_at,
  updated_at = now();

drop materialized view if exists public.mv_latest_fundamentals;

create materialized view public.mv_latest_fundamentals as
select
  ranked.ticker,
  ranked.period,
  ranked.period_end,
  ranked.metric,
  ranked.value,
  ranked.value_text,
  ranked.source,
  ranked.fetched_at
from (
  select
    f.*,
    row_number() over (
      partition by f.ticker, f.metric
      order by f.period_end desc, f.fetched_at desc, f.period desc
    ) as row_num
  from public.market_fundamentals_v2 f
  where f.metric not in (
    'market_cap',
    'shares_outstanding',
    'trailing_pe',
    'eps',
    'ebitda',
    'free_cash_flow',
    'dividend_yield',
    'dividend_rate',
    'trailing_annual_dividend_yield',
    'trailing_annual_dividend_rate',
    'payout_ratio',
    'beta',
    'beta_3y',
    'ytd_return',
    'three_year_avg_return',
    'five_year_avg_return',
    'ex_dividend_date',
    'payout_frequency'
  )
) ranked
where ranked.row_num = 1

union all

select
  p.ticker,
  'point_in_time' as period,
  p.as_of_date as period_end,
  p.metric,
  p.value,
  p.value_text,
  p.source,
  p.fetched_at
from public.ticker_fundamental_point_in_time p;

create unique index if not exists idx_mv_latest_fundamentals_ticker_metric
  on public.mv_latest_fundamentals (ticker, metric);

create or replace function public.refresh_mv_latest_fundamentals()
returns void
language plpgsql
security definer
as $$
begin
  refresh materialized view public.mv_latest_fundamentals;
end;
$$;
