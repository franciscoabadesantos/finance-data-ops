-- Data Ops v3 schema surfaces (macro + economic release calendar).
-- Ownership: finance-data-ops is the only fetch/normalize/publish boundary for these domains.
-- Migration strategy: additive + idempotent; no destructive changes.

create extension if not exists pgcrypto;

-- Canonical macro series metadata and staleness/alignment policy.
create table if not exists public.macro_series_catalog (
  series_key text primary key,
  source_provider text not null,
  source_code text not null,
  frequency text not null,
  required_by_default boolean not null default false,
  optional boolean not null default false,
  staleness_max_bdays integer not null default 0,
  release_calendar_source text,
  description text,
  updated_at timestamptz not null default now()
);

alter table if exists public.macro_series_catalog
  add column if not exists series_key text;
alter table if exists public.macro_series_catalog
  add column if not exists source_provider text;
alter table if exists public.macro_series_catalog
  add column if not exists source_code text;
alter table if exists public.macro_series_catalog
  add column if not exists frequency text;
alter table if exists public.macro_series_catalog
  add column if not exists required_by_default boolean not null default false;
alter table if exists public.macro_series_catalog
  add column if not exists optional boolean not null default false;
alter table if exists public.macro_series_catalog
  add column if not exists staleness_max_bdays integer not null default 0;
alter table if exists public.macro_series_catalog
  add column if not exists release_calendar_source text;
alter table if exists public.macro_series_catalog
  add column if not exists description text;
alter table if exists public.macro_series_catalog
  add column if not exists updated_at timestamptz not null default now();

-- Canonical macro observations (observation-level history).
create table if not exists public.macro_observations (
  series_key text not null,
  observation_period text not null,
  observation_date date not null,
  frequency text not null,
  value double precision,
  source_provider text,
  source_code text,
  release_timestamp_utc timestamptz,
  release_timezone text,
  release_date_local date,
  release_calendar_source text,
  source text,
  fetched_at timestamptz not null default now(),
  ingested_at timestamptz not null default now(),
  primary key (series_key, observation_period)
);

alter table if exists public.macro_observations
  add column if not exists series_key text;
alter table if exists public.macro_observations
  add column if not exists observation_period text;
alter table if exists public.macro_observations
  add column if not exists observation_date date;
alter table if exists public.macro_observations
  add column if not exists frequency text;
alter table if exists public.macro_observations
  add column if not exists value double precision;
alter table if exists public.macro_observations
  add column if not exists source_provider text;
alter table if exists public.macro_observations
  add column if not exists source_code text;
alter table if exists public.macro_observations
  add column if not exists release_timestamp_utc timestamptz;
alter table if exists public.macro_observations
  add column if not exists release_timezone text;
alter table if exists public.macro_observations
  add column if not exists release_date_local date;
alter table if exists public.macro_observations
  add column if not exists release_calendar_source text;
alter table if exists public.macro_observations
  add column if not exists source text;
alter table if exists public.macro_observations
  add column if not exists fetched_at timestamptz not null default now();
alter table if exists public.macro_observations
  add column if not exists ingested_at timestamptz not null default now();

create index if not exists idx_macro_observations_series_observation_date
  on public.macro_observations (series_key, observation_date desc);
create index if not exists idx_macro_observations_release_ts
  on public.macro_observations (release_calendar_source, release_timestamp_utc desc);

-- Canonical business-day aligned macro surface.
create table if not exists public.macro_daily (
  as_of_date date not null,
  series_key text not null,
  value double precision,
  source_observation_period text,
  source_observation_date date,
  available_at_utc timestamptz,
  staleness_bdays integer,
  is_stale boolean not null default false,
  alignment_mode text not null default 'release_timed',
  ingested_at timestamptz not null default now(),
  primary key (as_of_date, series_key)
);

alter table if exists public.macro_daily
  add column if not exists as_of_date date;
alter table if exists public.macro_daily
  add column if not exists series_key text;
alter table if exists public.macro_daily
  add column if not exists value double precision;
alter table if exists public.macro_daily
  add column if not exists source_observation_period text;
alter table if exists public.macro_daily
  add column if not exists source_observation_date date;
alter table if exists public.macro_daily
  add column if not exists available_at_utc timestamptz;
alter table if exists public.macro_daily
  add column if not exists staleness_bdays integer;
alter table if exists public.macro_daily
  add column if not exists is_stale boolean not null default false;
alter table if exists public.macro_daily
  add column if not exists alignment_mode text not null default 'release_timed';
alter table if exists public.macro_daily
  add column if not exists ingested_at timestamptz not null default now();

create index if not exists idx_macro_daily_series_asof
  on public.macro_daily (series_key, as_of_date desc);
create index if not exists idx_macro_daily_asof
  on public.macro_daily (as_of_date desc);

-- Canonical release/event calendar.
create table if not exists public.economic_release_calendar (
  series_key text not null,
  observation_period text not null,
  observation_date date not null,
  release_timestamp_utc timestamptz not null,
  release_timezone text not null,
  release_date_local date not null,
  release_calendar_source text not null,
  source text,
  provenance_class text,
  ingested_at timestamptz not null default now(),
  primary key (series_key, observation_period)
);

alter table if exists public.economic_release_calendar
  add column if not exists series_key text;
alter table if exists public.economic_release_calendar
  add column if not exists observation_period text;
alter table if exists public.economic_release_calendar
  add column if not exists observation_date date;
alter table if exists public.economic_release_calendar
  add column if not exists release_timestamp_utc timestamptz;
alter table if exists public.economic_release_calendar
  add column if not exists release_timezone text;
alter table if exists public.economic_release_calendar
  add column if not exists release_date_local date;
alter table if exists public.economic_release_calendar
  add column if not exists release_calendar_source text;
alter table if exists public.economic_release_calendar
  add column if not exists source text;
alter table if exists public.economic_release_calendar
  add column if not exists provenance_class text;
alter table if exists public.economic_release_calendar
  add column if not exists ingested_at timestamptz not null default now();

create index if not exists idx_economic_release_calendar_release_source_ts
  on public.economic_release_calendar (release_calendar_source, release_timestamp_utc desc);
create index if not exists idx_economic_release_calendar_series_release_ts
  on public.economic_release_calendar (series_key, release_timestamp_utc desc);

-- Latest macro observation per series.
create materialized view if not exists public.mv_latest_macro_observations as
select
  ranked.series_key,
  ranked.observation_period,
  ranked.observation_date,
  ranked.frequency,
  ranked.value,
  ranked.source_provider,
  ranked.source_code,
  ranked.release_timestamp_utc,
  ranked.release_timezone,
  ranked.release_date_local,
  ranked.release_calendar_source,
  ranked.source,
  ranked.fetched_at,
  ranked.ingested_at
from (
  select
    o.*,
    row_number() over (
      partition by o.series_key
      order by o.observation_date desc nulls last, o.fetched_at desc nulls last, o.ingested_at desc nulls last
    ) as row_num
  from public.macro_observations o
) ranked
where ranked.row_num = 1;

create unique index if not exists idx_mv_latest_macro_observations_series
  on public.mv_latest_macro_observations (series_key);

create or replace function public.refresh_mv_latest_macro_observations()
returns void
language plpgsql
security definer
as $$
begin
  refresh materialized view public.mv_latest_macro_observations;
end;
$$;

-- Latest release-calendar timestamp per (series, release calendar source).
create materialized view if not exists public.mv_latest_economic_release_calendar as
select
  ranked.series_key,
  ranked.release_calendar_source,
  ranked.observation_period,
  ranked.observation_date,
  ranked.release_timestamp_utc,
  ranked.release_timezone,
  ranked.release_date_local,
  ranked.source,
  ranked.provenance_class,
  ranked.ingested_at
from (
  select
    e.*,
    row_number() over (
      partition by e.series_key, e.release_calendar_source
      order by e.release_timestamp_utc desc nulls last, e.ingested_at desc nulls last
    ) as row_num
  from public.economic_release_calendar e
) ranked
where ranked.row_num = 1;

create unique index if not exists idx_mv_latest_economic_release_calendar_series_source
  on public.mv_latest_economic_release_calendar (series_key, release_calendar_source);

create or replace function public.refresh_mv_latest_economic_release_calendar()
returns void
language plpgsql
security definer
as $$
begin
  refresh materialized view public.mv_latest_economic_release_calendar;
end;
$$;
