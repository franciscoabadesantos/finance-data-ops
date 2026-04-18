-- Data Ops v4 release-availability semantics.
-- Adds explicit schedule-vs-observed availability fields for economic release calendar.

alter table if exists public.economic_release_calendar
  add column if not exists scheduled_release_timestamp_utc timestamptz;
alter table if exists public.economic_release_calendar
  add column if not exists observed_first_available_at_utc timestamptz;
alter table if exists public.economic_release_calendar
  add column if not exists availability_status text;
alter table if exists public.economic_release_calendar
  add column if not exists availability_source text;
alter table if exists public.economic_release_calendar
  add column if not exists delay_vs_schedule_seconds bigint;
alter table if exists public.economic_release_calendar
  add column if not exists is_schedule_based_only boolean not null default true;

update public.economic_release_calendar
set scheduled_release_timestamp_utc = coalesce(scheduled_release_timestamp_utc, release_timestamp_utc)
where scheduled_release_timestamp_utc is null;

update public.economic_release_calendar
set release_timestamp_utc = coalesce(release_timestamp_utc, scheduled_release_timestamp_utc)
where release_timestamp_utc is null;

update public.economic_release_calendar
set
  is_schedule_based_only = case
    when observed_first_available_at_utc is null then true
    else false
  end
where is_schedule_based_only is distinct from (
  case when observed_first_available_at_utc is null then true else false end
);

update public.economic_release_calendar
set
  delay_vs_schedule_seconds = case
    when observed_first_available_at_utc is not null and scheduled_release_timestamp_utc is not null
      then extract(epoch from (observed_first_available_at_utc - scheduled_release_timestamp_utc))::bigint
    else null
  end
where true;

update public.economic_release_calendar
set
  availability_status = case
    when observed_first_available_at_utc is not null then 'observed_available'
    when scheduled_release_timestamp_utc is not null then 'scheduled_provisional'
    else 'scheduled_provisional'
  end
where availability_status is null or btrim(availability_status) = '';

update public.economic_release_calendar
set
  availability_source = case
    when observed_first_available_at_utc is not null then coalesce(nullif(btrim(source), ''), 'migration_backfill_v1')
    else coalesce(nullif(btrim(release_calendar_source), ''), 'migration_backfill_v1')
  end
where availability_source is null or btrim(availability_source) = '';

alter table if exists public.economic_release_calendar
  alter column scheduled_release_timestamp_utc set not null;
alter table if exists public.economic_release_calendar
  alter column availability_status set not null;
alter table if exists public.economic_release_calendar
  alter column availability_source set not null;

create index if not exists idx_economic_release_calendar_status
  on public.economic_release_calendar (availability_status, scheduled_release_timestamp_utc desc);
create index if not exists idx_economic_release_calendar_observed_ts
  on public.economic_release_calendar (observed_first_available_at_utc desc);

drop materialized view if exists public.mv_latest_economic_release_calendar;

create materialized view public.mv_latest_economic_release_calendar as
select
  ranked.series_key,
  ranked.release_calendar_source,
  ranked.observation_period,
  ranked.observation_date,
  ranked.release_timestamp_utc,
  ranked.scheduled_release_timestamp_utc,
  ranked.observed_first_available_at_utc,
  ranked.availability_status,
  ranked.availability_source,
  ranked.delay_vs_schedule_seconds,
  ranked.is_schedule_based_only,
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
      order by coalesce(e.observed_first_available_at_utc, e.scheduled_release_timestamp_utc) desc nulls last,
               e.scheduled_release_timestamp_utc desc nulls last,
               e.ingested_at desc nulls last
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

