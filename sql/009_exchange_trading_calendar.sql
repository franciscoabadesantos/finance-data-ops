-- Dias de negociacao por bolsa (fonte: exchange_calendars).
-- Lido pelo backend para cobertura internacional.
create table if not exists public.exchange_trading_calendar (
  exchange_mic text not null,
  session_date date not null,
  is_half_day boolean not null default false,
  ingested_at timestamptz not null default now(),
  primary key (exchange_mic, session_date)
);

create index if not exists idx_exchange_trading_calendar_mic_date
  on public.exchange_trading_calendar (exchange_mic, session_date);

-- Groundwork: bolsa + moeda por ticker no registry.
alter table if exists public.ticker_registry
  add column if not exists exchange_mic text;

alter table if exists public.ticker_registry
  add column if not exists currency text;
