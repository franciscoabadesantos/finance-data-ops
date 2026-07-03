-- Thematic ETF catalog labels for relationship-map theme_etf evidence.

create table if not exists public.etf_themes (
  etf_ticker text primary key,
  theme text not null,
  wave integer not null check (wave in (1, 2)),
  issuer text,
  source_type text not null,
  source_ref text,
  active boolean not null default true,
  fetched_at timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists idx_etf_themes_theme_wave
  on public.etf_themes (theme, wave, active);
