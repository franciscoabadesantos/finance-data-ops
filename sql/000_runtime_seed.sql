-- Minimal bootstrap data for a fresh runtime baseline.
-- Seed only catalog rows that must exist on day 0.

insert into public.macro_series_catalog (
  series_key,
  source_provider,
  source_code,
  frequency,
  required_by_default,
  required_from_date,
  optional,
  staleness_max_bdays,
  release_calendar_source,
  description
)
values
  ('VIX', 'yfinance', '^VIX', 'daily', true, '1990-01-02', false, 5, null, 'CBOE Volatility Index close.'),
  ('VIX3M', 'yfinance', '^VIX3M', 'daily', true, '2006-07-17', false, 5, null, 'CBOE 3M volatility index close.'),
  ('VVIX', 'yfinance', '^VVIX', 'daily', true, '2007-01-03', false, 5, null, 'CBOE VVIX close.'),
  ('10Y_Treasury_Yield', 'fred', 'DGS10', 'daily', true, null, false, 5, null, '10Y treasury yield.'),
  ('2Y_Treasury_Yield', 'fred', 'DGS2', 'daily', true, null, false, 5, null, '2Y treasury yield.'),
  ('High_Yield_Spread', 'fred', 'BAMLH0A0HYM2', 'daily', true, null, false, 5, null, 'US high yield OAS.'),
  ('TED_Spread', 'fred', 'TEDRATE', 'daily', false, null, true, 5, null, 'TED spread optional.'),
  ('CPI_Headline', 'fred', 'CPIAUCSL', 'monthly', false, null, false, 45, 'bls_cpi_release_calendar_v1', 'Headline CPI.'),
  ('CPI_Core', 'fred', 'CPILFESL', 'monthly', false, null, false, 45, 'bls_cpi_release_calendar_v1', 'Core CPI.'),
  ('UNRATE', 'fred', 'UNRATE', 'monthly', true, null, false, 45, 'bls_unrate_release_calendar_v1', 'U-3 unemployment rate.'),
  ('U6RATE', 'fred', 'U6RATE', 'monthly', false, null, false, 45, 'bls_unrate_release_calendar_v1', 'U-6 unemployment rate.'),
  ('ICSA', 'fred', 'ICSA', 'weekly', false, null, false, 10, 'dol_icsa_release_calendar_v1', 'Initial jobless claims.'),
  ('CIVPART', 'fred', 'CIVPART', 'monthly', false, null, false, 45, 'bls_unrate_release_calendar_v1', 'Labor force participation.'),
  ('WTI', 'fred', 'DCOILWTICO', 'daily', false, null, false, 5, null, 'WTI spot price.'),
  ('Gasoline_US_Regular', 'fred', 'GASREGW', 'weekly', false, null, false, 10, null, 'US gasoline weekly.'),
  ('NatGas_HenryHub', 'fred', 'DHHNGSP', 'daily', false, null, false, 5, null, 'Henry Hub nat gas.'),
  ('DBC', 'yfinance', 'DBC', 'daily', false, null, false, 5, null, 'DBC ETF close.')
on conflict (series_key) do update set
  source_provider = excluded.source_provider,
  source_code = excluded.source_code,
  frequency = excluded.frequency,
  required_by_default = excluded.required_by_default,
  required_from_date = excluded.required_from_date,
  optional = excluded.optional,
  staleness_max_bdays = excluded.staleness_max_bdays,
  release_calendar_source = excluded.release_calendar_source,
  description = excluded.description,
  updated_at = now();
