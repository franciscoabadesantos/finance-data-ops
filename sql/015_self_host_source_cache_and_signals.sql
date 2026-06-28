CREATE SCHEMA IF NOT EXISTS source_cache;
CREATE SCHEMA IF NOT EXISTS feature_store;

CREATE TABLE IF NOT EXISTS source_cache.market_price_daily (
    symbol TEXT NOT NULL,
    price_date DATE NOT NULL,
    open NUMERIC(18, 6),
    high NUMERIC(18, 6),
    low NUMERIC(18, 6),
    close NUMERIC(18, 6) NOT NULL,
    adj_close NUMERIC(18, 6),
    volume BIGINT,
    source_updated_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, price_date)
);

CREATE INDEX IF NOT EXISTS ix_source_cache_market_price_daily_date
    ON source_cache.market_price_daily (price_date);

CREATE TABLE IF NOT EXISTS source_cache.macro_daily (
    as_of_date DATE NOT NULL,
    series_key TEXT NOT NULL,
    value DOUBLE PRECISION,
    source_updated_at TIMESTAMPTZ,
    alignment_mode TEXT,
    is_stale BOOLEAN,
    staleness_bdays INTEGER,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (as_of_date, series_key)
);

CREATE INDEX IF NOT EXISTS ix_source_cache_macro_daily_as_of_date
    ON source_cache.macro_daily (as_of_date);

CREATE INDEX IF NOT EXISTS ix_source_cache_macro_daily_series_key
    ON source_cache.macro_daily (series_key);

CREATE TABLE IF NOT EXISTS source_cache.fundamentals (
    symbol TEXT NOT NULL,
    report_date DATE NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION,
    value_text TEXT,
    period_end DATE NOT NULL,
    period_type TEXT NOT NULL DEFAULT 'unknown',
    fiscal_year INTEGER,
    fiscal_quarter TEXT,
    currency TEXT NOT NULL,
    source TEXT,
    source_updated_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, metric, period_end, period_type, report_date)
);

CREATE INDEX IF NOT EXISTS ix_source_cache_fundamentals_symbol_report_date
    ON source_cache.fundamentals (symbol, report_date DESC);

CREATE INDEX IF NOT EXISTS ix_source_cache_fundamentals_metric_report_date
    ON source_cache.fundamentals (metric, report_date DESC);

CREATE INDEX IF NOT EXISTS ix_source_cache_fundamentals_period_end
    ON source_cache.fundamentals (period_end DESC);

CREATE TABLE IF NOT EXISTS source_cache.earnings (
    symbol TEXT NOT NULL,
    report_date DATE NOT NULL,
    earnings_date DATE NOT NULL,
    fiscal_period TEXT NOT NULL DEFAULT 'unknown',
    earnings_time TEXT,
    actual_eps DOUBLE PRECISION,
    estimate_eps DOUBLE PRECISION,
    surprise_eps DOUBLE PRECISION,
    actual_revenue DOUBLE PRECISION,
    estimate_revenue DOUBLE PRECISION,
    surprise_revenue DOUBLE PRECISION,
    currency TEXT NOT NULL,
    source TEXT,
    source_updated_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, report_date, earnings_date, fiscal_period)
);

CREATE INDEX IF NOT EXISTS ix_source_cache_earnings_symbol_report_date
    ON source_cache.earnings (symbol, report_date DESC);

CREATE INDEX IF NOT EXISTS ix_source_cache_earnings_earnings_date
    ON source_cache.earnings (earnings_date DESC);

CREATE TABLE IF NOT EXISTS source_cache.calendars (
    exchange_mic TEXT NOT NULL,
    session_date DATE NOT NULL,
    is_trading_day BOOLEAN NOT NULL DEFAULT TRUE,
    is_half_day BOOLEAN NOT NULL DEFAULT FALSE,
    currency TEXT,
    source TEXT,
    source_updated_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exchange_mic, session_date)
);

CREATE INDEX IF NOT EXISTS ix_source_cache_calendars_session_date
    ON source_cache.calendars (session_date);

CREATE TABLE IF NOT EXISTS feature_store.entity_attributes_static (
    entity_id TEXT PRIMARY KEY,
    country TEXT NOT NULL,
    region TEXT NOT NULL,
    exchange TEXT NULL,
    exchange_mic TEXT NULL,
    currency TEXT NULL,
    sector TEXT NULL,
    name TEXT NULL,
    market_cap DOUBLE PRECISION NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE IF EXISTS feature_store.entity_attributes_static
    ADD COLUMN IF NOT EXISTS exchange TEXT NULL;

ALTER TABLE IF EXISTS feature_store.entity_attributes_static
    ADD COLUMN IF NOT EXISTS exchange_mic TEXT NULL;

ALTER TABLE IF EXISTS feature_store.entity_attributes_static
    ADD COLUMN IF NOT EXISTS currency TEXT NULL;

CREATE TABLE IF NOT EXISTS public.production_signals (
    signal_date DATE NOT NULL,
    strategy_family TEXT NOT NULL,
    universe TEXT NOT NULL,
    environment TEXT NOT NULL DEFAULT 'production',
    symbol TEXT NOT NULL,
    active_pointer_id TEXT,
    candidate_id TEXT,
    bundle_id TEXT,
    score DOUBLE PRECISION,
    rank INTEGER,
    percentile DOUBLE PRECISION,
    selected BOOLEAN,
    horizon TEXT,
    target_exposure DOUBLE PRECISION,
    signal_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_ref TEXT,
    orchestrator_run_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (signal_date, strategy_family, universe, environment, symbol)
);

CREATE INDEX IF NOT EXISTS ix_production_signals_date
    ON public.production_signals (signal_date DESC);

CREATE INDEX IF NOT EXISTS ix_production_signals_pointer
    ON public.production_signals (active_pointer_id);
