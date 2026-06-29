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
