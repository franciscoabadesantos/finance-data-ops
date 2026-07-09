-- Allow the data-ops worker to run readiness cleanup/audit commands using the
-- canonical feature-store readiness surface.

grant usage on schema feature_store to finance_data_ops_worker;
grant select on feature_store.ticker_readiness to finance_data_ops_worker;
