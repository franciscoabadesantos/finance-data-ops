from __future__ import annotations

from pathlib import Path

from scripts import reconcile_etf_canonical_schema as reconcile

TICKER_READINESS_COLUMNS = (
    ("symbol", "text", "text"),
    ("name", "text", "text"),
    ("registry_key", "text", "text"),
    ("registry_status", "text", "text"),
    ("validation_status", "text", "text"),
    ("promotion_status", "text", "text"),
    ("validation_reason", "text", "text"),
    ("has_prices", "boolean", "bool"),
    ("price_row_count", "integer", "int4"),
    ("first_price_date", "date", "date"),
    ("last_price_date", "date", "date"),
    ("has_technicals", "boolean", "bool"),
    ("technical_row_count", "integer", "int4"),
    ("first_technical_date", "date", "date"),
    ("last_technical_date", "date", "date"),
    ("has_fundamentals", "boolean", "bool"),
    ("has_earnings", "boolean", "bool"),
    ("has_scorecard", "boolean", "bool"),
    ("latest_scorecard_as_of", "date", "date"),
    ("is_tracked", "boolean", "bool"),
    ("is_scorecard_ready", "boolean", "bool"),
    ("coverage_state", "text", "text"),
    ("missing_inputs", "ARRAY", "_text"),
    ("candidate_sources", "ARRAY", "_text"),
    ("computed_at", "timestamp with time zone", "timestamptz"),
)

ETF_CONSTITUENT_COLUMNS = (
    ("symbol", "text", "text"),
    ("etf_symbol", "text", "text"),
    ("source", "text", "text"),
)

TICKER_READINESS_DEFINITION = """
select
  symbol,
  name,
  registry_key,
  registry_status,
  validation_status,
  promotion_status,
  validation_reason,
  has_prices,
  price_row_count,
  first_price_date,
  last_price_date,
  has_technicals,
  technical_row_count,
  first_technical_date,
  last_technical_date,
  has_fundamentals,
  has_earnings,
  has_scorecard,
  latest_scorecard_as_of,
  is_tracked,
  is_scorecard_ready,
  coverage_state,
  missing_inputs,
  candidate_sources,
  computed_at
from feature_store.current_ticker_readiness_source
"""

ETF_CONSTITUENT_DEFINITION = """
select
  upper(h.holding_symbol) as symbol,
  upper(h.etf_ticker) as etf_symbol,
  'etf_holdings'::text as source
from source_cache.etf_holdings h
"""


def test_dry_run_plan_detects_old_views_and_public_tables() -> None:
    states = _old_live_states()

    plan = reconcile.build_plan(
        states=states,
        dependencies=_known_dependency_states(),
        backend_read_roles=("finance_backend_read",),
    )

    replace_actions = [action for action in plan if action["action"] == "replace_view_with_table"]
    copy_actions = [action for action in plan if action["action"] == "copy_old_public_rows"]
    assert {action["object"] for action in replace_actions} == {
        "source_cache.etf_holdings",
        "source_cache.etf_themes",
    }
    assert {action["object"] for action in copy_actions} == {"public.etf_holdings", "public.etf_themes"}
    assert next(action for action in copy_actions if action["object"] == "public.etf_holdings")["rows"] == 506
    assert any(action["action"] == "drop_known_dependent_views" for action in plan)
    assert any(action["action"] == "ensure_readiness_snapshot_table" for action in plan)
    assert any(action["action"] == "rebuild_readiness" for action in plan)
    assert any(action["action"] == "recreate_known_dependent_views" for action in plan)


def test_apply_replaces_views_creates_writable_tables_and_copies_rows() -> None:
    conn = RecordingConnection()

    reconcile.apply_plan(
        conn,
        states=_old_live_states(),
        dependencies=_known_dependency_states(),
        backend_read_roles=("finance_backend_read",),
    )

    sql = conn.normalized_sql
    assert "cascade" not in sql
    assert sql.index("select pg_get_viewdef") < sql.index("drop view if exists feature_store.ticker_readiness")
    assert sql.index("drop view if exists feature_store.ticker_readiness") < sql.index(
        "drop view if exists feature_store.ticker_readiness_etf_constituents"
    )
    assert sql.index("drop view if exists feature_store.ticker_readiness_etf_constituents") < sql.index(
        "drop view source_cache.etf_holdings"
    )
    assert "drop view source_cache.etf_holdings" in sql
    assert "drop view source_cache.etf_themes" in sql
    assert "create table if not exists source_cache.etf_holdings" in sql
    assert "create table if not exists source_cache.etf_themes" in sql
    assert "create table if not exists source_cache.etf_theme_readiness" in sql
    assert "insert into source_cache.etf_holdings" in sql
    assert "from public.etf_holdings" in sql
    assert "insert into source_cache.etf_themes" in sql
    assert "from public.etf_themes" in sql
    assert "truncate table source_cache.etf_theme_readiness" in sql
    assert "insert into source_cache.etf_theme_readiness" in sql
    assert 'create view "feature_store"."ticker_readiness_etf_constituents" as' in sql
    assert 'create view "feature_store"."ticker_readiness" as' in sql
    assert "from feature_store.current_ticker_readiness_source" in sql
    assert sql.index('create view "feature_store"."ticker_readiness_etf_constituents" as') < sql.index(
        'create view "feature_store"."ticker_readiness" as'
    )


def test_live_like_ticker_readiness_contract_is_preserved_after_apply() -> None:
    conn = RecordingConnection()

    reconcile.apply_plan(conn, states=_old_live_states(), dependencies=_known_dependency_states())

    assert conn.columns_by_view[("feature_store", "ticker_readiness")] == TICKER_READINESS_COLUMNS
    assert "registry_key" in conn.normalized_sql
    assert "candidate_sources" in conn.normalized_sql
    assert "readiness_status" not in conn.normalized_sql
    assert "market_data_available" not in conn.normalized_sql
    assert conn.column_fetch_counts[("feature_store", "ticker_readiness")] >= 2


def test_apply_creates_readiness_rows_from_prices_and_technicals_contract() -> None:
    conn = RecordingConnection()

    reconcile.apply_plan(conn, states=_old_live_states(), dependencies=_known_dependency_states())

    sql = conn.normalized_sql
    assert "from source_cache.market_price_daily p" in sql
    assert "from feature_store.technical_features_daily t" in sql
    assert "technical_constituent_count >= 5" in sql
    assert ">= 0.10" in sql
    assert "'insufficient_constituent_coverage'" in sql


def test_apply_grants_are_conditional_for_worker_and_backend_read_roles() -> None:
    conn = RecordingConnection()

    reconcile.apply_plan(
        conn,
        states=_old_live_states(),
        dependencies=_known_dependency_states(),
        backend_read_roles=("finance_backend_read", "backend_read"),
    )

    sql = conn.normalized_sql
    assert "if exists (select 1 from pg_roles where rolname = 'finance_data_ops_worker')" in sql
    assert "grant select, insert, update, delete on source_cache.etf_holdings, source_cache.etf_themes, source_cache.etf_theme_readiness to finance_data_ops_worker" in sql
    assert "grant usage on schema feature_store to finance_data_ops_worker" in sql
    assert "grant select on feature_store.ticker_readiness to finance_data_ops_worker" in sql
    assert "grant select on feature_store.ticker_readiness_etf_constituents to finance_data_ops_worker" in sql
    assert "foreach read_role in array array['finance_backend_read', 'backend_read'] loop" in sql
    assert "grant select on source_cache.etf_holdings, source_cache.etf_themes, source_cache.etf_theme_readiness to %i" in sql
    assert "grant usage on schema feature_store to %i" in sql
    assert "grant select on feature_store.ticker_readiness to %i" in sql
    assert "grant select on feature_store.ticker_readiness_etf_constituents to %i" in sql


def test_recreated_dependent_view_grants_are_applied_after_view_recreation() -> None:
    conn = RecordingConnection()

    reconcile.apply_plan(
        conn,
        states=_old_live_states(),
        dependencies=_known_dependency_states(),
        backend_read_roles=("finance_backend_read",),
    )

    sql = conn.normalized_sql
    assert sql.index('create view "feature_store"."ticker_readiness" as') < sql.index(
        "grant select on feature_store.ticker_readiness to finance_data_ops_worker"
    )
    assert sql.index('create view "feature_store"."ticker_readiness_etf_constituents" as') < sql.index(
        "grant select on feature_store.ticker_readiness_etf_constituents to finance_data_ops_worker"
    )
    assert "to_regclass('feature_store.ticker_readiness') is not null" in sql
    assert "to_regclass('feature_store.ticker_readiness_etf_constituents') is not null" in sql


def test_reconciler_does_not_hardcode_feature_store_ticker_readiness_definition() -> None:
    script = Path("scripts/reconcile_etf_canonical_schema.py").read_text()

    assert "CREATE_TICKER_READINESS_VIEW_SQL" not in script
    assert "readiness_status" not in script
    assert "market_data_available" not in script
    assert "pg_get_viewdef" in script


def test_unexpected_dependencies_fail_with_manual_review_and_no_cascade() -> None:
    dependencies = [
        *_known_dependency_states(),
        reconcile.DependencyState("feature_store", "unexpected_view", "v", "source_cache", "etf_holdings"),
    ]
    plan = reconcile.build_plan(states=_old_live_states(), dependencies=dependencies)

    manual_review = next(action for action in plan if action["action"] == "manual_review_required")
    assert manual_review["reason"] == "unexpected_dependencies"
    assert manual_review["dependencies"][0]["dependent"] == "feature_store.unexpected_view"

    conn = RecordingConnection()
    try:
        reconcile.apply_plan(conn, states=_old_live_states(), dependencies=dependencies)
    except RuntimeError as exc:
        assert "unexpected ETF dependencies" in str(exc)
    else:  # pragma: no cover - explicit assertion for readability
        raise AssertionError("expected unexpected dependency failure")
    assert "cascade" not in conn.normalized_sql


def test_statement_failure_reports_original_sql_and_stops() -> None:
    conn = RecordingConnection(fail_on="drop view source_cache.etf_holdings")

    try:
        reconcile.apply_plan(conn, states=_old_live_states())
    except reconcile.ReconciliationSQLError as exc:
        message = str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected statement failure")

    assert "drop view source_cache.etf_holdings" in message
    assert "simulated sql failure" in message
    assert "create table if not exists source_cache.etf_holdings" not in conn.normalized_sql


def test_count_permission_error_does_not_abort_dry_run_inspection() -> None:
    conn = InspectConnection(count_failures={("source_cache", "etf_holdings"): "permission denied for view"})
    warnings: list[dict[str, object]] = []

    states = reconcile.inspect_objects(conn, warnings=warnings)
    dependencies = reconcile.inspect_dependencies(conn, warnings=warnings)

    by_object = {state.qualified_name: state for state in states}
    assert by_object["source_cache.etf_holdings"].rows is None
    assert by_object["source_cache.etf_holdings"].row_count_error == "permission denied for view"
    assert dependencies[0].dependent_qualified_name == "feature_store.ticker_readiness_etf_constituents"
    assert warnings == [
        {
            "object": "source_cache.etf_holdings",
            "warning": "row_count_unavailable",
            "error": "permission denied for view",
        }
    ]
    assert conn.aborted is False
    assert any("rollback to savepoint etf_count_source_cache_etf_holdings" in statement for statement in conn.normalized)


def test_dependency_inspection_permission_error_reports_warning_without_aborting() -> None:
    conn = InspectConnection(dependency_failure="permission denied for pg_depend")
    warnings: list[dict[str, object]] = []

    dependencies = reconcile.inspect_dependencies(conn, warnings=warnings)

    assert dependencies == []
    assert warnings == [
        {
            "object": "pg_catalog.dependencies",
            "warning": "dependency_inspection_unavailable",
            "error": "permission denied for pg_depend",
        }
    ]
    assert conn.aborted is False


def test_apply_with_worker_or_non_ddl_role_fails_early_and_clearly() -> None:
    worker_conn = RecordingConnection(current_user="finance_data_ops_worker", ddl_capable=False)

    try:
        reconcile.apply_plan(worker_conn, states=_old_live_states(), dependencies=_known_dependency_states())
    except RuntimeError as exc:
        assert "apply_requires_admin_role" in str(exc)
        assert "finance_data_ops_worker cannot run" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected worker role to be rejected")
    assert "drop view" not in worker_conn.normalized_sql

    readonly_conn = RecordingConnection(current_user="data_ops_readonly", ddl_capable=False)
    try:
        reconcile.apply_plan(readonly_conn, states=_old_live_states(), dependencies=_known_dependency_states())
    except RuntimeError as exc:
        assert "apply_requires_admin_role" in str(exc)
        assert "lacks source_cache DDL privilege" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected non-DDL role to be rejected")
    assert "drop view" not in readonly_conn.normalized_sql


def test_runtime_schema_contains_final_etf_contract_and_conditional_grants() -> None:
    schema = Path("sql/000_runtime_schema.sql").read_text()

    assert "create table if not exists source_cache.etf_holdings" in schema
    assert "create table if not exists source_cache.etf_themes" in schema
    assert "create table if not exists source_cache.etf_theme_readiness" in schema
    assert "create table if not exists public.etf_holdings" not in schema
    assert "create table if not exists public.etf_themes" not in schema
    assert "Operational identity read model" in schema
    assert "grant select, insert, update, delete" in schema
    assert "source_cache.etf_theme_readiness" in schema
    assert "where rolname = 'finance_data_ops_worker'" in schema


class RecordingConnection:
    def __init__(
        self,
        *,
        fail_on: str | None = None,
        current_user: str = "postgres",
        session_user: str | None = None,
        ddl_capable: bool = True,
    ) -> None:
        self.statements: list[str] = []
        self.fail_on = fail_on
        self.current_user = current_user
        self.session_user = session_user or current_user
        self.ddl_capable = ddl_capable
        self.definitions_by_view = {
            ("feature_store", "ticker_readiness"): TICKER_READINESS_DEFINITION,
            ("feature_store", "ticker_readiness_etf_constituents"): ETF_CONSTITUENT_DEFINITION,
        }
        self.columns_by_view = {
            ("feature_store", "ticker_readiness"): TICKER_READINESS_COLUMNS,
            ("feature_store", "ticker_readiness_etf_constituents"): ETF_CONSTITUENT_COLUMNS,
        }
        self.column_fetch_counts: dict[tuple[str, str], int] = {}

    def cursor(self) -> "RecordingCursor":
        return RecordingCursor(self)

    @property
    def normalized_sql(self) -> str:
        return " ".join(" ".join(statement.lower().split()) for statement in self.statements)


class RecordingCursor:
    def __init__(self, conn: RecordingConnection) -> None:
        self.conn = conn
        self._rows: list[tuple[object, ...]] = []

    def __enter__(self) -> "RecordingCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, statement: str, params: object | None = None) -> None:
        normalized = " ".join(str(statement).lower().split())
        if self.conn.fail_on and self.conn.fail_on in normalized:
            raise RuntimeError("simulated sql failure")
        if "select current_user" in normalized and "session_user" in normalized:
            self._rows = [(self.conn.current_user, self.conn.session_user, self.conn.ddl_capable)]
        elif "select pg_get_viewdef" in normalized:
            key = (str(params[0]), str(params[1])) if params else ("", "")
            self._rows = [(self.conn.definitions_by_view.get(key, ""),)]
        elif "from information_schema.columns" in normalized:
            key = (str(params[0]), str(params[1])) if params else ("", "")
            self.conn.column_fetch_counts[key] = self.conn.column_fetch_counts.get(key, 0) + 1
            self._rows = list(self.conn.columns_by_view.get(key, ()))
        self.conn.statements.append(statement)

    def fetchone(self) -> tuple[object, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


def _old_live_states() -> list[reconcile.ObjectState]:
    return [
        reconcile.ObjectState("source_cache", "etf_holdings", "v", 506),
        reconcile.ObjectState("source_cache", "etf_themes", "v", 32),
        reconcile.ObjectState("source_cache", "etf_theme_readiness", None, None),
        reconcile.ObjectState("public", "etf_holdings", "r", 506),
        reconcile.ObjectState("public", "etf_themes", "r", 32),
    ]


def _known_dependency_states() -> list[reconcile.DependencyState]:
    return [
        reconcile.DependencyState(
            "feature_store",
            "ticker_readiness_etf_constituents",
            "v",
            "source_cache",
            "etf_holdings",
        ),
        reconcile.DependencyState(
            "feature_store",
            "ticker_readiness",
            "v",
            "feature_store",
            "ticker_readiness_etf_constituents",
        ),
    ]


class InspectConnection:
    def __init__(
        self,
        *,
        count_failures: dict[tuple[str, str], str] | None = None,
        dependency_failure: str | None = None,
    ) -> None:
        self.statements: list[str] = []
        self.aborted = False
        self.count_failures = dict(count_failures or {})
        self.dependency_failure = dependency_failure

    def cursor(self) -> "InspectCursor":
        return InspectCursor(self)

    @property
    def normalized(self) -> list[str]:
        return [" ".join(statement.lower().split()) for statement in self.statements]


class InspectCursor:
    def __init__(self, conn: InspectConnection) -> None:
        self.conn = conn
        self._rows: list[tuple[object, ...]] = []

    def __enter__(self) -> "InspectCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, statement: str, params: object | None = None) -> None:
        normalized = " ".join(str(statement).lower().split())
        if self.conn.aborted and not normalized.startswith(("rollback to savepoint", "release savepoint")):
            raise RuntimeError("current transaction is aborted")
        self.conn.statements.append(statement)
        if normalized.startswith("rollback to savepoint"):
            self.conn.aborted = False
            return
        if normalized.startswith(("savepoint", "release savepoint")):
            return
        if "from pg_class c" in normalized and "c.relkind" in normalized:
            self._rows = [
                ("source_cache", "etf_holdings", "v"),
                ("source_cache", "etf_themes", "v"),
                ("public", "etf_holdings", "r"),
                ("public", "etf_themes", "r"),
            ]
            return
        if normalized.startswith('select count(*) from "'):
            schema_name, table_name = _count_target(normalized)
            failure = self.conn.count_failures.get((schema_name, table_name))
            if failure:
                self.conn.aborted = True
                raise RuntimeError(failure)
            self._rows = [(506 if table_name == "etf_holdings" else 32,)]
            return
        if "with recursive target_objects as" in normalized:
            if self.conn.dependency_failure:
                self.conn.aborted = True
                raise RuntimeError(self.conn.dependency_failure)
            self._rows = [
                (
                    "feature_store",
                    "ticker_readiness_etf_constituents",
                    "v",
                    "source_cache",
                    "etf_holdings",
                )
            ]
            return
        self._rows = []

    def fetchone(self) -> tuple[object, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


def _count_target(normalized: str) -> tuple[str, str]:
    marker = 'select count(*) from "'
    rest = normalized.split(marker, 1)[1]
    schema_name, rest = rest.split('"."', 1)
    table_name = rest.split('"', 1)[0]
    return schema_name, table_name
