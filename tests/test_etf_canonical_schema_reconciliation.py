from __future__ import annotations

from pathlib import Path

from scripts import reconcile_etf_canonical_schema as reconcile


def test_dry_run_plan_detects_old_views_and_public_tables() -> None:
    states = _old_live_states()

    plan = reconcile.build_plan(states=states, backend_read_roles=("finance_backend_read",))

    replace_actions = [action for action in plan if action["action"] == "replace_view_with_table"]
    copy_actions = [action for action in plan if action["action"] == "copy_old_public_rows"]
    assert {action["object"] for action in replace_actions} == {
        "source_cache.etf_holdings",
        "source_cache.etf_themes",
    }
    assert {action["object"] for action in copy_actions} == {"public.etf_holdings", "public.etf_themes"}
    assert next(action for action in copy_actions if action["object"] == "public.etf_holdings")["rows"] == 506
    assert any(action["action"] == "ensure_readiness_snapshot_table" for action in plan)
    assert any(action["action"] == "rebuild_readiness" for action in plan)


def test_apply_replaces_views_creates_writable_tables_and_copies_rows() -> None:
    conn = RecordingConnection()

    reconcile.apply_plan(conn, states=_old_live_states(), backend_read_roles=("finance_backend_read",))

    sql = conn.normalized_sql
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


def test_apply_creates_readiness_rows_from_prices_and_technicals_contract() -> None:
    conn = RecordingConnection()

    reconcile.apply_plan(conn, states=_old_live_states())

    sql = conn.normalized_sql
    assert "from source_cache.market_price_daily p" in sql
    assert "from feature_store.technical_features_daily t" in sql
    assert "technical_constituent_count >= 5" in sql
    assert ">= 0.10" in sql
    assert "'insufficient_constituent_coverage'" in sql


def test_apply_grants_are_conditional_for_worker_and_backend_read_roles() -> None:
    conn = RecordingConnection()

    reconcile.apply_plan(conn, states=_old_live_states(), backend_read_roles=("finance_backend_read", "backend_read"))

    sql = conn.normalized_sql
    assert "if exists (select 1 from pg_roles where rolname = 'finance_data_ops_worker')" in sql
    assert "grant select, insert, update, delete on source_cache.etf_holdings, source_cache.etf_themes, source_cache.etf_theme_readiness to finance_data_ops_worker" in sql
    assert "foreach read_role in array array['finance_backend_read', 'backend_read'] loop" in sql
    assert "grant select on source_cache.etf_holdings, source_cache.etf_themes, source_cache.etf_theme_readiness to %i" in sql


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
    def __init__(self) -> None:
        self.statements: list[str] = []

    def cursor(self) -> "RecordingCursor":
        return RecordingCursor(self)

    @property
    def normalized_sql(self) -> str:
        return " ".join(" ".join(statement.lower().split()) for statement in self.statements)


class RecordingCursor:
    def __init__(self, conn: RecordingConnection) -> None:
        self.conn = conn

    def __enter__(self) -> "RecordingCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, statement: str, params: object | None = None) -> None:
        self.conn.statements.append(statement)


def _old_live_states() -> list[reconcile.ObjectState]:
    return [
        reconcile.ObjectState("source_cache", "etf_holdings", "v", 506),
        reconcile.ObjectState("source_cache", "etf_themes", "v", 32),
        reconcile.ObjectState("source_cache", "etf_theme_readiness", None, None),
        reconcile.ObjectState("public", "etf_holdings", "r", 506),
        reconcile.ObjectState("public", "etf_themes", "r", 32),
    ]
