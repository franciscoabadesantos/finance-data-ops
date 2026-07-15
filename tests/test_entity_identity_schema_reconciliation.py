from __future__ import annotations

import pytest

from scripts import reconcile_entity_identity_schema as reconcile


def test_dry_run_plans_missing_entity_identity_tables() -> None:
    state = reconcile.SchemaState(
        tables={"source_cache.openfigi_mapping_raw", "feature_store.entity_master", "feature_store.entity_listing"},
        columns={},
        indexes=set(),
        roles=set(),
        grants=set(),
    )

    plan = reconcile.build_reconciliation_plan(state)
    create_objects = {action["object"] for action in plan["actions"] if action["action"] == "create_missing_table"}

    assert "source_cache.listing_isin_raw" in create_objects
    assert "source_cache.gleif_isin_lei_raw" in create_objects
    assert "source_cache.gleif_lei_isin_raw" in create_objects
    assert "feature_store.entity_identity_review" in create_objects
    assert "feature_store.entity_identity_publication_batch" in create_objects
    assert "feature_store.entity_identity_publication_current" in create_objects


def test_dry_run_plans_missing_columns_on_existing_entity_tables() -> None:
    state = reconcile.SchemaState(
        tables={"feature_store.entity_master", "feature_store.entity_listing"},
        columns={
            "feature_store.entity_master.entity_id": reconcile.ColumnState("text", "NO", ""),
            "feature_store.entity_listing.symbol": reconcile.ColumnState("text", "NO", ""),
        },
        indexes=set(),
        roles=set(),
        grants=set(),
    )

    plan = reconcile.build_reconciliation_plan(state)
    add_columns = {action["object"] for action in plan["actions"] if action["action"] == "add_missing_column"}

    assert "feature_store.entity_master.publication_batch_id" in add_columns
    assert "feature_store.entity_listing.attach_method" in add_columns
    assert "feature_store.entity_listing.attach_confidence" in add_columns
    assert "feature_store.entity_listing.review_state" in add_columns
    assert "feature_store.entity_listing.evidence_payload" in add_columns
    assert "feature_store.entity_listing.source_freshness" in add_columns
    assert "feature_store.entity_listing.publication_batch_id" in add_columns


def test_plan_uses_add_column_if_not_exists_and_no_drop_cascade() -> None:
    state = reconcile.SchemaState(
        tables={"feature_store.entity_master", "feature_store.entity_listing"},
        columns={},
        indexes=set(),
        roles=set(),
        grants=set(),
    )

    plan = reconcile.build_reconciliation_plan(state)
    sql = "\n".join(plan["sql"]).lower()

    assert "alter table feature_store.entity_master add column if not exists publication_batch_id text" in sql
    assert "alter table feature_store.entity_listing add column if not exists attach_method text" in sql
    assert "alter table feature_store.entity_listing add column if not exists evidence_payload jsonb not null default '{}'::jsonb" in sql
    assert "drop " not in sql
    assert " cascade" not in sql


def test_admin_role_check_blocks_worker_or_non_ddl_role() -> None:
    with pytest.raises(RuntimeError, match="apply_requires_admin_role"):
        reconcile._require_apply_admin_role(_FakeConn(("finance_data_ops_worker", "finance_data_ops_worker", True, True)))

    with pytest.raises(RuntimeError, match="lacks source_cache/feature_store DDL privilege"):
        reconcile._require_apply_admin_role(_FakeConn(("readonly", "readonly", True, False)))


def test_admin_role_check_allows_non_worker_ddl_role() -> None:
    reconcile._require_apply_admin_role(_FakeConn(("postgres", "postgres", True, True)))


def test_post_apply_verification_catches_missing_columns() -> None:
    state = reconcile.apply_plan_to_state(reconcile.SchemaState(tables=set(), columns={}, indexes=set(), roles=set(), grants=set()))
    columns = dict(state.columns)
    columns.pop("feature_store.entity_listing.evidence_payload")
    broken = reconcile.SchemaState(
        tables=set(state.tables),
        columns=columns,
        indexes=set(state.indexes),
        roles=set(state.roles),
        grants=set(state.grants or set()),
    )

    verification = reconcile.verify_schema(broken)

    assert verification["ok"] is False
    assert "feature_store.entity_listing.evidence_payload" in verification["missing_columns"]


def test_grants_and_indexes_are_planned() -> None:
    state = reconcile.SchemaState(
        tables=set(reconcile.REQUIRED_TABLES),
        columns={},
        indexes=set(),
        roles={"finance_data_ops_worker", "finance_backend_read"},
        grants=set(),
    )

    plan = reconcile.build_reconciliation_plan(state)
    actions = [action["action"] for action in plan["actions"]]
    sql = "\n".join(plan["sql"])

    assert "create_missing_index" in actions
    assert "apply_conditional_grants" in actions
    assert "idx_entity_listing_publication_batch_id" in sql
    assert "grant select, insert, update, delete" in sql
    assert "feature_store.entity_identity_publication_batch" in sql


def test_idempotent_second_run_has_no_table_column_or_index_actions() -> None:
    state = reconcile.apply_plan_to_state(reconcile.SchemaState(tables=set(), columns={}, indexes=set(), roles=set(), grants=set()))

    plan = reconcile.build_reconciliation_plan(state)
    action_types = {action["action"] for action in plan["actions"]}
    verification = reconcile.verify_schema(state)

    assert "create_missing_table" not in action_types
    assert "add_missing_column" not in action_types
    assert "create_missing_index" not in action_types
    assert verification["ok"] is True


class _FakeConn:
    def __init__(self, row):
        self.row = row

    def cursor(self):
        return _FakeCursor(self.row)


class _FakeCursor:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *_args, **_kwargs):
        return None

    def fetchone(self):
        return self.row
