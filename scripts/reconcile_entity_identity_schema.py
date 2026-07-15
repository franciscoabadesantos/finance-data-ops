#!/usr/bin/env python3
"""Reconcile Entity Layer side-by-side publication schema.

Dry-run is the default. `--apply` runs additive DDL only: schemas/tables,
missing columns, indexes, and conditional grants. It never publishes cache or
entity data.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.settings import load_settings

WORKER_ROLE = "finance_data_ops_worker"
DEFAULT_BACKEND_READ_ROLES = ("finance_backend_read", "finance_backend", "backend_read")


@dataclass(frozen=True, slots=True)
class ColumnRequirement:
    schema_name: str
    table_name: str
    column_name: str
    data_type: str
    is_nullable: str | None = None
    column_default_contains: str | None = None

    @property
    def qualified_table(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

    @property
    def qualified_column(self) -> str:
        return f"{self.qualified_table}.{self.column_name}"


@dataclass(frozen=True, slots=True)
class ColumnState:
    data_type: str
    is_nullable: str
    column_default: str


@dataclass(frozen=True, slots=True)
class CheckConstraintRequirement:
    schema_name: str
    table_name: str
    constraint_name: str
    column_name: str
    allowed_values: tuple[str, ...]
    nullable: bool = False

    @property
    def qualified_table(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

    @property
    def qualified_constraint(self) -> str:
        return f"{self.qualified_table}.{self.constraint_name}"

    @property
    def expression_sql(self) -> str:
        values = ", ".join(_sql_literal(value) for value in self.allowed_values)
        membership = f"{self.column_name} in ({values})"
        if self.nullable:
            return f"({self.column_name} is null or {membership})"
        return f"({membership})"


@dataclass(frozen=True, slots=True)
class SchemaState:
    tables: set[str]
    columns: dict[str, ColumnState]
    indexes: set[str]
    roles: set[str]
    grants: set[str] | None = None
    check_constraints: dict[str, str] = field(default_factory=dict)


CREATE_TABLE_SQL: dict[str, str] = {
    "source_cache.openfigi_mapping_raw": """
create table if not exists source_cache.openfigi_mapping_raw (
  request_hash text primary key,
  request_payload jsonb not null,
  response_payload jsonb,
  status text not null,
  error_message text,
  requested_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (status in ('success', 'not_found', 'ambiguous', 'error'))
)
""",
    "source_cache.gleif_entity_raw": """
create table if not exists source_cache.gleif_entity_raw (
  lei text primary key,
  response_payload jsonb not null,
  status text not null,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
)
""",
    "source_cache.listing_isin_raw": """
create table if not exists source_cache.listing_isin_raw (
  symbol text not null,
  provider text not null,
  request_payload jsonb not null,
  response_payload jsonb,
  isin text,
  status text not null,
  error_message text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (symbol, provider)
)
""",
    "source_cache.gleif_isin_lei_raw": """
create table if not exists source_cache.gleif_isin_lei_raw (
  isin text primary key,
  lei text,
  legal_name text,
  response_payload jsonb,
  status text not null,
  error_message text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
)
""",
    "source_cache.gleif_lei_isin_raw": """
create table if not exists source_cache.gleif_lei_isin_raw (
  lei text primary key,
  response_payload jsonb,
  isin_list text[] not null default array[]::text[],
  status text not null,
  error_message text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
)
""",
    "feature_store.entity_master": """
create table if not exists feature_store.entity_master (
  entity_id text primary key,
  legal_name text,
  display_name text,
  home_country text,
  lei text,
  entity_source text not null,
  resolution_confidence double precision not null default 0,
  resolution_status text not null,
  primary_listing_symbol text,
  primary_listing_reason text,
  publication_batch_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  check (resolution_status in ('resolved', 'provisional', 'needs_manual_review', 'conflict', 'rejected', 'superseded', 'ambiguous', 'unresolved', 'manual_review'))
)
""",
    "feature_store.entity_listing": """
create table if not exists feature_store.entity_listing (
  symbol text primary key,
  entity_id text not null references feature_store.entity_master(entity_id),
  provider_symbol text,
  exchange text,
  exchange_mic text,
  country text,
  currency text,
  figi text,
  composite_figi text,
  share_class_figi text,
  isin text,
  lei text,
  listing_type text,
  is_primary_listing boolean not null default false,
  primary_listing_reason text,
  resolution_source text not null,
  resolution_confidence double precision not null default 0,
  resolution_status text not null,
  attach_method text,
  attach_confidence text,
  review_state text,
  evidence_payload jsonb not null default '{}'::jsonb,
  source_freshness jsonb not null default '{}'::jsonb,
  publication_batch_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  check (resolution_status in ('resolved', 'provisional', 'needs_manual_review', 'conflict', 'rejected', 'superseded', 'ambiguous', 'unresolved', 'manual_review')),
  check (review_state is null or review_state in ('resolved', 'provisional', 'needs_manual_review', 'conflict', 'rejected', 'superseded', 'ambiguous', 'unresolved', 'manual_review')),
  check (attach_confidence is null or attach_confidence in ('high', 'medium', 'low', 'provisional'))
)
""",
    "feature_store.entity_identity_audit": """
create table if not exists feature_store.entity_identity_audit (
  audit_id bigserial primary key,
  symbol text,
  entity_id text,
  issue_type text not null,
  issue_severity text not null default 'warning',
  details jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
)
""",
    "feature_store.entity_identity_review": """
create table if not exists feature_store.entity_identity_review (
  review_id bigserial primary key,
  review_key text unique,
  symbol text,
  entity_id text,
  candidate_lei text,
  attach_method text,
  resolution_state text not null default 'needs_manual_review',
  review_reason text,
  evidence_summary jsonb not null default '{}'::jsonb,
  conflicting_candidates jsonb not null default '[]'::jsonb,
  reviewer_decision text,
  reviewer_identity text,
  reviewed_at timestamptz,
  override_provenance text,
  publication_batch_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (resolution_state in ('resolved', 'provisional', 'needs_manual_review', 'conflict', 'rejected', 'superseded', 'ambiguous', 'unresolved', 'manual_review'))
)
""",
    "feature_store.entity_identity_review_audit": """
create table if not exists feature_store.entity_identity_review_audit (
  audit_id bigserial primary key,
  review_id bigint references feature_store.entity_identity_review(review_id),
  symbol text,
  entity_id text,
  event_type text not null,
  event_payload jsonb not null default '{}'::jsonb,
  actor text,
  created_at timestamptz not null default now()
)
""",
    "feature_store.entity_identity_publication_batch": """
create table if not exists feature_store.entity_identity_publication_batch (
  batch_id text primary key,
  scope_key text not null default 'default',
  source text not null default 'entity_identity_measurement',
  mode text not null,
  status text not null,
  is_current boolean not null default false,
  publication_gate jsonb not null default '{}'::jsonb,
  summary jsonb not null default '{}'::jsonb,
  planned_counts jsonb not null default '{}'::jsonb,
  actual_counts jsonb not null default '{}'::jsonb,
  verification_summary jsonb not null default '{}'::jsonb,
  blocked_reasons jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (mode in ('dry_run', 'apply_cache', 'apply_entities', 'apply_cache_and_entities')),
  check (status in ('planned', 'published_side_by_side', 'blocked', 'failed', 'superseded'))
)
""",
    "feature_store.entity_identity_publication_current": """
create table if not exists feature_store.entity_identity_publication_current (
  scope_key text primary key,
  batch_id text not null references feature_store.entity_identity_publication_batch(batch_id),
  updated_at timestamptz not null default now()
)
""",
}


REQUIRED_TABLES = tuple(CREATE_TABLE_SQL)

REQUIRED_COLUMNS = (
    ColumnRequirement("feature_store", "entity_master", "publication_batch_id", "text"),
    ColumnRequirement("feature_store", "entity_listing", "attach_method", "text"),
    ColumnRequirement("feature_store", "entity_listing", "attach_confidence", "text"),
    ColumnRequirement("feature_store", "entity_listing", "review_state", "text"),
    ColumnRequirement(
        "feature_store",
        "entity_listing",
        "evidence_payload",
        "jsonb",
        is_nullable="NO",
        column_default_contains="'{}'::jsonb",
    ),
    ColumnRequirement(
        "feature_store",
        "entity_listing",
        "source_freshness",
        "jsonb",
        is_nullable="NO",
        column_default_contains="'{}'::jsonb",
    ),
    ColumnRequirement("feature_store", "entity_listing", "publication_batch_id", "text"),
)

LIFECYCLE_STATES = (
    "resolved",
    "provisional",
    "needs_manual_review",
    "conflict",
    "rejected",
    "superseded",
    "ambiguous",
    "unresolved",
    "manual_review",
)

REQUIRED_CHECK_CONSTRAINTS = (
    CheckConstraintRequirement(
        "feature_store",
        "entity_master",
        "entity_master_resolution_status_check",
        "resolution_status",
        LIFECYCLE_STATES,
    ),
    CheckConstraintRequirement(
        "feature_store",
        "entity_listing",
        "entity_listing_resolution_status_check",
        "resolution_status",
        LIFECYCLE_STATES,
    ),
    CheckConstraintRequirement(
        "feature_store",
        "entity_listing",
        "entity_listing_review_state_check",
        "review_state",
        LIFECYCLE_STATES,
        nullable=True,
    ),
    CheckConstraintRequirement(
        "feature_store",
        "entity_listing",
        "entity_listing_attach_confidence_check",
        "attach_confidence",
        ("high", "medium", "low", "provisional"),
        nullable=True,
    ),
    CheckConstraintRequirement(
        "feature_store",
        "entity_identity_review",
        "entity_identity_review_resolution_state_check",
        "resolution_state",
        LIFECYCLE_STATES,
    ),
    CheckConstraintRequirement(
        "feature_store",
        "entity_identity_publication_batch",
        "entity_identity_publication_batch_mode_check",
        "mode",
        ("dry_run", "apply_cache", "apply_entities", "apply_cache_and_entities"),
    ),
    CheckConstraintRequirement(
        "feature_store",
        "entity_identity_publication_batch",
        "entity_identity_publication_batch_status_check",
        "status",
        ("planned", "published_side_by_side", "blocked", "failed", "superseded"),
    ),
)

ADD_COLUMN_SQL = {
    "feature_store.entity_master.publication_batch_id": (
        "alter table feature_store.entity_master add column if not exists publication_batch_id text"
    ),
    "feature_store.entity_listing.attach_method": (
        "alter table feature_store.entity_listing add column if not exists attach_method text"
    ),
    "feature_store.entity_listing.attach_confidence": (
        "alter table feature_store.entity_listing add column if not exists attach_confidence text"
    ),
    "feature_store.entity_listing.review_state": (
        "alter table feature_store.entity_listing add column if not exists review_state text"
    ),
    "feature_store.entity_listing.evidence_payload": (
        "alter table feature_store.entity_listing add column if not exists evidence_payload jsonb not null default '{}'::jsonb"
    ),
    "feature_store.entity_listing.source_freshness": (
        "alter table feature_store.entity_listing add column if not exists source_freshness jsonb not null default '{}'::jsonb"
    ),
    "feature_store.entity_listing.publication_batch_id": (
        "alter table feature_store.entity_listing add column if not exists publication_batch_id text"
    ),
}

INDEX_SQL: dict[str, str] = {
    "idx_listing_isin_raw_isin": "create index if not exists idx_listing_isin_raw_isin on source_cache.listing_isin_raw (isin)",
    "idx_gleif_isin_lei_raw_lei": "create index if not exists idx_gleif_isin_lei_raw_lei on source_cache.gleif_isin_lei_raw (lei)",
    "idx_entity_master_home_country": "create index if not exists idx_entity_master_home_country on feature_store.entity_master (home_country)",
    "idx_entity_master_publication_batch_id": "create index if not exists idx_entity_master_publication_batch_id on feature_store.entity_master (publication_batch_id)",
    "idx_entity_listing_entity_id": "create index if not exists idx_entity_listing_entity_id on feature_store.entity_listing (entity_id)",
    "idx_entity_listing_isin": "create index if not exists idx_entity_listing_isin on feature_store.entity_listing (isin)",
    "idx_entity_listing_figi": "create index if not exists idx_entity_listing_figi on feature_store.entity_listing (figi)",
    "idx_entity_listing_composite_figi": "create index if not exists idx_entity_listing_composite_figi on feature_store.entity_listing (composite_figi)",
    "idx_entity_listing_share_class_figi": "create index if not exists idx_entity_listing_share_class_figi on feature_store.entity_listing (share_class_figi)",
    "idx_entity_listing_exchange_mic": "create index if not exists idx_entity_listing_exchange_mic on feature_store.entity_listing (exchange_mic)",
    "idx_entity_listing_publication_batch_id": "create index if not exists idx_entity_listing_publication_batch_id on feature_store.entity_listing (publication_batch_id)",
    "idx_entity_identity_audit_symbol_issue_type": "create index if not exists idx_entity_identity_audit_symbol_issue_type on feature_store.entity_identity_audit (symbol, issue_type)",
    "idx_entity_identity_review_symbol_state": "create index if not exists idx_entity_identity_review_symbol_state on feature_store.entity_identity_review (symbol, resolution_state)",
    "idx_entity_identity_review_entity_id": "create index if not exists idx_entity_identity_review_entity_id on feature_store.entity_identity_review (entity_id)",
    "idx_entity_identity_review_publication_batch_id": "create index if not exists idx_entity_identity_review_publication_batch_id on feature_store.entity_identity_review (publication_batch_id)",
    "idx_entity_identity_review_audit_review_id": "create index if not exists idx_entity_identity_review_audit_review_id on feature_store.entity_identity_review_audit (review_id)",
    "idx_entity_identity_publication_batch_scope_current": "create index if not exists idx_entity_identity_publication_batch_scope_current on feature_store.entity_identity_publication_batch (scope_key, is_current)",
}

GRANT_SQL_TEMPLATE = """
do $$
declare
  read_role text;
begin
  if exists (select 1 from pg_roles where rolname = 'finance_data_ops_worker') then
    grant usage on schema source_cache to finance_data_ops_worker;
    grant usage on schema feature_store to finance_data_ops_worker;
    grant select, insert, update, delete
      on source_cache.openfigi_mapping_raw,
         source_cache.gleif_entity_raw,
         source_cache.listing_isin_raw,
         source_cache.gleif_isin_lei_raw,
         source_cache.gleif_lei_isin_raw
      to finance_data_ops_worker;
    grant select, insert, update, delete
      on feature_store.entity_master,
         feature_store.entity_listing,
         feature_store.entity_identity_audit,
         feature_store.entity_identity_review,
         feature_store.entity_identity_review_audit,
         feature_store.entity_identity_publication_batch,
         feature_store.entity_identity_publication_current
      to finance_data_ops_worker;
    grant usage, select
      on sequence feature_store.entity_identity_audit_audit_id_seq,
                  feature_store.entity_identity_review_review_id_seq,
                  feature_store.entity_identity_review_audit_audit_id_seq
      to finance_data_ops_worker;
  end if;

  foreach read_role in array {read_roles} loop
    if exists (select 1 from pg_roles where rolname = read_role) then
      execute format('grant usage on schema source_cache to %I', read_role);
      execute format('grant usage on schema feature_store to %I', read_role);
      execute format(
        'grant select on source_cache.openfigi_mapping_raw, source_cache.gleif_entity_raw, source_cache.listing_isin_raw, source_cache.gleif_isin_lei_raw, source_cache.gleif_lei_isin_raw to %I',
        read_role
      );
      execute format(
        'grant select on feature_store.entity_master, feature_store.entity_listing, feature_store.entity_identity_audit, feature_store.entity_identity_review, feature_store.entity_identity_review_audit, feature_store.entity_identity_publication_batch, feature_store.entity_identity_publication_current to %I',
        read_role
      );
    end if;
  end loop;
end $$;
"""


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    if args.apply:
        settings.require_database()
        result = reconcile_live_schema(
            database_dsn=settings.database_dsn,
            apply=True,
            backend_read_roles=tuple(args.backend_read_role),
        )
    else:
        state = _empty_state() if args.assume_empty else None
        if state is None:
            settings.require_database()
            result = reconcile_live_schema(
                database_dsn=settings.database_dsn,
                apply=False,
                backend_read_roles=tuple(args.backend_read_role),
            )
        else:
            plan = build_reconciliation_plan(state, backend_read_roles=tuple(args.backend_read_role))
            result = {
                "mode": "dry_run",
                "apply": False,
                "planned_actions": plan["actions"],
                "missing_before": verification_summary(verify_schema(state)),
                "missing_after": verification_summary(verify_schema(apply_plan_to_state(state))),
                "blockers": [],
            }
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    if result.get("blockers"):
        raise SystemExit(2)


def reconcile_live_schema(
    *,
    database_dsn: str,
    apply: bool,
    backend_read_roles: tuple[str, ...] = DEFAULT_BACKEND_READ_ROLES,
) -> dict[str, Any]:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for schema reconciliation.") from exc
    with psycopg.connect(database_dsn, autocommit=False) as conn:
        before = inspect_schema(conn)
        plan = build_reconciliation_plan(before, backend_read_roles=backend_read_roles)
        result: dict[str, Any] = {
            "mode": "apply" if apply else "dry_run",
            "apply": bool(apply),
            "planned_actions": plan["actions"],
            "missing_before": verification_summary(verify_schema(before)),
            "blockers": [],
        }
        if not apply:
            result["apply_status"] = "dry_run"
            result["schema_verification"] = verify_schema(before)
            result["missing_after"] = result["missing_before"]
            conn.rollback()
            return result
        _require_apply_admin_role(conn)
        try:
            for statement in plan["sql"]:
                _execute(conn, statement)
            after = inspect_schema(conn)
            verification = verify_schema(after)
            if not verification["ok"]:
                raise RuntimeError(f"post_apply_schema_verification_failed: {verification}")
            conn.commit()
            result["apply_status"] = "applied"
            result["schema_verification"] = verification
            result["missing_after"] = verification_summary(verification)
        except Exception as exc:
            conn.rollback()
            result["apply_status"] = "rolled_back"
            result["blockers"] = [str(exc)]
        return result


def build_reconciliation_plan(
    state: SchemaState,
    *,
    backend_read_roles: tuple[str, ...] = DEFAULT_BACKEND_READ_ROLES,
) -> dict[str, Any]:
    actions: list[dict[str, Any]] = []
    sql: list[str] = [
        "create schema if not exists source_cache",
        "create schema if not exists feature_store",
    ]
    actions.append({"action": "ensure_schema", "schema": "source_cache"})
    actions.append({"action": "ensure_schema", "schema": "feature_store"})

    for table in REQUIRED_TABLES:
        if table not in state.tables:
            actions.append({"action": "create_missing_table", "object": table})
            sql.append(CREATE_TABLE_SQL[table])

    for requirement in REQUIRED_COLUMNS:
        key = requirement.qualified_column
        if requirement.qualified_table in state.tables and key not in state.columns:
            actions.append({"action": "add_missing_column", "object": key})
            sql.append(ADD_COLUMN_SQL[key])

    for requirement in REQUIRED_CHECK_CONSTRAINTS:
        if requirement.qualified_table not in state.tables:
            continue
        current_definition = state.check_constraints.get(requirement.qualified_constraint)
        if current_definition and _check_constraint_is_compatible(requirement, current_definition):
            continue
        reason = "missing" if not current_definition else "incompatible"
        actions.append(
            {
                "action": "reconcile_check_constraint",
                "object": requirement.qualified_constraint,
                "table": requirement.qualified_table,
                "column": requirement.column_name,
                "reason": reason,
            }
        )
        sql.extend(_reconcile_check_constraint_sql(requirement))

    for index_name, statement in INDEX_SQL.items():
        if index_name not in state.indexes:
            actions.append({"action": "create_missing_index", "object": index_name})
            sql.append(statement)

    grants_sql = _grant_sql(backend_read_roles)
    actions.append(
        {
            "action": "apply_conditional_grants",
            "worker_role": WORKER_ROLE,
            "backend_read_roles": [role for role in backend_read_roles if role in state.roles] or list(backend_read_roles),
            "conditional": True,
        }
    )
    sql.append(grants_sql)
    _assert_no_forbidden_sql(sql)
    return {"actions": actions, "sql": sql}


def verify_schema(state: SchemaState) -> dict[str, Any]:
    missing_tables = [table for table in REQUIRED_TABLES if table not in state.tables]
    missing_columns = [
        requirement.qualified_column
        for requirement in REQUIRED_COLUMNS
        if requirement.qualified_table not in missing_tables and requirement.qualified_column not in state.columns
    ]
    mismatched_columns = []
    for requirement in REQUIRED_COLUMNS:
        current = state.columns.get(requirement.qualified_column)
        if not current:
            continue
        if _normalize_type(current.data_type) != _normalize_type(requirement.data_type):
            mismatched_columns.append(
                {
                    "column": requirement.qualified_column,
                    "expected_type": requirement.data_type,
                    "actual_type": current.data_type,
                }
            )
        if requirement.is_nullable and current.is_nullable.upper() != requirement.is_nullable.upper():
            mismatched_columns.append(
                {
                    "column": requirement.qualified_column,
                    "expected_nullable": requirement.is_nullable,
                    "actual_nullable": current.is_nullable,
                }
            )
        if requirement.column_default_contains and requirement.column_default_contains not in current.column_default:
            mismatched_columns.append(
                {
                    "column": requirement.qualified_column,
                    "expected_default_contains": requirement.column_default_contains,
                    "actual_default": current.column_default,
                }
            )
    missing_indexes = [index_name for index_name in INDEX_SQL if index_name not in state.indexes]
    missing_grants = _missing_grants(state)
    missing_check_constraints = []
    mismatched_check_constraints = []
    for requirement in REQUIRED_CHECK_CONSTRAINTS:
        if requirement.qualified_table in missing_tables or requirement.qualified_table not in state.tables:
            continue
        current_definition = state.check_constraints.get(requirement.qualified_constraint)
        if not current_definition:
            missing_check_constraints.append(requirement.qualified_constraint)
            continue
        if not _check_constraint_is_compatible(requirement, current_definition):
            mismatched_check_constraints.append(
                {
                    "constraint": requirement.qualified_constraint,
                    "expected_allowed_values": list(requirement.allowed_values),
                    "actual_definition": current_definition,
                }
            )
    return {
        "ok": not (
            missing_tables
            or missing_columns
            or mismatched_columns
            or missing_indexes
            or missing_grants
            or missing_check_constraints
            or mismatched_check_constraints
        ),
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "mismatched_columns": mismatched_columns,
        "missing_indexes": missing_indexes,
        "missing_grants": missing_grants,
        "missing_check_constraints": missing_check_constraints,
        "mismatched_check_constraints": mismatched_check_constraints,
    }


def verification_summary(verification: dict[str, Any]) -> dict[str, Any]:
    return {
        "missing_tables": list(verification.get("missing_tables") or []),
        "missing_columns": list(verification.get("missing_columns") or []),
        "mismatched_columns": list(verification.get("mismatched_columns") or []),
        "missing_indexes": list(verification.get("missing_indexes") or []),
        "missing_grants": list(verification.get("missing_grants") or []),
        "missing_check_constraints": list(verification.get("missing_check_constraints") or []),
        "mismatched_check_constraints": list(verification.get("mismatched_check_constraints") or []),
    }


def apply_plan_to_state(state: SchemaState) -> SchemaState:
    tables = set(state.tables)
    columns = dict(state.columns)
    indexes = set(state.indexes)
    check_constraints = dict(state.check_constraints)
    tables.update(REQUIRED_TABLES)
    for requirement in REQUIRED_COLUMNS:
        columns.setdefault(
            requirement.qualified_column,
            ColumnState(
                data_type=requirement.data_type,
                is_nullable=requirement.is_nullable or "YES",
                column_default=requirement.column_default_contains or "",
            ),
        )
    indexes.update(INDEX_SQL)
    for requirement in REQUIRED_CHECK_CONSTRAINTS:
        check_constraints[requirement.qualified_constraint] = _expected_check_constraint_definition(requirement)
    return SchemaState(
        tables=tables,
        columns=columns,
        indexes=indexes,
        roles=set(state.roles),
        grants=set(state.grants or set()),
        check_constraints=check_constraints,
    )


def inspect_schema(conn: Any) -> SchemaState:
    tables = _fetch_tables(conn)
    columns = _fetch_columns(conn)
    indexes = _fetch_indexes(conn)
    roles = _fetch_roles(conn)
    grants = _fetch_grants(conn)
    check_constraints = _fetch_check_constraints(conn)
    return SchemaState(
        tables=tables,
        columns=columns,
        indexes=indexes,
        roles=roles,
        grants=grants,
        check_constraints=check_constraints,
    )


def _fetch_tables(conn: Any) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select table_schema, table_name
            from information_schema.tables
            where table_schema in ('source_cache', 'feature_store')
            """
        )
        return {f"{row[0]}.{row[1]}" for row in cur.fetchall()}


def _fetch_columns(conn: Any) -> dict[str, ColumnState]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select table_schema, table_name, column_name, data_type, is_nullable, coalesce(column_default, '')
            from information_schema.columns
            where table_schema in ('source_cache', 'feature_store')
            """
        )
        return {
            f"{row[0]}.{row[1]}.{row[2]}": ColumnState(
                data_type=str(row[3]),
                is_nullable=str(row[4]),
                column_default=str(row[5] or ""),
            )
            for row in cur.fetchall()
        }


def _fetch_indexes(conn: Any) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select indexname
            from pg_indexes
            where schemaname in ('source_cache', 'feature_store')
            """
        )
        return {str(row[0]) for row in cur.fetchall()}


def _fetch_check_constraints(conn: Any) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select n.nspname, c.relname, con.conname, pg_get_constraintdef(con.oid, true)
            from pg_constraint con
            join pg_class c on c.oid = con.conrelid
            join pg_namespace n on n.oid = c.relnamespace
            where con.contype = 'c'
              and n.nspname in ('source_cache', 'feature_store')
            """
        )
        return {f"{row[0]}.{row[1]}.{row[2]}": str(row[3] or "") for row in cur.fetchall()}


def _fetch_roles(conn: Any) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("select rolname from pg_roles")
        return {str(row[0]) for row in cur.fetchall()}


def _fetch_grants(conn: Any) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select grantee || ':' || table_schema || '.' || table_name || ':' || privilege_type
            from information_schema.role_table_grants
            where table_schema in ('source_cache', 'feature_store')
            """
        )
        return {str(row[0]) for row in cur.fetchall()}


def _missing_grants(state: SchemaState) -> list[str]:
    if state.grants is None:
        return []
    expected = []
    if WORKER_ROLE in state.roles:
        for table in (
            "source_cache.listing_isin_raw",
            "source_cache.gleif_isin_lei_raw",
            "source_cache.gleif_lei_isin_raw",
            "feature_store.entity_master",
            "feature_store.entity_listing",
            "feature_store.entity_identity_review",
            "feature_store.entity_identity_publication_batch",
            "feature_store.entity_identity_publication_current",
        ):
            expected.append(f"{WORKER_ROLE}:{table}:INSERT")
            expected.append(f"{WORKER_ROLE}:{table}:UPDATE")
    for role in DEFAULT_BACKEND_READ_ROLES:
        if role in state.roles:
            for table in ("feature_store.entity_master", "feature_store.entity_listing"):
                expected.append(f"{role}:{table}:SELECT")
    return [grant for grant in expected if grant not in state.grants]


def _require_apply_admin_role(conn: Any) -> None:
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                select
                  current_user,
                  session_user,
                  case
                    when to_regnamespace('source_cache') is null
                      then has_database_privilege(current_user, current_database(), 'CREATE')
                    else has_schema_privilege(current_user, 'source_cache', 'CREATE')
                  end as source_cache_ddl_capable,
                  case
                    when to_regnamespace('feature_store') is null
                      then has_database_privilege(current_user, current_database(), 'CREATE')
                    else has_schema_privilege(current_user, 'feature_store', 'CREATE')
                  end as feature_store_ddl_capable
                """
            )
            row = cur.fetchone()
        except Exception as exc:
            raise RuntimeError(f"apply_requires_admin_role: failed to inspect current database role: {exc}") from exc
    current_user = str(row[0] if row else "").strip()
    session_user = str(row[1] if row else "").strip()
    source_cache_ok = bool(row[2]) if row and len(row) > 2 else False
    feature_store_ok = bool(row[3]) if row and len(row) > 3 else False
    users = {current_user.lower(), session_user.lower()}
    if WORKER_ROLE.lower() in users:
        raise RuntimeError(
            f"apply_requires_admin_role: {WORKER_ROLE} cannot run Entity Layer schema reconciliation --apply"
        )
    if not source_cache_ok or not feature_store_ok:
        raise RuntimeError(
            f"apply_requires_admin_role: current role {current_user or '<unknown>'} lacks source_cache/feature_store DDL privilege"
        )


def _execute(conn: Any, statement: str) -> None:
    with conn.cursor() as cur:
        cur.execute(statement)


def _grant_sql(backend_read_roles: tuple[str, ...]) -> str:
    role_literal = "array[" + ", ".join(_sql_literal(role) for role in backend_read_roles) + "]"
    return GRANT_SQL_TEMPLATE.format(read_roles=role_literal)


def _assert_no_forbidden_sql(statements: list[str]) -> None:
    for statement in statements:
        normalized = " ".join(statement.lower().split())
        if " cascade" in normalized:
            raise ValueError(f"forbidden_ddl_in_entity_schema_reconciliation: {normalized[:160]}")
        if re.search(r"\bdrop\b", normalized) and not _is_allowed_check_constraint_drop(normalized):
            raise ValueError(f"forbidden_ddl_in_entity_schema_reconciliation: {normalized[:160]}")


def _normalize_type(value: str) -> str:
    text = str(value).strip().lower()
    return {
        "timestamp with time zone": "timestamptz",
        "double precision": "double precision",
        "ARRAY": "array",
    }.get(text, text)


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _reconcile_check_constraint_sql(requirement: CheckConstraintRequirement) -> list[str]:
    return [
        (
            f"alter table {requirement.qualified_table} "
            f"drop constraint if exists {requirement.constraint_name}"
        ),
        (
            f"alter table {requirement.qualified_table} "
            f"add constraint {requirement.constraint_name} check {requirement.expression_sql}"
        ),
    ]


def _is_allowed_check_constraint_drop(normalized_statement: str) -> bool:
    return any(
        normalized_statement
        == f"alter table {requirement.qualified_table} drop constraint if exists {requirement.constraint_name}".lower()
        for requirement in REQUIRED_CHECK_CONSTRAINTS
    )


def _expected_check_constraint_definition(requirement: CheckConstraintRequirement) -> str:
    return f"CHECK {requirement.expression_sql}"


def _check_constraint_is_compatible(requirement: CheckConstraintRequirement, definition: str) -> bool:
    normalized = " ".join(str(definition or "").lower().replace("::text", "").split())
    if requirement.column_name.lower() not in normalized:
        return False
    if requirement.nullable and "is null" not in normalized:
        return False
    return all(_sql_literal(value).lower() in normalized for value in requirement.allowed_values)


def _empty_state() -> SchemaState:
    return SchemaState(tables=set(), columns={}, indexes=set(), roles=set(), grants=set())


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile Entity Layer side-by-side publication schema.")
    parser.add_argument("--apply", action="store_true", help="Apply additive DDL. Dry-run by default.")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--assume-empty", action="store_true", help="Local dry-run helper that plans from an empty schema state.")
    parser.add_argument(
        "--backend-read-role",
        action="append",
        default=list(DEFAULT_BACKEND_READ_ROLES),
        help="Conditional backend/read role to grant SELECT. May be repeated.",
    )
    return parser


if __name__ == "__main__":
    main()
