#!/usr/bin/env python3
"""One-shot reconciliation for canonical ETF source-cache tables.

Dry-run is the default. Use --apply to replace old source_cache ETF views with
writable canonical tables and copy the old public ETF data into them.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from typing import Any


ETF_OBJECTS = [
    ("source_cache", "etf_holdings"),
    ("source_cache", "etf_themes"),
    ("source_cache", "etf_theme_readiness"),
    ("public", "etf_holdings"),
    ("public", "etf_themes"),
]

WORKER_ROLE = "finance_data_ops_worker"
DEFAULT_BACKEND_READ_ROLES = ("finance_backend_read", "finance_backend", "backend_read")


@dataclass(frozen=True)
class ObjectState:
    schema_name: str
    table_name: str
    relkind: str | None
    rows: int | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

    @property
    def kind(self) -> str:
        return RELKIND_LABELS.get(str(self.relkind or ""), "missing")


RELKIND_LABELS = {
    "r": "table",
    "p": "partitioned_table",
    "v": "view",
    "m": "materialized_view",
    "f": "foreign_table",
}


CREATE_ETF_HOLDINGS_TABLE_SQL = """
create table if not exists source_cache.etf_holdings (
  etf_ticker text not null,
  holding_symbol text not null,
  holding_name text,
  holding_country text,
  weight double precision,
  as_of date not null,
  source text,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (etf_ticker, holding_symbol, as_of)
)
"""

CREATE_ETF_HOLDINGS_INDEX_SQL = """
create index if not exists idx_etf_holdings_ticker_weight
  on source_cache.etf_holdings (etf_ticker, as_of desc, weight desc)
"""

CREATE_ETF_THEMES_TABLE_SQL = """
create table if not exists source_cache.etf_themes (
  etf_ticker text primary key,
  theme text not null,
  wave integer not null check (wave in (1, 2)),
  issuer text,
  source_type text not null,
  source_ref text,
  holdings_count integer,
  holdings_as_of date,
  holdings_source_depth text not null default 'unknown',
  holdings_shallow boolean not null default false,
  active boolean not null default true,
  fetched_at timestamptz,
  updated_at timestamptz not null default now()
)
"""

CREATE_ETF_THEMES_INDEX_SQL = """
create index if not exists idx_etf_themes_theme_wave
  on source_cache.etf_themes (theme, wave, active)
"""

CREATE_ETF_THEME_READINESS_TABLE_SQL = """
create table if not exists source_cache.etf_theme_readiness (
  etf_symbol text primary key,
  theme text,
  active boolean not null default true,
  holdings_count integer not null default 0,
  holdings_as_of date,
  holdings_shallow boolean not null default false,
  priced_constituent_count integer not null default 0,
  technical_constituent_count integer not null default 0,
  tracked_constituent_count integer not null default 0,
  coverage_ratio double precision not null default 0,
  relationship_map_eligible boolean not null default false,
  relationship_map_ineligible_reason text,
  computed_at timestamptz not null default now()
)
"""

CREATE_ETF_THEME_READINESS_INDEX_SQL = """
create index if not exists idx_etf_theme_readiness_eligibility
  on source_cache.etf_theme_readiness (relationship_map_eligible, active, computed_at desc)
"""

COPY_PUBLIC_HOLDINGS_SQL = """
insert into source_cache.etf_holdings (
  etf_ticker,
  holding_symbol,
  holding_name,
  holding_country,
  weight,
  as_of,
  source,
  fetched_at,
  updated_at
)
select
  etf_ticker,
  holding_symbol,
  holding_name,
  holding_country,
  weight,
  as_of,
  source,
  fetched_at,
  updated_at
from public.etf_holdings
on conflict (etf_ticker, holding_symbol, as_of) do update set
  holding_name = excluded.holding_name,
  holding_country = excluded.holding_country,
  weight = excluded.weight,
  source = excluded.source,
  fetched_at = excluded.fetched_at,
  updated_at = excluded.updated_at
"""

COPY_PUBLIC_THEMES_SQL = """
insert into source_cache.etf_themes (
  etf_ticker,
  theme,
  wave,
  issuer,
  source_type,
  source_ref,
  holdings_count,
  holdings_as_of,
  holdings_source_depth,
  holdings_shallow,
  active,
  fetched_at,
  updated_at
)
select
  etf_ticker,
  theme,
  wave,
  issuer,
  source_type,
  source_ref,
  holdings_count,
  holdings_as_of,
  holdings_source_depth,
  holdings_shallow,
  active,
  fetched_at,
  updated_at
from public.etf_themes
on conflict (etf_ticker) do update set
  theme = excluded.theme,
  wave = excluded.wave,
  issuer = excluded.issuer,
  source_type = excluded.source_type,
  source_ref = excluded.source_ref,
  holdings_count = excluded.holdings_count,
  holdings_as_of = excluded.holdings_as_of,
  holdings_source_depth = excluded.holdings_source_depth,
  holdings_shallow = excluded.holdings_shallow,
  active = excluded.active,
  fetched_at = excluded.fetched_at,
  updated_at = excluded.updated_at
"""

REFRESH_READINESS_SQL = """
truncate table source_cache.etf_theme_readiness;

with latest_holding_dates as (
  select upper(etf_ticker) as etf_ticker, max(as_of) as as_of
  from source_cache.etf_holdings
  group by upper(etf_ticker)
),
latest_holdings as (
  select
    upper(h.etf_ticker) as etf_ticker,
    upper(h.holding_symbol) as holding_symbol,
    h.as_of
  from source_cache.etf_holdings h
  join latest_holding_dates latest
    on upper(h.etf_ticker) = latest.etf_ticker
   and h.as_of = latest.as_of
  where h.holding_symbol is not null
),
holding_counts as (
  select
    etf_ticker,
    count(distinct holding_symbol)::int as holdings_count,
    max(as_of) as holdings_as_of
  from latest_holdings
  group by etf_ticker
),
priced_counts as (
  select h.etf_ticker, count(distinct h.holding_symbol)::int as priced_constituent_count
  from latest_holdings h
  where exists (
    select 1
    from source_cache.market_price_daily p
    where upper(p.symbol) = h.holding_symbol
  )
  group by h.etf_ticker
),
technical_counts as (
  select h.etf_ticker, count(distinct h.holding_symbol)::int as technical_constituent_count
  from latest_holdings h
  where exists (
    select 1
    from feature_store.technical_features_daily t
    where upper(t.symbol) = h.holding_symbol
  )
  group by h.etf_ticker
),
readiness as (
  select
    upper(t.etf_ticker) as etf_symbol,
    t.theme,
    coalesce(t.active, true) as active,
    coalesce(h.holdings_count, t.holdings_count, 0)::int as holdings_count,
    coalesce(h.holdings_as_of, t.holdings_as_of) as holdings_as_of,
    coalesce(t.holdings_shallow, false) as holdings_shallow,
    coalesce(p.priced_constituent_count, 0)::int as priced_constituent_count,
    coalesce(tc.technical_constituent_count, 0)::int as technical_constituent_count,
    coalesce(h.holdings_count, 0)::int as tracked_constituent_count
  from source_cache.etf_themes t
  left join holding_counts h on h.etf_ticker = upper(t.etf_ticker)
  left join priced_counts p on p.etf_ticker = upper(t.etf_ticker)
  left join technical_counts tc on tc.etf_ticker = upper(t.etf_ticker)
)
insert into source_cache.etf_theme_readiness (
  etf_symbol,
  theme,
  active,
  holdings_count,
  holdings_as_of,
  holdings_shallow,
  priced_constituent_count,
  technical_constituent_count,
  tracked_constituent_count,
  coverage_ratio,
  relationship_map_eligible,
  relationship_map_ineligible_reason,
  computed_at
)
select
  etf_symbol,
  theme,
  active,
  holdings_count,
  holdings_as_of,
  holdings_shallow,
  priced_constituent_count,
  technical_constituent_count,
  tracked_constituent_count,
  case when holdings_count > 0 then technical_constituent_count::double precision / holdings_count else 0 end,
  active
    and holdings_count > 0
    and technical_constituent_count >= 5
    and (technical_constituent_count::double precision / nullif(holdings_count, 0)) >= 0.10,
  case
    when not active then 'inactive'
    when holdings_count <= 0 then 'missing_holdings'
    when technical_constituent_count < 5 then 'insufficient_constituent_coverage'
    when (technical_constituent_count::double precision / nullif(holdings_count, 0)) < 0.10
      then 'insufficient_constituent_coverage'
    else null
  end,
  now()
from readiness
"""

GRANT_SQL_TEMPLATE = """
do $$
declare
  read_role text;
begin
  if exists (select 1 from pg_roles where rolname = 'finance_data_ops_worker') then
    grant usage on schema source_cache to finance_data_ops_worker;
    grant select, insert, update, delete
      on source_cache.etf_holdings,
         source_cache.etf_themes,
         source_cache.etf_theme_readiness
      to finance_data_ops_worker;
  end if;

  foreach read_role in array {read_roles} loop
    if exists (select 1 from pg_roles where rolname = read_role) then
      execute format('grant usage on schema source_cache to %I', read_role);
      execute format(
        'grant select on source_cache.etf_holdings, source_cache.etf_themes, source_cache.etf_theme_readiness to %I',
        read_role
      );
    end if;
  end loop;
end $$;
"""

DROP_OLD_PUBLIC_SQL = [
    "drop table if exists public.etf_holdings",
    "drop table if exists public.etf_themes",
]


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    database_dsn = args.database_dsn or os.environ.get("DATA_OPS_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not database_dsn:
        raise RuntimeError("DATA_OPS_DATABASE_URL or DATABASE_URL must be set.")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for live ETF schema reconciliation.") from exc

    read_roles = tuple(args.backend_read_role or DEFAULT_BACKEND_READ_ROLES)
    with psycopg.connect(database_dsn, autocommit=False) as conn:
        states = inspect_objects(conn)
        plan = build_plan(states=states, drop_old_public=bool(args.drop_old_public), backend_read_roles=read_roles)
        result: dict[str, Any] = {
            "apply": bool(args.apply),
            "drop_old_public": bool(args.drop_old_public),
            "objects": [state_to_dict(state) for state in states],
            "actions": plan,
        }
        if args.apply:
            apply_plan(conn, states=states, drop_old_public=bool(args.drop_old_public), backend_read_roles=read_roles)
            conn.commit()
            result["status"] = "applied"
            result["post_apply_objects"] = [state_to_dict(state) for state in inspect_objects(conn)]
        else:
            conn.rollback()
            result["status"] = "dry_run"

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile live ETF objects to the canonical source_cache contract.")
    parser.add_argument("--database-dsn", default=None)
    parser.add_argument("--apply", action="store_true", help="Apply the reconciliation. Dry-run is the default.")
    parser.add_argument(
        "--drop-old-public",
        action="store_true",
        help="After copying data, drop old public ETF holdings/themes tables.",
    )
    parser.add_argument(
        "--backend-read-role",
        action="append",
        default=[],
        help="Backend read role to grant SELECT when it exists. May be repeated.",
    )
    return parser


def inspect_objects(conn: Any) -> list[ObjectState]:
    object_rows = _fetch_object_rows(conn)
    states: list[ObjectState] = []
    for schema_name, table_name in ETF_OBJECTS:
        relkind = object_rows.get((schema_name, table_name))
        rows = _count_rows(conn, schema_name=schema_name, table_name=table_name) if relkind else None
        states.append(ObjectState(schema_name=schema_name, table_name=table_name, relkind=relkind, rows=rows))
    return states


def build_plan(
    *,
    states: list[ObjectState],
    drop_old_public: bool = False,
    backend_read_roles: tuple[str, ...] = DEFAULT_BACKEND_READ_ROLES,
) -> list[dict[str, Any]]:
    by_name = {state.qualified_name: state for state in states}
    actions: list[dict[str, Any]] = []
    for name in ("source_cache.etf_holdings", "source_cache.etf_themes"):
        state = by_name[name]
        if state.relkind == "v":
            actions.append({"action": "replace_view_with_table", "object": name, "rows_visible_before": state.rows})
        elif state.relkind in {None, "r", "p"}:
            actions.append({"action": "ensure_writable_table", "object": name, "current_kind": state.kind})
        else:
            actions.append({"action": "manual_review_required", "object": name, "current_kind": state.kind})
    readiness = by_name["source_cache.etf_theme_readiness"]
    actions.append(
        {
            "action": "ensure_readiness_snapshot_table",
            "object": readiness.qualified_name,
            "current_kind": readiness.kind,
            "rows_visible_before": readiness.rows,
        }
    )
    for name in ("public.etf_holdings", "public.etf_themes"):
        state = by_name[name]
        actions.append(
            {
                "action": "copy_old_public_rows" if state.relkind else "old_public_source_missing",
                "object": name,
                "current_kind": state.kind,
                "rows": state.rows,
            }
        )
    actions.append({"action": "rebuild_readiness", "object": "source_cache.etf_theme_readiness"})
    actions.append(
        {
            "action": "grant_roles",
            "worker_role": WORKER_ROLE,
            "backend_read_roles": list(backend_read_roles),
            "conditional": True,
        }
    )
    if drop_old_public:
        actions.append({"action": "drop_old_public_tables", "objects": ["public.etf_holdings", "public.etf_themes"]})
    return actions


def apply_plan(
    conn: Any,
    *,
    states: list[ObjectState],
    drop_old_public: bool = False,
    backend_read_roles: tuple[str, ...] = DEFAULT_BACKEND_READ_ROLES,
) -> None:
    _validate_apply_safe(states)
    by_name = {state.qualified_name: state for state in states}
    _execute(conn, "create schema if not exists source_cache")
    if by_name["source_cache.etf_holdings"].relkind == "v":
        _execute(conn, "drop view source_cache.etf_holdings")
    if by_name["source_cache.etf_themes"].relkind == "v":
        _execute(conn, "drop view source_cache.etf_themes")

    for statement in [
        CREATE_ETF_HOLDINGS_TABLE_SQL,
        CREATE_ETF_HOLDINGS_INDEX_SQL,
        CREATE_ETF_THEMES_TABLE_SQL,
        CREATE_ETF_THEMES_INDEX_SQL,
        CREATE_ETF_THEME_READINESS_TABLE_SQL,
        CREATE_ETF_THEME_READINESS_INDEX_SQL,
    ]:
        _execute(conn, statement)

    if by_name["public.etf_holdings"].relkind:
        _execute(conn, COPY_PUBLIC_HOLDINGS_SQL)
    if by_name["public.etf_themes"].relkind:
        _execute(conn, COPY_PUBLIC_THEMES_SQL)
    _execute(conn, REFRESH_READINESS_SQL)
    _execute(conn, _grant_sql(backend_read_roles))
    if drop_old_public:
        for statement in DROP_OLD_PUBLIC_SQL:
            _execute(conn, statement)


def state_to_dict(state: ObjectState) -> dict[str, Any]:
    return {
        "object": state.qualified_name,
        "kind": state.kind,
        "relkind": state.relkind,
        "rows": state.rows,
    }


def _validate_apply_safe(states: list[ObjectState]) -> None:
    invalid = [
        state
        for state in states
        if state.qualified_name in {"source_cache.etf_holdings", "source_cache.etf_themes"}
        and state.relkind not in {None, "r", "p", "v"}
    ]
    if invalid:
        details = ", ".join(f"{state.qualified_name}={state.kind}" for state in invalid)
        raise RuntimeError(f"Cannot reconcile unexpected ETF object kind(s): {details}")


def _fetch_object_rows(conn: Any) -> dict[tuple[str, str], str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select n.nspname as schema_name, c.relname as table_name, c.relkind
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where (n.nspname, c.relname) in (
              ('source_cache', 'etf_holdings'),
              ('source_cache', 'etf_themes'),
              ('source_cache', 'etf_theme_readiness'),
              ('public', 'etf_holdings'),
              ('public', 'etf_themes')
            )
            """
        )
        return {
            (str(row[0]), str(row[1])): str(row[2])
            for row in cur.fetchall()
        }


def _count_rows(conn: Any, *, schema_name: str, table_name: str) -> int | None:
    try:
        with conn.cursor() as cur:
            cur.execute(f'select count(*) from "{schema_name}"."{table_name}"')
            row = cur.fetchone()
            return int(row[0]) if row else None
    except Exception:
        return None


def _execute(conn: Any, statement: str) -> None:
    with conn.cursor() as cur:
        cur.execute(statement)


def _grant_sql(backend_read_roles: tuple[str, ...]) -> str:
    role_literal = "array[" + ", ".join(_sql_literal(role) for role in backend_read_roles) + "]"
    return GRANT_SQL_TEMPLATE.format(read_roles=role_literal)


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


if __name__ == "__main__":
    raise SystemExit(main())
