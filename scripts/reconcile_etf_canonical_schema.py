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
KNOWN_DEPENDENT_VIEWS = (
    ("feature_store", "ticker_readiness"),
    ("feature_store", "ticker_readiness_etf_constituents"),
)
DROP_DEPENDENT_VIEWS_SQL = [
    "drop view if exists feature_store.ticker_readiness",
    "drop view if exists feature_store.ticker_readiness_etf_constituents",
]


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


@dataclass(frozen=True)
class DependencyState:
    dependent_schema: str
    dependent_name: str
    dependent_relkind: str
    source_schema: str
    source_name: str

    @property
    def dependent_qualified_name(self) -> str:
        return f"{self.dependent_schema}.{self.dependent_name}"

    @property
    def source_qualified_name(self) -> str:
        return f"{self.source_schema}.{self.source_name}"

    @property
    def dependent_kind(self) -> str:
        return RELKIND_LABELS.get(str(self.dependent_relkind or ""), "unknown")


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

CREATE_TICKER_READINESS_ETF_CONSTITUENTS_VIEW_SQL = """
create view feature_store.ticker_readiness_etf_constituents as
select distinct
  upper(h.holding_symbol) as symbol,
  upper(h.etf_ticker) as etf_symbol,
  r.theme,
  r.relationship_map_eligible,
  r.relationship_map_ineligible_reason,
  now() as updated_at
from source_cache.etf_holdings h
join source_cache.etf_theme_readiness r
  on upper(r.etf_symbol) = upper(h.etf_ticker)
where h.holding_symbol is not null
"""

CREATE_TICKER_READINESS_VIEW_SQL = """
create view feature_store.ticker_readiness as
with symbols as (
  select upper(normalized_symbol) as symbol
  from public.ticker_registry
  where normalized_symbol is not null
  union
  select upper(symbol) as symbol
  from source_cache.market_price_daily
  where symbol is not null
  union
  select upper(symbol) as symbol
  from source_cache.fundamentals
  where symbol is not null
  union
  select upper(symbol) as symbol
  from source_cache.earnings
  where symbol is not null
  union
  select upper(symbol) as symbol
  from feature_store.technical_features_daily
  where symbol is not null
  union
  select upper(symbol) as symbol
  from feature_store.scorecard_daily
  where symbol is not null
  union
  select upper(symbol) as symbol
  from feature_store.ticker_page_summary
  where symbol is not null
  union
  select upper(symbol) as symbol
  from feature_store.ticker_readiness_etf_constituents
  where symbol is not null
),
materialized as (
  select
    s.symbol,
    exists (
      select 1 from source_cache.market_price_daily p where upper(p.symbol) = s.symbol
    ) as market_data_available,
    exists (
      select 1 from source_cache.fundamentals f where upper(f.symbol) = s.symbol
    ) as fundamentals_available,
    exists (
      select 1 from source_cache.earnings e where upper(e.symbol) = s.symbol
    ) as earnings_available,
    exists (
      select 1 from feature_store.technical_features_daily t where upper(t.symbol) = s.symbol
    ) as technical_features_available,
    exists (
      select 1 from feature_store.scorecard_daily sc where upper(sc.symbol) = s.symbol
    ) as scorecard_available,
    exists (
      select 1 from feature_store.ticker_page_summary ps where upper(ps.symbol) = s.symbol
    ) as ticker_page_summary_available
  from symbols s
)
select
  symbol,
  case
    when market_data_available and technical_features_available then 'ready'
    when market_data_available then 'partial'
    else 'pending'
  end as readiness_status,
  market_data_available,
  fundamentals_available,
  earnings_available,
  technical_features_available,
  scorecard_available,
  ticker_page_summary_available,
  case
    when not market_data_available then 'missing_market_price_daily'
    when not technical_features_available then 'missing_technical_features'
    when not scorecard_available then 'missing_scorecard'
    when not ticker_page_summary_available then 'missing_ticker_page_summary'
    else null
  end as reason,
  now() as updated_at
from materialized
"""

CREATE_DEPENDENT_VIEWS_SQL = [
    CREATE_TICKER_READINESS_ETF_CONSTITUENTS_VIEW_SQL,
    CREATE_TICKER_READINESS_VIEW_SQL,
]

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
        dependencies = inspect_dependencies(conn)
        plan = build_plan(
            states=states,
            dependencies=dependencies,
            drop_old_public=bool(args.drop_old_public),
            backend_read_roles=read_roles,
        )
        result: dict[str, Any] = {
            "apply": bool(args.apply),
            "drop_old_public": bool(args.drop_old_public),
            "objects": [state_to_dict(state) for state in states],
            "dependencies": [dependency_to_dict(dependency) for dependency in dependencies],
            "actions": plan,
        }
        if args.apply:
            try:
                apply_plan(
                    conn,
                    states=states,
                    dependencies=dependencies,
                    drop_old_public=bool(args.drop_old_public),
                    backend_read_roles=read_roles,
                )
            except ReconciliationSQLError:
                conn.rollback()
                raise
            except Exception:
                conn.rollback()
                raise
            else:
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


def inspect_dependencies(conn: Any) -> list[DependencyState]:
    rows = _fetch_dependency_rows(conn)
    return [
        DependencyState(
            dependent_schema=str(row["dependent_schema"]),
            dependent_name=str(row["dependent_name"]),
            dependent_relkind=str(row["dependent_relkind"]),
            source_schema=str(row["source_schema"]),
            source_name=str(row["source_name"]),
        )
        for row in rows
    ]


def build_plan(
    *,
    states: list[ObjectState],
    dependencies: list[DependencyState] | None = None,
    drop_old_public: bool = False,
    backend_read_roles: tuple[str, ...] = DEFAULT_BACKEND_READ_ROLES,
) -> list[dict[str, Any]]:
    by_name = {state.qualified_name: state for state in states}
    dependencies = list(dependencies or [])
    actions: list[dict[str, Any]] = []
    unexpected_dependencies = _unexpected_dependencies(dependencies)
    if unexpected_dependencies:
        actions.append(
            {
                "action": "manual_review_required",
                "reason": "unexpected_dependencies",
                "dependencies": [dependency_to_dict(dependency) for dependency in unexpected_dependencies],
            }
        )
    known_dependencies = _known_dependencies(dependencies)
    if known_dependencies:
        actions.append(
            {
                "action": "drop_known_dependent_views",
                "objects": _known_dependent_view_names(known_dependencies),
                "order": [statement.removeprefix("drop view if exists ") for statement in DROP_DEPENDENT_VIEWS_SQL],
            }
        )
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
    if known_dependencies:
        actions.append(
            {
                "action": "recreate_known_dependent_views",
                "objects": [
                    "feature_store.ticker_readiness_etf_constituents",
                    "feature_store.ticker_readiness",
                ],
            }
        )
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
    dependencies: list[DependencyState] | None = None,
    drop_old_public: bool = False,
    backend_read_roles: tuple[str, ...] = DEFAULT_BACKEND_READ_ROLES,
) -> None:
    dependencies = list(dependencies or [])
    _validate_apply_safe(states, dependencies=dependencies)
    by_name = {state.qualified_name: state for state in states}
    _execute(conn, "create schema if not exists source_cache")
    for statement in DROP_DEPENDENT_VIEWS_SQL:
        if _should_drop_dependent_view(statement, dependencies):
            _execute(conn, statement)
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
    for statement in CREATE_DEPENDENT_VIEWS_SQL:
        if _known_dependencies(dependencies):
            _execute(conn, statement)
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


def dependency_to_dict(dependency: DependencyState) -> dict[str, str]:
    return {
        "dependent": dependency.dependent_qualified_name,
        "dependent_kind": dependency.dependent_kind,
        "source": dependency.source_qualified_name,
    }


def _validate_apply_safe(states: list[ObjectState], *, dependencies: list[DependencyState]) -> None:
    invalid = [
        state
        for state in states
        if state.qualified_name in {"source_cache.etf_holdings", "source_cache.etf_themes"}
        and state.relkind not in {None, "r", "p", "v"}
    ]
    if invalid:
        details = ", ".join(f"{state.qualified_name}={state.kind}" for state in invalid)
        raise RuntimeError(f"Cannot reconcile unexpected ETF object kind(s): {details}")
    unexpected_dependencies = _unexpected_dependencies(dependencies)
    if unexpected_dependencies:
        details = ", ".join(
            f"{dependency.dependent_qualified_name} depends on {dependency.source_qualified_name}"
            for dependency in unexpected_dependencies
        )
        raise RuntimeError(f"Cannot reconcile unexpected ETF dependencies without manual review: {details}")


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


def _fetch_dependency_rows(conn: Any) -> list[dict[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with recursive target_objects as (
              select c.oid, n.nspname as source_schema, c.relname as source_name
              from pg_class c
              join pg_namespace n on n.oid = c.relnamespace
              where (n.nspname, c.relname) in (
                ('source_cache', 'etf_holdings'),
                ('source_cache', 'etf_themes')
              )
            ),
            dependent_views as (
              select
                dependent.oid,
                dependent_ns.nspname as dependent_schema,
                dependent.relname as dependent_name,
                dependent.relkind as dependent_relkind,
                target.source_schema,
                target.source_name
              from target_objects target
              join pg_depend dep on dep.refobjid = target.oid
              join pg_rewrite rewrite on rewrite.oid = dep.objid
              join pg_class dependent on dependent.oid = rewrite.ev_class
              join pg_namespace dependent_ns on dependent_ns.oid = dependent.relnamespace
              where dependent.oid <> target.oid
              union
              select
                next_dependent.oid,
                next_dependent_ns.nspname as dependent_schema,
                next_dependent.relname as dependent_name,
                next_dependent.relkind as dependent_relkind,
                known.dependent_schema as source_schema,
                known.dependent_name as source_name
              from dependent_views known
              join pg_depend dep on dep.refobjid = known.oid
              join pg_rewrite rewrite on rewrite.oid = dep.objid
              join pg_class next_dependent on next_dependent.oid = rewrite.ev_class
              join pg_namespace next_dependent_ns on next_dependent_ns.oid = next_dependent.relnamespace
              where next_dependent.oid <> known.oid
            )
            select distinct dependent_schema, dependent_name, dependent_relkind, source_schema, source_name
            from dependent_views
            order by dependent_schema, dependent_name, source_schema, source_name
            """
        )
        return [
            {
                "dependent_schema": str(row[0]),
                "dependent_name": str(row[1]),
                "dependent_relkind": str(row[2]),
                "source_schema": str(row[3]),
                "source_name": str(row[4]),
            }
            for row in cur.fetchall()
        ]


def _count_rows(conn: Any, *, schema_name: str, table_name: str) -> int | None:
    try:
        with conn.cursor() as cur:
            cur.execute(f'select count(*) from "{schema_name}"."{table_name}"')
            row = cur.fetchone()
            return int(row[0]) if row else None
    except Exception:
        return None


class ReconciliationSQLError(RuntimeError):
    pass


def _execute(conn: Any, statement: str) -> None:
    with conn.cursor() as cur:
        try:
            cur.execute(statement)
        except Exception as exc:
            preview = " ".join(str(statement).strip().split())
            if len(preview) > 600:
                preview = preview[:600] + " ..."
            raise ReconciliationSQLError(f"ETF schema reconciliation SQL failed: {exc}; statement={preview}") from exc


def _grant_sql(backend_read_roles: tuple[str, ...]) -> str:
    role_literal = "array[" + ", ".join(_sql_literal(role) for role in backend_read_roles) + "]"
    return GRANT_SQL_TEMPLATE.format(read_roles=role_literal)


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _known_dependencies(dependencies: list[DependencyState]) -> list[DependencyState]:
    known_names = {f"{schema}.{name}" for schema, name in KNOWN_DEPENDENT_VIEWS}
    return [
        dependency
        for dependency in dependencies
        if dependency.dependent_qualified_name in known_names and dependency.dependent_relkind == "v"
    ]


def _unexpected_dependencies(dependencies: list[DependencyState]) -> list[DependencyState]:
    known_names = {f"{schema}.{name}" for schema, name in KNOWN_DEPENDENT_VIEWS}
    return [
        dependency
        for dependency in dependencies
        if dependency.dependent_qualified_name not in known_names or dependency.dependent_relkind != "v"
    ]


def _known_dependent_view_names(dependencies: list[DependencyState]) -> list[str]:
    names = {dependency.dependent_qualified_name for dependency in dependencies}
    order = [
        "feature_store.ticker_readiness",
        "feature_store.ticker_readiness_etf_constituents",
    ]
    return [name for name in order if name in names]


def _should_drop_dependent_view(statement: str, dependencies: list[DependencyState]) -> bool:
    target = str(statement).removeprefix("drop view if exists ").strip()
    return target in set(_known_dependent_view_names(_known_dependencies(dependencies)))


if __name__ == "__main__":
    raise SystemExit(main())
