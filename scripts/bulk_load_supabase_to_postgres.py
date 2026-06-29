#!/usr/bin/env python3
"""One-time Supabase to self-hosted Postgres bulk loader.

This script is intentionally not a Prefect flow. The server agent runs it once
with explicit source/target credentials after the local schema is in place.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import urllib.parse
import urllib.request


TABLES = [
    ("public.market_price_daily", "ticker,date"),
    ("public.market_quotes", "ticker"),
    ("public.market_quotes_history", "ticker,fetched_at"),
    ("public.market_fundamentals_v2", "ticker,period,period_end,metric"),
    ("public.ticker_fundamental_point_in_time", "ticker,metric"),
    ("public.ticker_fundamental_summary", "ticker"),
    ("public.market_earnings_events", "ticker,earnings_date"),
    ("public.market_earnings_history", "ticker,earnings_date"),
    ("public.exchange_trading_calendar", "exchange_mic,session_date"),
    ("public.macro_series_catalog", "series_key"),
    ("public.macro_observations", "series_key,observation_period"),
    ("public.macro_daily", "as_of_date,series_key"),
    ("public.economic_release_calendar", "series_key,observation_period"),
    ("public.ticker_registry", "registry_key"),
    ("public.analysis_jobs", "job_id"),
    ("public.analysis_results", "job_id"),
    ("public.etf_holdings", "etf_ticker,holding_symbol,as_of"),
    ("public.etf_sector_weights", "etf_ticker,sector,as_of"),
]


def main() -> None:
    args = build_parser().parse_args()
    target_dsn = args.target_dsn or os.environ.get("DATA_OPS_DATABASE_URL") or os.environ.get("DATABASE_URL")
    source_dsn = args.source_dsn or os.environ.get("SUPABASE_DATABASE_URL")
    if not target_dsn:
        raise RuntimeError("Target DSN is required via --target-dsn, DATA_OPS_DATABASE_URL, or DATABASE_URL.")
    if not source_dsn and not (args.supabase_url and args.supabase_key):
        raise RuntimeError("Provide --source-dsn or --supabase-url/--supabase-key for read-only extraction.")

    for table_name, conflict_columns in _selected_tables(args.tables):
        if source_dsn:
            copy_table_postgres_to_postgres(
                source_dsn=source_dsn,
                target_dsn=target_dsn,
                table_name=table_name,
                conflict_columns=conflict_columns,
                truncate=bool(args.truncate),
            )
        else:
            copy_table_rest_to_postgres(
                supabase_url=str(args.supabase_url),
                supabase_key=str(args.supabase_key),
                target_dsn=target_dsn,
                table_name=table_name,
                conflict_columns=conflict_columns,
                page_size=int(args.page_size),
                truncate=bool(args.truncate),
            )
    if not args.skip_source_cache_backfill:
        backfill_source_cache_from_public(target_dsn)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bulk-load historical Supabase tables into local Postgres.")
    parser.add_argument("--source-dsn", default=None, help="Supabase direct Postgres DSN. Preferred path.")
    parser.add_argument("--target-dsn", default=None, help="Local Postgres/pgbouncer DSN.")
    parser.add_argument("--supabase-url", default=os.environ.get("SUPABASE_URL"))
    parser.add_argument("--supabase-key", default=os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_SECRET_KEY"))
    parser.add_argument("--tables", default="all", help="Comma-separated table names or all.")
    parser.add_argument("--page-size", type=int, default=50000)
    parser.add_argument("--truncate", action="store_true", help="Truncate target tables before loading selected tables.")
    parser.add_argument(
        "--skip-source-cache-backfill",
        action="store_true",
        help="Load public history only; do not backfill source_cache tables from public history.",
    )
    return parser


def _selected_tables(raw: str) -> list[tuple[str, str]]:
    token = str(raw or "all").strip()
    if token == "all":
        return list(TABLES)
    wanted = {part.strip() for part in token.split(",") if part.strip()}
    return [(table, conflict) for table, conflict in TABLES if table in wanted or table.split(".", 1)[-1] in wanted]


def copy_table_postgres_to_postgres(
    *,
    source_dsn: str,
    target_dsn: str,
    table_name: str,
    conflict_columns: str,
    truncate: bool,
) -> None:
    import psycopg

    schema_name, relation_name = _parse_table(table_name)
    with psycopg.connect(source_dsn) as source_conn, psycopg.connect(target_dsn, autocommit=True) as target_conn:
        columns = _target_columns(target_conn, schema_name=schema_name, table_name=relation_name)
        if not columns:
            print(f"skip {table_name}: target table missing")
            return
        if truncate:
            target_conn.execute(f'truncate table "{schema_name}"."{relation_name}"')
        column_sql = ", ".join(f'"{column}"' for column in columns)
        copy_sql = f'copy (select {column_sql} from "{schema_name}"."{relation_name}") to stdout with csv header'
        temp_name = create_temp_like(target_conn, schema_name=schema_name, table_name=relation_name)
        with source_conn.cursor() as source_cur, target_conn.cursor() as target_cur:
            with source_cur.copy(copy_sql) as source_copy:
                with target_cur.copy(f'copy "{temp_name}" ({column_sql}) from stdin with csv header') as target_copy:
                    for chunk in source_copy:
                        target_copy.write(chunk)
        loaded = merge_temp_to_target(
            target_conn=target_conn,
            temp_name=temp_name,
            table_name=table_name,
            columns=columns,
            conflict_columns=conflict_columns,
        )
    print(f"loaded {table_name}: {loaded} rows")


def copy_table_rest_to_postgres(
    *,
    supabase_url: str,
    supabase_key: str,
    target_dsn: str,
    table_name: str,
    conflict_columns: str,
    page_size: int,
    truncate: bool,
) -> None:
    import psycopg

    schema_name, relation_name = _parse_table(table_name)
    if schema_name != "public":
        print(f"skip {table_name}: REST fallback only supports public schema")
        return
    with psycopg.connect(target_dsn, autocommit=True) as target_conn:
        columns = _target_columns(target_conn, schema_name=schema_name, table_name=relation_name)
        if truncate:
            target_conn.execute(f'truncate table "{schema_name}"."{relation_name}"')
        offset = 0
        total = 0
        while True:
            csv_text = fetch_supabase_csv(
                supabase_url=supabase_url,
                supabase_key=supabase_key,
                table_name=relation_name,
                columns=columns,
                order_columns=[part.strip() for part in conflict_columns.split(",") if part.strip()],
                offset=offset,
                limit=page_size,
            )
            csv_text = normalize_rest_csv_for_target(target_conn, table_name=table_name, csv_text=csv_text)
            rows = max(sum(1 for _ in csv.reader(io.StringIO(csv_text))) - 1, 0)
            if rows == 0:
                break
            total += copy_csv_to_target(
                target_conn=target_conn,
                table_name=table_name,
                columns=columns,
                csv_text=csv_text,
                conflict_columns=conflict_columns,
            )
            offset += rows
    print(f"loaded {table_name}: {total} rows")


def fetch_supabase_csv(
    *,
    supabase_url: str,
    supabase_key: str,
    table_name: str,
    columns: list[str],
    order_columns: list[str],
    offset: int,
    limit: int,
) -> str:
    order = ",".join(f"{column}.asc" for column in order_columns)
    query = urllib.parse.urlencode(
        {"select": ",".join(columns), "order": order, "offset": str(offset), "limit": str(limit)}
    )
    url = f"{supabase_url.rstrip('/')}/rest/v1/{urllib.parse.quote(table_name)}?{query}"
    request = urllib.request.Request(
        url,
        headers={
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Accept": "text/csv",
        },
    )
    with urllib.request.urlopen(request, timeout=300) as response:
        return response.read().decode("utf-8")


def copy_csv_to_target(
    *,
    target_conn,
    table_name: str,
    columns: list[str],
    csv_text: str,
    conflict_columns: str,
) -> int:
    schema_name, relation_name = _parse_table(table_name)
    temp_name = create_temp_like(target_conn, schema_name=schema_name, table_name=relation_name)
    column_sql = ", ".join(f'"{column}"' for column in columns)
    with target_conn.cursor() as cur:
        with cur.copy(f'copy "{temp_name}" ({column_sql}) from stdin with csv header') as copy:
            copy.write(csv_text)
    return merge_temp_to_target(
        target_conn=target_conn,
        temp_name=temp_name,
        table_name=table_name,
        columns=columns,
        conflict_columns=conflict_columns,
    )


def normalize_rest_csv_for_target(target_conn, *, table_name: str, csv_text: str) -> str:
    schema_name, relation_name = _parse_table(table_name)
    json_columns = {
        str(row[0])
        for row in target_conn.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = %s and table_name = %s
              and data_type in ('json', 'jsonb')
            """,
            (schema_name, relation_name),
        ).fetchall()
    }
    if not json_columns:
        return csv_text

    source = io.StringIO(csv_text)
    reader = csv.DictReader(source)
    if not reader.fieldnames:
        return csv_text
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=reader.fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in reader:
        for column in json_columns.intersection(row):
            value = row.get(column)
            if not value:
                continue
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError:
                try:
                    decoded = json.loads(value.replace('\\"', '"'))
                except json.JSONDecodeError:
                    continue
            if isinstance(decoded, str):
                try:
                    decoded = json.loads(decoded)
                except json.JSONDecodeError:
                    pass
            row[column] = json.dumps(decoded, separators=(",", ":"))
        writer.writerow(row)
    return output.getvalue()


def create_temp_like(target_conn, *, schema_name: str, table_name: str) -> str:
    temp_name = f"bulk_{schema_name}_{table_name}"
    target_conn.execute(f'drop table if exists "{temp_name}"')
    target_conn.execute(f'create temp table "{temp_name}" (like "{schema_name}"."{table_name}" including defaults)')
    return temp_name


def merge_temp_to_target(
    *,
    target_conn,
    temp_name: str,
    table_name: str,
    columns: list[str],
    conflict_columns: str,
) -> int:
    schema_name, relation_name = _parse_table(table_name)
    column_sql = ", ".join(f'"{column}"' for column in columns)
    conflict = [part.strip() for part in conflict_columns.split(",") if part.strip()]
    update_columns = [column for column in columns if column not in set(conflict)]
    conflict_sql = ", ".join(f'"{column}"' for column in conflict)
    update_sql = ", ".join(f'"{column}" = excluded."{column}"' for column in update_columns)
    target_conn.execute(
        f'insert into "{schema_name}"."{relation_name}" ({column_sql}) '
        f'select {column_sql} from "{temp_name}" '
        f'on conflict ({conflict_sql}) do update set {update_sql}'
    )
    with target_conn.cursor() as cur:
        cur.execute(f'select count(*) from "{temp_name}"')
        return int(cur.fetchone()[0])


def backfill_source_cache_from_public(target_dsn: str) -> None:
    import psycopg

    statements = [
        """
        insert into source_cache.market_price_daily (
          symbol, price_date, open, high, low, close, adj_close, volume, source_updated_at, ingested_at
        )
        select ticker, date, open, high, low, close, adj_close, volume::bigint, fetched_at, created_at
        from public.market_price_daily
        on conflict (symbol, price_date) do update set
          open = excluded.open, high = excluded.high, low = excluded.low, close = excluded.close,
          adj_close = excluded.adj_close, volume = excluded.volume,
          source_updated_at = excluded.source_updated_at, ingested_at = excluded.ingested_at
        """,
        """
        insert into source_cache.macro_daily (
          as_of_date, series_key, value, source_updated_at, alignment_mode, is_stale, staleness_bdays, ingested_at
        )
        select as_of_date, series_key, value, available_at_utc, alignment_mode, is_stale, staleness_bdays, ingested_at
        from public.macro_daily
        on conflict (as_of_date, series_key) do update set
          value = excluded.value, source_updated_at = excluded.source_updated_at,
          alignment_mode = excluded.alignment_mode, is_stale = excluded.is_stale,
          staleness_bdays = excluded.staleness_bdays, ingested_at = excluded.ingested_at
        """,
        """
        insert into source_cache.fundamentals (
          symbol, report_date, metric, value, value_text, period_end, period_type,
          fiscal_year, fiscal_quarter, currency, source, source_updated_at, ingested_at
        )
        select
          ticker,
          coalesce(fetched_at::date, period_end),
          metric,
          value,
          value_text,
          period_end,
          coalesce(nullif(period, ''), 'unknown'),
          extract(year from period_end)::integer,
          case when upper(period) ~ 'Q[1-4]' then substring(upper(period) from 'Q[1-4]') else null end,
          'USD',
          source,
          fetched_at,
          coalesce(fetched_at, now())
        from public.market_fundamentals_v2
        on conflict (symbol, metric, period_end, period_type, report_date) do update set
          value = excluded.value, value_text = excluded.value_text, fiscal_year = excluded.fiscal_year,
          fiscal_quarter = excluded.fiscal_quarter, currency = excluded.currency, source = excluded.source,
          source_updated_at = excluded.source_updated_at, ingested_at = excluded.ingested_at
        """,
        """
        insert into source_cache.earnings (
          symbol, report_date, earnings_date, fiscal_period, earnings_time, actual_eps, estimate_eps,
          surprise_eps, actual_revenue, estimate_revenue, surprise_revenue, currency, source,
          source_updated_at, ingested_at
        )
        select
          ticker,
          coalesce(fetched_at::date, earnings_date),
          earnings_date,
          coalesce(nullif(fiscal_period, ''), 'unknown'),
          earnings_time,
          null::double precision,
          estimate_eps,
          null::double precision,
          null::double precision,
          estimate_revenue,
          null::double precision,
          'USD',
          source,
          fetched_at,
          coalesce(updated_at, fetched_at, now())
        from public.market_earnings_events
        on conflict (symbol, report_date, earnings_date, fiscal_period) do update set
          earnings_time = excluded.earnings_time, estimate_eps = excluded.estimate_eps,
          estimate_revenue = excluded.estimate_revenue, source = excluded.source,
          source_updated_at = excluded.source_updated_at, ingested_at = excluded.ingested_at
        """,
        """
        insert into source_cache.earnings (
          symbol, report_date, earnings_date, fiscal_period, earnings_time, actual_eps, estimate_eps,
          surprise_eps, actual_revenue, estimate_revenue, surprise_revenue, currency, source,
          source_updated_at, ingested_at
        )
        select
          ticker,
          coalesce(fetched_at::date, earnings_date),
          earnings_date,
          coalesce(nullif(fiscal_period, ''), 'unknown'),
          null,
          actual_eps,
          estimate_eps,
          surprise_eps,
          actual_revenue,
          estimate_revenue,
          surprise_revenue,
          'USD',
          source,
          fetched_at,
          coalesce(updated_at, fetched_at, now())
        from public.market_earnings_history
        on conflict (symbol, report_date, earnings_date, fiscal_period) do update set
          actual_eps = excluded.actual_eps, estimate_eps = excluded.estimate_eps,
          surprise_eps = excluded.surprise_eps, actual_revenue = excluded.actual_revenue,
          estimate_revenue = excluded.estimate_revenue, surprise_revenue = excluded.surprise_revenue,
          source = excluded.source, source_updated_at = excluded.source_updated_at, ingested_at = excluded.ingested_at
        """,
        """
        insert into source_cache.calendars (
          exchange_mic, session_date, is_trading_day, is_half_day, source, source_updated_at, ingested_at
        )
        select exchange_mic, session_date, true, is_half_day, 'exchange_calendars', ingested_at, ingested_at
        from public.exchange_trading_calendar
        on conflict (exchange_mic, session_date) do update set
          is_trading_day = excluded.is_trading_day, is_half_day = excluded.is_half_day,
          source = excluded.source, source_updated_at = excluded.source_updated_at,
          ingested_at = excluded.ingested_at
        """,
    ]
    with psycopg.connect(target_dsn, autocommit=True) as conn:
        for statement in statements:
            conn.execute(statement)
    print("backfilled source_cache tables from public history")


def _target_columns(conn, *, schema_name: str, table_name: str) -> list[str]:
    rows = conn.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = %s and table_name = %s
          and is_generated = 'NEVER'
        order by ordinal_position
        """,
        (schema_name, table_name),
    ).fetchall()
    return [str(row[0]) for row in rows]


def _parse_table(table_name: str) -> tuple[str, str]:
    parts = str(table_name).strip().split(".")
    if len(parts) != 2:
        raise ValueError(f"Expected schema.table, got {table_name!r}")
    for part in parts:
        if not part.replace("_", "a").isalnum() or part[0].isdigit():
            raise ValueError(f"Invalid identifier in table name: {table_name!r}")
    return parts[0], parts[1]


if __name__ == "__main__":
    main()
