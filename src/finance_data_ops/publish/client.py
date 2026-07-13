"""Postgres publishing client primitives."""

from __future__ import annotations

from datetime import date, datetime
import json
from dataclasses import dataclass, field
from numbers import Integral, Real
from typing import Any, Protocol

import numpy as np
import pandas as pd


class Publisher(Protocol):
    def upsert(self, table: str, rows: list[dict[str, Any]], *, on_conflict: str | None = None) -> dict[str, Any]:
        ...

    def insert(self, table: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        ...

    def rpc(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        ...


def to_json_safe(value: Any) -> Any:
    """Recursively coerce pandas/numpy values into JSON-safe Python primitives."""

    if value is None:
        return None

    if _is_missing_scalar(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, bool):
        return value

    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Real):
        return float(value)

    if isinstance(value, np.ndarray):
        return [to_json_safe(inner) for inner in value.tolist()]
    if isinstance(value, dict):
        return {str(key): to_json_safe(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_json_safe(inner) for inner in value]

    return value


def _is_missing_scalar(value: Any) -> bool:
    if isinstance(value, (str, bytes, bytearray, dict, list, tuple, set)):
        return False
    try:
        missing = pd.isna(value)
    except Exception:
        return False
    if isinstance(missing, (bool, np.bool_)):
        return bool(missing)
    return False


class PostgresPublisher:
    def __init__(
        self,
        *,
        database_dsn: str,
        application_name: str = "finance-data-ops",
    ) -> None:
        self.database_dsn = str(database_dsn).strip()
        self.application_name = str(application_name).strip() or "finance-data-ops"
        if not self.database_dsn:
            raise ValueError("DATA_OPS_DATABASE_URL is required.")

    def upsert(
        self,
        table: str,
        rows: list[dict[str, Any]],
        *,
        on_conflict: str | None = None,
    ) -> dict[str, Any]:
        if not rows:
            return {"table": table, "status": "skipped", "rows": 0}
        normalized_rows = to_json_safe(rows)
        schema_name, table_name = _parse_table_name(table)
        conflict_columns = _parse_identifier_list(on_conflict)
        if not conflict_columns:
            raise ValueError(f"on_conflict is required for Postgres upsert into {table}.")

        columns = _ordered_columns(normalized_rows)
        values = [[_adapt_postgres_value(row.get(column)) for column in columns] for row in normalized_rows]
        update_columns = [column for column in columns if column not in set(conflict_columns)]
        query = _build_upsert_sql(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
        )
        with _connect(self.database_dsn, self.application_name) as conn:
            with conn.cursor() as cur:
                cur.executemany(query, values)
        return {"table": table, "status": "ok", "rows": len(normalized_rows), "status_code": 200}

    def insert(self, table: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {"table": table, "status": "skipped", "rows": 0}
        normalized_rows = to_json_safe(rows)
        schema_name, table_name = _parse_table_name(table)
        columns = _ordered_columns(normalized_rows)
        values = [[_adapt_postgres_value(row.get(column)) for column in columns] for row in normalized_rows]
        query = _build_insert_sql(schema_name=schema_name, table_name=table_name, columns=columns)
        with _connect(self.database_dsn, self.application_name) as conn:
            with conn.cursor() as cur:
                cur.executemany(query, values)
        return {"table": table, "status": "ok", "rows": len(normalized_rows), "status_code": 200}

    def rpc(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        function_name = _parse_function_name(name)
        with _connect(self.database_dsn, self.application_name) as conn:
            with conn.cursor() as cur:
                if args:
                    cur.execute(f"select public.{function_name}(%s)", (_adapt_postgres_value(args),))
                else:
                    cur.execute(f"select public.{function_name}()")
        return {"status": "ok", "name": name, "status_code": 200}


def _connect(database_dsn: str, application_name: str):
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - exercised in deployment env
        raise RuntimeError("psycopg[binary] is required for Postgres publishing.") from exc
    return psycopg.connect(database_dsn, autocommit=True, application_name=application_name)


def _adapt_postgres_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        try:
            from psycopg.types.json import Jsonb
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("psycopg JSON adapters are unavailable.") from exc
        return Jsonb(value)
    return value


def _build_upsert_sql(
    *,
    schema_name: str,
    table_name: str,
    columns: list[str],
    conflict_columns: list[str],
    update_columns: list[str],
) -> str:
    column_sql = ", ".join(_quote_ident(column) for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    conflict_sql = ", ".join(_quote_ident(column) for column in conflict_columns)
    if update_columns:
        update_sql = ", ".join(
            _build_update_assignment(schema_name=schema_name, table_name=table_name, column=column)
            for column in update_columns
        )
        conflict_action = f"do update set {update_sql}"
    else:
        conflict_action = "do nothing"
    return (
        f"insert into {_quote_ident(schema_name)}.{_quote_ident(table_name)} ({column_sql}) "
        f"values ({placeholders}) on conflict ({conflict_sql}) {conflict_action}"
    )


def _build_insert_sql(*, schema_name: str, table_name: str, columns: list[str]) -> str:
    column_sql = ", ".join(_quote_ident(column) for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"insert into {_quote_ident(schema_name)}.{_quote_ident(table_name)} ({column_sql}) values ({placeholders})"


def _build_update_assignment(*, schema_name: str, table_name: str, column: str) -> str:
    quoted_column = _quote_ident(column)
    if schema_name == "feature_store" and table_name == "entity_attributes_static" and column == "name":
        return f'{quoted_column} = coalesce({_quote_ident(table_name)}.{quoted_column}, excluded.{quoted_column})'
    return f"{quoted_column} = excluded.{quoted_column}"


def _ordered_columns(rows: list[dict[str, Any]]) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            column = str(key).strip()
            if column and column not in seen:
                _validate_identifier(column)
                seen.add(column)
                columns.append(column)
    if not columns:
        raise ValueError("Cannot upsert rows without columns.")
    return columns


def _parse_table_name(raw: str) -> tuple[str, str]:
    text = str(raw).strip()
    parts = text.split(".")
    if len(parts) == 1:
        schema_name, table_name = "public", parts[0]
    elif len(parts) == 2:
        schema_name, table_name = parts
    else:
        raise ValueError(f"Invalid table name: {raw!r}")
    _validate_identifier(schema_name)
    _validate_identifier(table_name)
    return schema_name, table_name


def _parse_function_name(raw: str) -> str:
    text = str(raw).strip()
    _validate_identifier(text)
    return _quote_ident(text)


def _parse_identifier_list(raw: str | None) -> list[str]:
    if raw is None:
        return []
    out: list[str] = []
    for part in str(raw).split(","):
        value = part.strip()
        if value:
            _validate_identifier(value)
            out.append(value)
    return out


def _validate_identifier(value: str) -> None:
    if not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"Invalid SQL identifier: {value!r}")


def _quote_ident(value: str) -> str:
    _validate_identifier(value)
    return '"' + value.replace('"', '""') + '"'


@dataclass(slots=True)
class RecordingPublisher:
    """In-memory publisher used by tests and local dry-runs."""

    upserts: list[dict[str, Any]] = field(default_factory=list)
    inserts: list[dict[str, Any]] = field(default_factory=list)
    rpcs: list[dict[str, Any]] = field(default_factory=list)

    def upsert(self, table: str, rows: list[dict[str, Any]], *, on_conflict: str | None = None) -> dict[str, Any]:
        normalized_rows = to_json_safe(rows)
        self.upserts.append(
            {
                "table": table,
                "rows": list(normalized_rows),
                "on_conflict": on_conflict,
            }
        )
        return {"table": table, "status": "ok", "rows": len(normalized_rows)}

    def insert(self, table: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        normalized_rows = to_json_safe(rows)
        self.inserts.append(
            {
                "table": table,
                "rows": list(normalized_rows),
            }
        )
        return {"table": table, "status": "ok", "rows": len(normalized_rows)}

    def rpc(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        self.rpcs.append({"name": name, "args": dict(args or {})})
        return {"status": "ok", "name": name}
