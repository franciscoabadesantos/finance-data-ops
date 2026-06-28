from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from typing import Any


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def parse_notes(raw: object) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    text = str(raw).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return {}
    return {}


def payload_hash(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


class WorkerRegistryStore:
    def __init__(self, client_or_dsn: Any) -> None:
        self.client = client_or_dsn if hasattr(client_or_dsn, "table") else None
        self.database_dsn = "" if self.client is not None else str(client_or_dsn or "").strip()
        if self.client is None and self.database_dsn:
            self.client = _PostgresTableClient(self)

    @property
    def _uses_postgres(self) -> bool:
        return bool(self.database_dsn)

    def _connect(self):
        if not self.database_dsn:
            raise RuntimeError("DATABASE_URL is required for the worker registry store.")
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - dependency error path
            raise RuntimeError("psycopg is required for the worker registry store.") from exc
        return psycopg.connect(self.database_dsn, connect_timeout=30, row_factory=dict_row)

    def _fetch_one(self, table_name: str, column: str, value: Any) -> dict[str, Any] | None:
        _validate_identifier(table_name)
        _validate_identifier(column)
        query = f'select * from "{table_name}" where "{column}" = %s limit 1'
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (value,))
                row = cur.fetchone()
        return dict(row) if row else None

    def _fetch_many(
        self,
        table_name: str,
        *,
        predicates: dict[str, Any],
        limit: int = 5000,
        order_by: str | None = None,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        _validate_identifier(table_name)
        for column in predicates:
            _validate_identifier(column)
        if order_by:
            _validate_identifier(order_by)
        where_sql = " and ".join(f'"{column}" = %s' for column in predicates) or "true"
        order_sql = f' order by "{order_by}" {"desc" if descending else "asc"}' if order_by else ""
        query = f'select * from "{table_name}" where {where_sql}{order_sql} limit %s'
        params = [*_adapt_values(predicates.values()), max(1, int(limit))]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    def _update_by(self, table_name: str, key_column: str, key_value: Any, patch: dict[str, Any]) -> dict[str, Any] | None:
        _validate_identifier(table_name)
        _validate_identifier(key_column)
        columns = [str(column) for column in patch]
        for column in columns:
            _validate_identifier(column)
        if not columns:
            return self._fetch_one(table_name, key_column, key_value)
        set_sql = ", ".join(f'"{column}" = %s' for column in columns)
        query = f'update "{table_name}" set {set_sql} where "{key_column}" = %s returning *'
        params = [*_adapt_values(patch[column] for column in columns), key_value]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        return dict(row) if row else None

    def _upsert(self, table_name: str, row: dict[str, Any], *, conflict_columns: list[str]) -> None:
        _validate_identifier(table_name)
        columns = [str(column) for column in row]
        for column in columns:
            _validate_identifier(column)
        for column in conflict_columns:
            _validate_identifier(column)
        placeholders = ", ".join(["%s"] * len(columns))
        column_sql = ", ".join(f'"{column}"' for column in columns)
        conflict_sql = ", ".join(f'"{column}"' for column in conflict_columns)
        update_columns = [column for column in columns if column not in conflict_columns]
        update_sql = ", ".join(f'"{column}" = excluded."{column}"' for column in update_columns)
        action_sql = f"do update set {update_sql}" if update_sql else "do nothing"
        query = f'insert into "{table_name}" ({column_sql}) values ({placeholders}) on conflict ({conflict_sql}) {action_sql}'
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, list(_adapt_values(row[column] for column in columns)))

    def get_by_key(self, registry_key: str) -> dict[str, Any] | None:
        if self._uses_postgres:
            return self._fetch_one("ticker_registry", "registry_key", str(registry_key).strip())
        response = (
            self.client.table("ticker_registry")
            .select("*")
            .eq("registry_key", str(registry_key).strip())
            .limit(1)
            .execute()
        )
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def get_analysis_job(self, job_id: str) -> dict[str, Any] | None:
        if self._uses_postgres:
            return self._fetch_one("analysis_jobs", "job_id", str(job_id).strip())
        response = (
            self.client.table("analysis_jobs")
            .select("*")
            .eq("job_id", str(job_id).strip())
            .limit(1)
            .execute()
        )
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def patch_analysis_job(self, job_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        update_payload = dict(patch)
        if self._uses_postgres:
            row = self._update_by("analysis_jobs", "job_id", str(job_id).strip(), update_payload)
            if row is None:
                raise RuntimeError("analysis_jobs row missing after update.")
            return row
        self.client.table("analysis_jobs").update(update_payload).eq("job_id", str(job_id).strip()).execute()
        row = self.get_analysis_job(job_id)
        if row is None:
            raise RuntimeError("analysis_jobs row missing after update.")
        return row

    def upsert_analysis_result(
        self,
        *,
        job_id: str,
        analysis_type: str,
        result_json: dict[str, Any],
        summary_text: str | None,
    ) -> None:
        row = {
            "job_id": str(job_id).strip(),
            "analysis_type": str(analysis_type).strip(),
            "result_json": dict(result_json),
            "summary_text": (str(summary_text).strip() if summary_text is not None else None),
            "created_at": now_iso(),
        }
        if self._uses_postgres:
            self._upsert("analysis_results", row, conflict_columns=["job_id"])
            return
        try:
            self.client.table("analysis_results").upsert(row, on_conflict="job_id").execute()
            return
        except Exception:
            pass

        existing_response = (
            self.client.table("analysis_results").select("*").eq("job_id", str(job_id).strip()).limit(1).execute()
        )
        existing = existing_response.data or []
        if existing:
            self.client.table("analysis_results").update(row).eq("job_id", str(job_id).strip()).execute()
        else:
            self.client.table("analysis_results").insert(row).execute()

    def patch_row(self, registry_key: str, patch: dict[str, Any]) -> dict[str, Any]:
        update_payload = dict(patch)
        update_payload["updated_at"] = now_iso()
        if self._uses_postgres:
            row = self._update_by("ticker_registry", "registry_key", registry_key, update_payload)
            if row is None:
                raise RuntimeError("ticker_registry row missing after update.")
            return row
        self.client.table("ticker_registry").update(update_payload).eq("registry_key", registry_key).execute()
        row = self.get_by_key(registry_key)
        if row is None:
            raise RuntimeError("ticker_registry row missing after update.")
        return row

    def resolve_registry_row(self, *, ticker: str, region: str, exchange: str | None) -> dict[str, Any] | None:
        if self._uses_postgres:
            predicates: dict[str, Any] = {
                "input_symbol": str(ticker).strip().upper(),
                "region": str(region).strip().lower(),
            }
            if exchange:
                predicates["exchange"] = str(exchange).strip().upper()
            rows = self._fetch_many("ticker_registry", predicates=predicates, limit=1)
            return rows[0] if rows else None
        query = (
            self.client.table("ticker_registry")
            .select("*")
            .eq("input_symbol", str(ticker).strip().upper())
            .eq("region", str(region).strip().lower())
        )
        if exchange:
            query = query.eq("exchange", str(exchange).strip().upper())
        response = query.execute()
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def fetch_market_snapshot(self, ticker: str) -> dict[str, Any] | None:
        if self._uses_postgres:
            return self._fetch_one("ticker_market_stats_snapshot", "ticker", str(ticker).strip().upper())
        response = (
            self.client.table("ticker_market_stats_snapshot")
            .select("*")
            .eq("ticker", str(ticker).strip().upper())
            .limit(1)
            .execute()
        )
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def fetch_next_earnings_row(self, ticker: str) -> dict[str, Any] | None:
        if self._uses_postgres:
            return self._fetch_one("mv_next_earnings", "ticker", str(ticker).strip().upper())
        response = (
            self.client.table("mv_next_earnings")
            .select("*")
            .eq("ticker", str(ticker).strip().upper())
            .limit(1)
            .execute()
        )
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def fetch_symbol_coverage(self, ticker: str) -> dict[str, Any] | None:
        if self._uses_postgres:
            return self._fetch_one("symbol_data_coverage", "ticker", str(ticker).strip().upper())
        response = (
            self.client.table("symbol_data_coverage")
            .select("*")
            .eq("ticker", str(ticker).strip().upper())
            .limit(1)
            .execute()
        )
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def fetch_market_price_daily_rows(self, ticker: str, *, limit: int = 5000) -> list[dict[str, Any]]:
        symbol = str(ticker).strip().upper()
        if self._uses_postgres:
            rows = self._fetch_many(
                "market_price_daily",
                predicates={"ticker": symbol},
                limit=limit,
                order_by="date",
                descending=True,
            )
            if rows:
                return rows
            return self._fetch_rows_for_ticker("market_price_daily", ticker=ticker, limit=limit)
        try:
            query = (
                self.client.table("market_price_daily")
                .select("*")
                .eq("ticker", symbol)
                .order("date", desc=True)
                .limit(max(1, int(limit)))
            )
            response = query.execute()
            rows = [dict(item) for item in (response.data or []) if isinstance(item, dict)]
            if rows:
                return rows
        except Exception:
            pass
        return self._fetch_rows_for_ticker("market_price_daily", ticker=ticker, limit=limit)

    def fetch_fundamentals_rows(self, ticker: str, *, limit: int = 5000) -> list[dict[str, Any]]:
        return self._fetch_rows_for_ticker("market_fundamentals_v2", ticker=ticker, limit=limit)

    def fetch_earnings_history_rows(self, ticker: str, *, limit: int = 5000) -> list[dict[str, Any]]:
        return self._fetch_rows_for_ticker("market_earnings_history", ticker=ticker, limit=limit)

    def fetch_data_asset_status(self) -> dict[str, dict[str, Any]]:
        if self._uses_postgres:
            rows = self._fetch_many("data_asset_status", predicates={}, limit=10000)
            by_key: dict[str, dict[str, Any]] = {}
            for row in rows:
                key = str(row.get("asset_key") or "").strip()
                if key:
                    by_key[key] = row
            return by_key
        response = self.client.table("data_asset_status").select("*").execute()
        rows = [dict(item) for item in (response.data or []) if isinstance(item, dict)]
        by_key: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = str(row.get("asset_key") or "").strip()
            if key:
                by_key[key] = row
        return by_key

    def _fetch_rows_for_ticker(self, table_name: str, *, ticker: str, limit: int) -> list[dict[str, Any]]:
        symbol = str(ticker).strip().upper()
        if self._uses_postgres:
            for column in ("ticker", "symbol"):
                try:
                    rows = self._fetch_many(str(table_name), predicates={column: symbol}, limit=limit)
                except Exception:
                    continue
                if rows:
                    return rows
            return []
        for column in ("ticker", "symbol"):
            try:
                response = (
                    self.client.table(str(table_name))
                    .select("*")
                    .eq(column, symbol)
                    .limit(max(1, int(limit)))
                    .execute()
                )
            except Exception:
                continue
            rows = [dict(item) for item in (response.data or []) if isinstance(item, dict)]
            if rows:
                return rows
        return []

    def merge_notes(self, registry_key: str, patch: dict[str, Any]) -> dict[str, Any]:
        row = self.get_by_key(registry_key)
        if row is None:
            raise RuntimeError("ticker_registry row not found.")
        notes = parse_notes(row.get("notes"))
        notes.update(patch)
        return self.patch_row(registry_key, {"notes": notes})

    def reject(self, registry_key: str, reason: str) -> dict[str, Any]:
        return self.patch_row(
            registry_key,
            {
                "status": "rejected",
                "validation_status": "rejected",
                "promotion_status": "rejected",
                "validation_reason": str(reason),
                "last_validated_at": now_iso(),
            },
        )

    def record_async_job_run(
        self,
        *,
        job_id: str,
        job_type: str,
        registry_key: str | None,
        idempotency_key: str,
        status: str,
        attempt: int,
        payload: dict[str, Any],
        started_at: str | None = None,
        finished_at: str | None = None,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        row = {
            "job_id": str(job_id),
            "job_type": str(job_type),
            "registry_key": str(registry_key or ""),
            "idempotency_key": str(idempotency_key),
            "status": str(status),
            "attempt": int(attempt),
            "payload_hash": payload_hash(payload),
            "started_at": started_at,
            "finished_at": finished_at,
            "error_message": error_message,
            "metadata": metadata or {},
            "updated_at": now_iso(),
        }
        if self._uses_postgres:
            try:
                self._upsert("async_job_runs", row, conflict_columns=["job_id"])
            except Exception:
                return
            return
        try:
            self.client.table("async_job_runs").upsert(row, on_conflict="job_id").execute()
        except Exception:
            # Keep job execution resilient when audit table migration is not present yet.
            return


def _validate_identifier(value: str) -> None:
    if not value or not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"Invalid SQL identifier: {value!r}")


def _adapt_values(values: Any) -> list[Any]:
    try:
        from psycopg.types.json import Jsonb
    except ImportError:  # pragma: no cover - dependency error path
        Jsonb = None  # type: ignore[assignment]
    out: list[Any] = []
    for value in values:
        if isinstance(value, (dict, list)) and Jsonb is not None:
            out.append(Jsonb(value))
        else:
            out.append(value)
    return out


class _Response:
    def __init__(self, data: list[dict[str, Any]] | None = None, count: int | None = None) -> None:
        self.data = data or []
        self.count = count


class _PostgresTableClient:
    def __init__(self, store: WorkerRegistryStore) -> None:
        self.store = store

    def table(self, table_name: str) -> "_PostgresQuery":
        return _PostgresQuery(self.store, table_name)


class _PostgresQuery:
    def __init__(self, store: WorkerRegistryStore, table_name: str) -> None:
        _validate_identifier(str(table_name))
        self.store = store
        self.table_name = str(table_name)
        self.operation = "select"
        self.columns = "*"
        self.count_exact = False
        self.filters: list[tuple[str, str, Any]] = []
        self.limit_count: int | None = None
        self.order_column: str | None = None
        self.order_desc = False
        self.payload: Any = None
        self.conflict_columns: list[str] = []

    def select(self, columns: str = "*", count: str | None = None) -> "_PostgresQuery":
        self.operation = "select"
        self.columns = str(columns or "*")
        self.count_exact = count == "exact"
        return self

    def eq(self, field: str, value: Any) -> "_PostgresQuery":
        _validate_identifier(str(field))
        self.filters.append((str(field), "=", value))
        return self

    def in_(self, field: str, values: list[Any]) -> "_PostgresQuery":
        _validate_identifier(str(field))
        self.filters.append((str(field), "in", list(values)))
        return self

    def gte(self, field: str, value: Any) -> "_PostgresQuery":
        _validate_identifier(str(field))
        self.filters.append((str(field), ">=", value))
        return self

    def lte(self, field: str, value: Any) -> "_PostgresQuery":
        _validate_identifier(str(field))
        self.filters.append((str(field), "<=", value))
        return self

    def order(self, field: str, desc: bool = False) -> "_PostgresQuery":
        _validate_identifier(str(field))
        self.order_column = str(field)
        self.order_desc = bool(desc)
        return self

    def limit(self, count: int) -> "_PostgresQuery":
        self.limit_count = max(1, int(count))
        return self

    def upsert(self, payload: Any, on_conflict: str) -> "_PostgresQuery":
        self.operation = "upsert"
        self.payload = payload
        self.conflict_columns = [part.strip() for part in str(on_conflict).split(",") if part.strip()]
        return self

    def update(self, payload: dict[str, Any]) -> "_PostgresQuery":
        self.operation = "update"
        self.payload = dict(payload)
        return self

    def insert(self, payload: dict[str, Any]) -> "_PostgresQuery":
        self.operation = "insert"
        self.payload = dict(payload)
        return self

    def delete(self) -> "_PostgresQuery":
        self.operation = "delete"
        return self

    def execute(self) -> _Response:
        if self.operation == "select":
            return self._execute_select()
        if self.operation == "upsert":
            rows = self.payload if isinstance(self.payload, list) else [self.payload]
            for row in rows:
                self.store._upsert(self.table_name, dict(row), conflict_columns=self.conflict_columns)
            return _Response([dict(row) for row in rows if isinstance(row, dict)], len(rows))
        if self.operation == "insert":
            row = dict(self.payload or {})
            self._execute_insert(row)
            return _Response([row], 1)
        if self.operation == "update":
            return self._execute_update()
        if self.operation == "delete":
            return self._execute_delete()
        raise RuntimeError(f"Unsupported operation: {self.operation}")

    def _execute_select(self) -> _Response:
        column_sql = self._column_sql()
        where_sql, params = self._where_sql()
        order_sql = ""
        if self.order_column:
            order_sql = f' order by "{self.order_column}" {"desc" if self.order_desc else "asc"}'
        limit_sql = " limit %s" if self.limit_count is not None else ""
        query = f'select {column_sql} from "{self.table_name}" where {where_sql}{order_sql}{limit_sql}'
        query_params = [*params, self.limit_count] if self.limit_count is not None else params
        count_value: int | None = None
        with self.store._connect() as conn:
            with conn.cursor() as cur:
                if self.count_exact:
                    cur.execute(f'select count(*) as count from "{self.table_name}" where {where_sql}', params)
                    count_row = cur.fetchone()
                    count_value = int(count_row["count"] if isinstance(count_row, dict) else count_row[0])
                cur.execute(query, query_params)
                rows = cur.fetchall()
        return _Response([dict(row) for row in rows], count_value)

    def _execute_insert(self, row: dict[str, Any]) -> None:
        columns = [str(column) for column in row]
        for column in columns:
            _validate_identifier(column)
        column_sql = ", ".join(f'"{column}"' for column in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        with self.store._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'insert into "{self.table_name}" ({column_sql}) values ({placeholders})',
                    list(_adapt_values(row[column] for column in columns)),
                )

    def _execute_update(self) -> _Response:
        row = dict(self.payload or {})
        columns = [str(column) for column in row]
        for column in columns:
            _validate_identifier(column)
        set_sql = ", ".join(f'"{column}" = %s' for column in columns)
        where_sql, where_params = self._where_sql()
        params = [*_adapt_values(row[column] for column in columns), *where_params]
        with self.store._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'update "{self.table_name}" set {set_sql} where {where_sql} returning *', params)
                rows = cur.fetchall()
        return _Response([dict(item) for item in rows], len(rows))

    def _execute_delete(self) -> _Response:
        where_sql, params = self._where_sql()
        with self.store._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'delete from "{self.table_name}" where {where_sql} returning *', params)
                rows = cur.fetchall()
        return _Response([dict(item) for item in rows], len(rows))

    def _column_sql(self) -> str:
        if self.columns.strip() == "*":
            return "*"
        columns = [part.strip() for part in self.columns.split(",") if part.strip()]
        for column in columns:
            _validate_identifier(column)
        return ", ".join(f'"{column}"' for column in columns)

    def _where_sql(self) -> tuple[str, list[Any]]:
        if not self.filters:
            return "true", []
        clauses: list[str] = []
        params: list[Any] = []
        for column, operator, value in self.filters:
            if operator == "in":
                values = list(value or [])
                if not values:
                    clauses.append("false")
                    continue
                clauses.append(f'"{column}" = any(%s)')
                params.append(values)
            else:
                clauses.append(f'"{column}" {operator} %s')
                params.append(value)
        return " and ".join(clauses), params
