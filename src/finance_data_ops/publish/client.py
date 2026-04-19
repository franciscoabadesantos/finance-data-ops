"""Supabase publishing client primitives."""

from __future__ import annotations

from datetime import date, datetime
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from numbers import Integral, Real
from typing import Any, Protocol

import numpy as np
import pandas as pd


class Publisher(Protocol):
    def upsert(self, table: str, rows: list[dict[str, Any]], *, on_conflict: str | None = None) -> dict[str, Any]:
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


class SupabaseRestPublisher:
    def __init__(
        self,
        *,
        supabase_url: str,
        service_role_key: str,
        timeout_seconds: int = 300,
        max_retries: int = 3,
    ) -> None:
        self.supabase_url = str(supabase_url).strip().rstrip("/")
        self.service_role_key = str(service_role_key).strip()
        self.timeout_seconds = int(timeout_seconds)
        self.max_retries = max(int(max_retries), 1)
        if not self.supabase_url:
            raise ValueError("SUPABASE_URL is required.")
        if not self.service_role_key:
            raise ValueError("SUPABASE_SERVICE_ROLE_KEY is required.")

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
        query = {}
        if on_conflict:
            query["on_conflict"] = str(on_conflict)
        path = f"/rest/v1/{str(table).strip()}"
        if query:
            path = f"{path}?{urllib.parse.urlencode(query)}"
        response = self._request(
            path=path,
            method="POST",
            payload=normalized_rows,
            prefer="resolution=merge-duplicates,return=minimal",
        )
        response.update({"table": table, "rows": len(normalized_rows)})
        return response

    def rpc(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._request(
            path=f"/rest/v1/rpc/{str(name).strip()}",
            method="POST",
            payload=args or {},
            prefer="return=minimal",
        )

    def _request(
        self,
        *,
        path: str,
        method: str,
        payload: Any,
        prefer: str | None = None,
    ) -> dict[str, Any]:
        url = f"{self.supabase_url}{path}"
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json",
        }
        if prefer:
            headers["Prefer"] = prefer
        request = urllib.request.Request(url=url, data=body, headers=headers, method=method)
        attempt = 1
        while True:
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    raw = response.read().decode("utf-8")
                    parsed = json.loads(raw) if raw else None
                    return {"status_code": int(response.status), "data": parsed}
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                retryable_codes = {408, 429, 500, 502, 503, 504, 520, 521, 522, 524}
                if int(exc.code) in retryable_codes and attempt < self.max_retries:
                    time.sleep(float(2 ** (attempt - 1)))
                    attempt += 1
                    continue
                raise RuntimeError(f"Supabase request failed ({method} {path}): HTTP {exc.code} {detail}") from exc
            except (TimeoutError, urllib.error.URLError) as exc:
                if attempt < self.max_retries:
                    time.sleep(float(2 ** (attempt - 1)))
                    attempt += 1
                    continue
                raise RuntimeError(f"Supabase request failed ({method} {path}): {exc!r}") from exc


@dataclass(slots=True)
class RecordingPublisher:
    """In-memory publisher used by tests and local dry-runs."""

    upserts: list[dict[str, Any]] = field(default_factory=list)
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

    def rpc(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        self.rpcs.append({"name": name, "args": dict(args or {})})
        return {"status": "ok", "name": name}
