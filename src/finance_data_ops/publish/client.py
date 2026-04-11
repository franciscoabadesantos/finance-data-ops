"""Supabase publishing client primitives."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Protocol


class Publisher(Protocol):
    def upsert(self, table: str, rows: list[dict[str, Any]], *, on_conflict: str | None = None) -> dict[str, Any]:
        ...

    def rpc(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        ...


class SupabaseRestPublisher:
    def __init__(self, *, supabase_url: str, service_role_key: str, timeout_seconds: int = 60) -> None:
        self.supabase_url = str(supabase_url).strip().rstrip("/")
        self.service_role_key = str(service_role_key).strip()
        self.timeout_seconds = int(timeout_seconds)
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
        query = {}
        if on_conflict:
            query["on_conflict"] = str(on_conflict)
        path = f"/rest/v1/{str(table).strip()}"
        if query:
            path = f"{path}?{urllib.parse.urlencode(query)}"
        response = self._request(
            path=path,
            method="POST",
            payload=rows,
            prefer="resolution=merge-duplicates,return=minimal",
        )
        response.update({"table": table, "rows": len(rows)})
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
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
                parsed = json.loads(raw) if raw else None
                return {"status_code": int(response.status), "data": parsed}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase request failed ({method} {path}): HTTP {exc.code} {detail}") from exc


@dataclass(slots=True)
class RecordingPublisher:
    """In-memory publisher used by tests and local dry-runs."""

    upserts: list[dict[str, Any]] = field(default_factory=list)
    rpcs: list[dict[str, Any]] = field(default_factory=list)

    def upsert(self, table: str, rows: list[dict[str, Any]], *, on_conflict: str | None = None) -> dict[str, Any]:
        self.upserts.append(
            {
                "table": table,
                "rows": list(rows),
                "on_conflict": on_conflict,
            }
        )
        return {"table": table, "status": "ok", "rows": len(rows)}

    def rpc(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        self.rpcs.append({"name": name, "args": dict(args or {})})
        return {"status": "ok", "name": name}
