"""Minimal alert hooks for Data Ops v1 incidents."""

from __future__ import annotations

import json
import logging
import urllib.request
from datetime import UTC, datetime
from typing import Any


def build_alert_payload(
    *,
    severity: str,
    message: str,
    run_id: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "severity": str(severity).strip().lower() or "warning",
        "message": str(message),
        "run_id": run_id,
        "context": dict(context or {}),
        "created_at": datetime.now(UTC).isoformat(),
    }


def emit_alert(payload: dict[str, Any], *, logger: logging.Logger | None = None) -> None:
    sink = logger or logging.getLogger("finance_data_ops.alerts")
    sink.warning("data_ops_alert=%s", json.dumps(payload, sort_keys=True, default=str))


def emit_alert_webhook(
    payload: dict[str, Any],
    *,
    webhook_url: str | None,
    timeout_seconds: int = 10,
    logger: logging.Logger | None = None,
) -> bool:
    destination = str(webhook_url or "").strip()
    if not destination:
        return False

    body = json.dumps(payload, default=str).encode("utf-8")
    request = urllib.request.Request(
        url=destination,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=int(timeout_seconds)):
            pass
        return True
    except Exception as exc:  # pragma: no cover - defensive network boundary
        sink = logger or logging.getLogger("finance_data_ops.alerts")
        sink.warning("alert_webhook_failed=%s", repr(exc))
        return False
