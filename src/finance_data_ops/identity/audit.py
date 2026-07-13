"""Audit helpers for entity identity V0."""

from __future__ import annotations

from typing import Any

from finance_data_ops.identity.models import IdentityAuditRecord


def audit(
    issue_type: str,
    *,
    symbol: str = "",
    entity_id: str = "",
    severity: str = "warning",
    **details: Any,
) -> IdentityAuditRecord:
    return IdentityAuditRecord(
        symbol=str(symbol or "").strip().upper(),
        entity_id=str(entity_id or "").strip(),
        issue_type=issue_type,
        issue_severity=severity,
        details={key: value for key, value in details.items() if value is not None},
    )


def audit_rows(records: list[IdentityAuditRecord]) -> list[dict[str, Any]]:
    return [
        {
            "symbol": record.symbol or None,
            "entity_id": record.entity_id or None,
            "issue_type": record.issue_type,
            "issue_severity": record.issue_severity,
            "details": record.details,
        }
        for record in records
        if record.issue_type
    ]
