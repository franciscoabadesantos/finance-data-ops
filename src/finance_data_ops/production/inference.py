"""Daily production inference orchestration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
from typing import Any
from uuid import uuid4

from finance_data_ops.publish.client import PostgresPublisher


@dataclass(frozen=True, slots=True)
class ActiveProductionBundle:
    active_pointer: dict[str, Any]
    active_bundle: dict[str, Any]


def run_daily_production_inference(
    *,
    database_dsn: str,
    strategy_family: str,
    universe: str,
    environment: str = "production",
    target_date: str | None = None,
    orchestrator_command: str | None = None,
    orchestrator_config_ref: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    resolved_date = date.fromisoformat(target_date) if target_date else datetime.now(UTC).date()
    active = resolve_active_production_bundle(
        database_dsn=database_dsn,
        strategy_family=strategy_family,
        universe=universe,
        environment=environment,
    )
    payload = {
        "job_type": "daily_inference",
        "backend_job_id": f"dataops-daily-inference-{resolved_date.isoformat()}-{uuid4().hex[:8]}",
        "requested_by": "finance-data-ops",
        "target_dates": [resolved_date.isoformat()],
        "active_pointer": active.active_pointer,
        "active_bundle": active.active_bundle,
        "dry_run": bool(dry_run),
        "requested_at": datetime.now(UTC).isoformat(),
        "orchestrator_config_ref": orchestrator_config_ref,
    }
    payload = {key: value for key, value in payload.items() if value is not None}

    if dry_run:
        return {"status": "dry_run", "payload": payload, "signals_written": 0}
    if not orchestrator_command:
        raise ValueError("orchestrator_command is required unless dry_run=true.")

    result_payload = run_orchestrator_command(
        command_template=orchestrator_command,
        payload=payload,
        target_date=resolved_date.isoformat(),
        strategy_family=strategy_family,
        universe=universe,
        environment=environment,
    )
    signal_rows = build_production_signal_rows(
        result_payload=result_payload,
        active_pointer=active.active_pointer,
        active_bundle=active.active_bundle,
        strategy_family=strategy_family,
        universe=universe,
        environment=environment,
        target_date=resolved_date.isoformat(),
    )
    publisher = PostgresPublisher(database_dsn=database_dsn)
    write_result = publisher.upsert(
        "production_signals",
        signal_rows,
        on_conflict="signal_date,strategy_family,universe,environment,symbol",
    )
    return {
        "status": "completed",
        "target_date": resolved_date.isoformat(),
        "active_pointer_id": active.active_pointer.get("active_pointer_id"),
        "active_bundle_id": active.active_pointer.get("active_bundle_id"),
        "orchestrator_result": result_payload,
        "signals_written": int(write_result.get("rows") or 0),
        "write_result": write_result,
    }


def resolve_active_production_bundle(
    *,
    database_dsn: str,
    strategy_family: str,
    universe: str,
    environment: str,
) -> ActiveProductionBundle:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required to resolve registry active pointers.") from exc

    with psycopg.connect(database_dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select ap.active_pointer_json, b.bundle_manifest_json
                from registry_active_pointers ap
                join registry_bundles b on b.bundle_id = ap.active_bundle_id
                where ap.strategy_family = %s
                  and ap.universe = %s
                  and ap.environment = %s
                limit 1
                """,
                (strategy_family, universe, environment),
            )
            row = cur.fetchone()
    if row is None:
        raise RuntimeError(
            "No registry active pointer found for "
            f"strategy_family={strategy_family} universe={universe} environment={environment}."
        )
    return ActiveProductionBundle(
        active_pointer=dict(row["active_pointer_json"] or {}),
        active_bundle=dict(row["bundle_manifest_json"] or {}),
    )


def run_orchestrator_command(
    *,
    command_template: str,
    payload: dict[str, Any],
    target_date: str,
    strategy_family: str,
    universe: str,
    environment: str,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="finance-data-ops-inference-") as tmp_dir:
        payload_path = Path(tmp_dir) / "daily_inference_payload.json"
        payload_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        format_values = {
            "payload_path": str(payload_path),
            "target_date": target_date,
            "strategy_family": strategy_family,
            "universe": universe,
            "environment": environment,
        }
        command = command_template.format(**format_values)
        completed = subprocess.run(
            shlex.split(command),
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "DAILY_INFERENCE_PAYLOAD_PATH": str(payload_path)},
        )
    if completed.returncode != 0:
        raise RuntimeError(
            "Daily production inference command failed "
            f"(exit={completed.returncode}): {completed.stderr.strip() or completed.stdout.strip()}"
        )
    stdout = completed.stdout.strip()
    if not stdout:
        raise RuntimeError("Daily production inference command produced empty stdout; expected JSON.")
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Daily production inference command stdout was not valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Daily production inference command JSON must be an object.")
    return parsed


def build_production_signal_rows(
    *,
    result_payload: dict[str, Any],
    active_pointer: dict[str, Any],
    active_bundle: dict[str, Any],
    strategy_family: str,
    universe: str,
    environment: str,
    target_date: str,
) -> list[dict[str, Any]]:
    signal_rows = result_payload.get("signals") or result_payload.get("signal_panel_json") or []
    if not isinstance(signal_rows, list):
        raise RuntimeError("Daily production inference result must contain signals or signal_panel_json list.")
    result_ref = result_payload.get("result_ref") or result_payload.get("strategy_signal_panel_ref")
    out: list[dict[str, Any]] = []
    for row in signal_rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("ticker") or row.get("symbol") or row.get("entity_id") or "").strip().upper()
        if not symbol:
            continue
        signal_date = str(row.get("signal_date") or row.get("timestamp") or target_date)
        out.append(
            {
                "signal_date": signal_date,
                "strategy_family": strategy_family,
                "universe": universe,
                "environment": environment,
                "symbol": symbol,
                "active_pointer_id": active_pointer.get("active_pointer_id"),
                "candidate_id": active_pointer.get("active_candidate_id") or active_bundle.get("candidate_id"),
                "bundle_id": active_pointer.get("active_bundle_id") or active_bundle.get("bundle_id"),
                "score": _float_or_none(row.get("score")),
                "rank": _int_or_none(row.get("rank")),
                "percentile": _float_or_none(row.get("percentile")),
                "selected": _bool_or_none(row.get("selected")),
                "horizon": row.get("horizon"),
                "target_exposure": _float_or_none(row.get("target_exposure")),
                "signal_json": row,
                "result_ref": result_ref,
                "orchestrator_run_id": result_payload.get("orchestrator_experiment_id"),
                "updated_at": datetime.now(UTC).isoformat(),
            }
        )
    if not out:
        raise RuntimeError("Daily production inference produced no writable signal rows.")
    return out


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in {"true", "1", "yes", "y"}:
        return True
    if token in {"false", "0", "no", "n"}:
        return False
    return None
