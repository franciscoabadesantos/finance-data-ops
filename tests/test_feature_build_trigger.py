from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from types import SimpleNamespace
from typing import Any

from finance_data_ops.settings import DataOpsSettings
from flows import prefect_dataops_daily


def _settings(tmp_path) -> DataOpsSettings:
    return DataOpsSettings(
        repo_root=tmp_path,
        cache_root=tmp_path,
        database_dsn="postgresql://example.local/db",
        default_symbols=[],
        default_lookback_days=14,
        default_max_attempts=1,
        symbol_batch_size=100,
        alert_webhook_url="",
    )


@dataclass(frozen=True, slots=True)
class _FakePlan:
    domain: str
    table_name: str
    date_column: str
    gap_exists: bool
    latest_complete_canonical_date: str | None = "2026-06-30"
    mode: str = "normal"
    start_date: str = "2026-06-16"
    end_date: str = "2026-06-30"
    expected_end_date: str = "2026-06-30"
    missing_dates_count: int = 0
    earliest_missing_date: str | None = None
    safety_overlap_days: int = 0
    canonical_source: str = "postgres"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def test_feature_build_trigger_runs_when_watermarks_are_ready(monkeypatch, tmp_path) -> None:
    calls: list[dict[str, Any]] = []
    deployments: list[dict[str, Any]] = []
    alerts: list[dict[str, Any]] = []

    def _fake_resolve_watermark_execution(**kwargs):
        calls.append(kwargs)
        return _FakePlan(
            domain=str(kwargs["domain"]),
            table_name=str(kwargs["table_name"]),
            date_column=str(kwargs["date_column"]),
            gap_exists=False,
        )

    def _fake_run_deployment(name, **kwargs):
        deployments.append({"name": name, **kwargs})
        return SimpleNamespace(id="flow-run-1", state_name="Scheduled")

    monkeypatch.setattr(prefect_dataops_daily, "resolve_watermark_execution", _fake_resolve_watermark_execution)
    monkeypatch.setattr(prefect_dataops_daily, "run_deployment", _fake_run_deployment)
    monkeypatch.setattr(prefect_dataops_daily, "emit_alert", lambda payload: alerts.append(payload))
    monkeypatch.setattr(
        prefect_dataops_daily,
        "emit_alert_webhook",
        lambda payload, *, webhook_url: alerts.append({"webhook": webhook_url, "payload": payload}),
    )

    result = prefect_dataops_daily.trigger_feature_build_daily_if_ready(
        as_of_date="2026-06-30",
        settings=_settings(tmp_path),
    )

    assert result["status"] == "triggered"
    assert result["parameters"] == {"as_of_date": "2026-06-30"}
    assert result["gate"]["degraded"] == []
    assert result["gate"]["blocked"] == []
    assert result["alert"] is None
    assert alerts == []
    assert len(calls) == len(prefect_dataops_daily.FEATURE_BUILD_WATERMARK_REQUIREMENTS)
    assert {call["explicit_end"] for call in calls} == {"2026-06-30"}
    assert deployments == [
        {
            "name": "run_daily_feature_flow/feature-build-daily",
            "parameters": {"as_of_date": "2026-06-30"},
            "timeout": None,
            "poll_interval": 10,
            "flow_run_name": "feature-build-daily-2026-06-30",
            "idempotency_key": "feature-build-daily:2026-06-30",
        }
    ]


def test_feature_build_trigger_degrades_when_soft_watermark_is_not_ready(monkeypatch, tmp_path) -> None:
    deployments: list[dict[str, Any]] = []
    alerts: list[dict[str, Any]] = []

    def _fake_resolve_watermark_execution(**kwargs):
        domain = str(kwargs["domain"])
        return _FakePlan(
            domain=domain,
            table_name=str(kwargs["table_name"]),
            date_column=str(kwargs["date_column"]),
            gap_exists=domain == "macro",
            latest_complete_canonical_date="2026-06-29" if domain == "macro" else "2026-06-30",
            missing_dates_count=1 if domain == "macro" else 0,
            earliest_missing_date="2026-06-30" if domain == "macro" else None,
        )

    def _fake_run_deployment(name, **kwargs):
        deployments.append({"name": name, **kwargs})
        return SimpleNamespace(id="flow-run-1", state_name="Scheduled")

    monkeypatch.setattr(prefect_dataops_daily, "resolve_watermark_execution", _fake_resolve_watermark_execution)
    monkeypatch.setattr(prefect_dataops_daily, "run_deployment", _fake_run_deployment)
    monkeypatch.setattr(prefect_dataops_daily, "emit_alert", lambda payload: alerts.append(payload))
    monkeypatch.setattr(prefect_dataops_daily, "emit_alert_webhook", lambda payload, *, webhook_url: None)

    result = prefect_dataops_daily.trigger_feature_build_daily_if_ready(
        as_of_date="2026-06-30",
        settings=_settings(tmp_path),
    )

    assert result["status"] == "triggered"
    assert result["gate"]["ready"] is True
    assert result["gate"]["blocked"] == []
    assert result["gate"]["degraded"][0]["domain"] == "macro"
    assert result["gate"]["degraded"][0]["lag_days"] == 1
    assert deployments
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "warning"
    assert alerts[0]["run_id"] == "flow-run-1"
    assert alerts[0]["message"] == "Feature build ran for 2026-06-30 with stale sources: macro lag 1d."
    assert alerts[0]["context"]["degraded"][0]["domain"] == "macro"


def test_feature_build_trigger_skips_and_alerts_when_market_watermark_is_not_ready(monkeypatch, tmp_path) -> None:
    alerts: list[dict[str, Any]] = []

    def _fake_resolve_watermark_execution(**kwargs):
        domain = str(kwargs["domain"])
        return _FakePlan(
            domain=domain,
            table_name=str(kwargs["table_name"]),
            date_column=str(kwargs["date_column"]),
            gap_exists=domain == "market",
            latest_complete_canonical_date="2026-06-28" if domain == "market" else "2026-06-30",
            missing_dates_count=2 if domain == "market" else 0,
            earliest_missing_date="2026-06-29" if domain == "market" else None,
        )

    def _unexpected_run_deployment(*_args, **_kwargs):
        raise AssertionError("feature build should not be triggered before market prices are ready")

    monkeypatch.setattr(prefect_dataops_daily, "resolve_watermark_execution", _fake_resolve_watermark_execution)
    monkeypatch.setattr(prefect_dataops_daily, "run_deployment", _unexpected_run_deployment)
    monkeypatch.setattr(prefect_dataops_daily, "emit_alert", lambda payload: alerts.append(payload))
    monkeypatch.setattr(prefect_dataops_daily, "emit_alert_webhook", lambda payload, *, webhook_url: None)

    result = prefect_dataops_daily.trigger_feature_build_daily_if_ready(
        as_of_date="2026-06-30",
        settings=_settings(tmp_path),
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "watermarks_not_ready"
    assert result["gate"]["ready"] is False
    assert result["gate"]["blocked"][0]["domain"] == "market"
    assert result["gate"]["blocked"][0]["lag_days"] == 2
    assert result["gate"]["degraded"] == []
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "error"
    assert alerts[0]["message"] == "Feature build blocked: market prices not ready for 2026-06-30."
    assert alerts[0]["context"]["blocked"][0]["domain"] == "market"


def test_macro_daily_flow_invokes_feature_build_gate_after_publish(monkeypatch, tmp_path) -> None:
    captured: dict[str, Any] = {}

    def _fake_resolve_gap_aware_window(**_kwargs):
        return SimpleNamespace(
            start_date="2026-06-30",
            end_date="2026-06-30",
            mode="normal",
            latest_complete_canonical_date="2026-06-30",
            gap_exists=False,
            as_dict=lambda: {"mode": "normal", "end_date": "2026-06-30"},
        )

    def _fake_run_dataops_macro_daily(**kwargs):
        captured["macro_kwargs"] = kwargs
        return {"ok": True}

    def _fake_trigger_feature_build_daily_if_ready(**kwargs):
        captured["trigger_kwargs"] = kwargs
        return {"status": "triggered", "as_of_date": kwargs["as_of_date"]}

    monkeypatch.setattr(prefect_dataops_daily, "resolve_gap_aware_window", _fake_resolve_gap_aware_window)
    monkeypatch.setattr(prefect_dataops_daily, "run_dataops_macro_daily", _fake_run_dataops_macro_daily)
    monkeypatch.setattr(
        prefect_dataops_daily,
        "trigger_feature_build_daily_if_ready",
        _fake_trigger_feature_build_daily_if_ready,
    )
    monkeypatch.setattr(prefect_dataops_daily, "get_run_logger", lambda: logging.getLogger("test-feature-build"))

    result = prefect_dataops_daily.dataops_macro_daily_flow.fn(
        end="2026-06-30",
        cache_root=str(tmp_path),
        publish_enabled=True,
    )

    assert captured["macro_kwargs"]["end"] == "2026-06-30"
    assert captured["trigger_kwargs"]["as_of_date"] == "2026-06-30"
    assert captured["trigger_kwargs"]["deployment_name"] == "run_daily_feature_flow/feature-build-daily"
    assert result["feature_build_trigger"] == {"status": "triggered", "as_of_date": "2026-06-30"}
