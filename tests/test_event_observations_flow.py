from __future__ import annotations
import pytest
from flows import event_observations

def _logger(): return type("Logger", (), {"info": lambda *_: None, "error": lambda *_: None})()
def test_disabled_domains_do_not_execute(monkeypatch) -> None:
    monkeypatch.setattr(event_observations, "get_run_logger", _logger)
    monkeypatch.setattr(event_observations, "run_guidance_shadow", lambda **_: pytest.fail("not selected"))
    assert event_observations.dataops_event_observations_daily_flow.fn(symbols=["AAPL"])["domains"] == {}
def test_fn_uses_standard_logger_without_prefect_context(monkeypatch) -> None:
    monkeypatch.setattr(event_observations, "get_run_logger", lambda: (_ for _ in ()).throw(RuntimeError("no context")))
    monkeypatch.setattr(event_observations, "load_settings", lambda: type("Settings", (), {"database_dsn": ""})())
    assert event_observations.dataops_event_observations_daily_flow.fn(symbols=["AAPL"])["status"] == "completed"
def test_selected_runner_summary_is_preserved(monkeypatch) -> None:
    monkeypatch.setattr(event_observations, "get_run_logger", _logger); monkeypatch.setattr(event_observations, "load_settings", lambda: type("Settings", (), {"database_dsn": "postgresql://test"})())
    monkeypatch.setattr(event_observations, "PostgresGuidanceRepository", lambda **_: object()); monkeypatch.setattr(event_observations, "run_guidance_shadow", lambda **_: {"status": "skipped", "reason": "no_guidance_providers_enabled", "live_calls": 0})
    assert event_observations.dataops_event_observations_daily_flow.fn(symbols=["AAPL"], run_guidance=True)["domains"]["guidance"]["reason"] == "no_guidance_providers_enabled"
def test_domain_failure_is_reported_and_fails_flow(monkeypatch) -> None:
    monkeypatch.setattr(event_observations, "get_run_logger", _logger); monkeypatch.setattr(event_observations, "load_settings", lambda: type("Settings", (), {"database_dsn": "postgresql://test"})())
    monkeypatch.setattr(event_observations, "PostgresGuidanceRepository", lambda **_: object()); monkeypatch.setattr(event_observations, "run_guidance_shadow", lambda **_: (_ for _ in ()).throw(ValueError("boom")))
    with pytest.raises(event_observations.EventObservationsFlowError) as exc_info: event_observations.dataops_event_observations_daily_flow.fn(symbols=["AAPL"], run_guidance=True)
    assert exc_info.value.report["domains"]["guidance"]["reason"] == "ValueError"
