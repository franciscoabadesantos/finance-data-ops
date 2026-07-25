"""Operational orchestration for opt-in observational event shadows."""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, Callable

from prefect import flow, get_run_logger

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.settings import load_settings
from finance_data_ops.shadow.corporate_actions import PostgresCorporateActionsShadowRepository, run_corporate_actions_shadow
from finance_data_ops.shadow.equity_capital_events import PostgresEquityCapitalEventsRepository, run_equity_capital_events_shadow
from finance_data_ops.shadow.fund_distributions import PostgresFundDistributionRepository, run_fund_distributions_shadow
from finance_data_ops.shadow.fund_rebalances import PostgresFundRebalanceRepository, run_fund_rebalances_shadow
from finance_data_ops.shadow.guidance import PostgresGuidanceRepository, run_guidance_shadow
from finance_data_ops.shadow.investor_events import PostgresInvestorEventsRepository, run_investor_events_shadow
from finance_data_ops.shadow.sec_filings import PostgresSecFilingsShadowRepository, run_sec_filings_shadow


class EventObservationsFlowError(RuntimeError):
    def __init__(self, report: dict[str, Any]) -> None:
        self.report = report
        super().__init__(json.dumps(report, default=str, sort_keys=True))


def _get_logger() -> logging.Logger:
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(__name__)


def _symbols(symbols: list[str] | None) -> list[str]:
    return list(dict.fromkeys(str(value).strip().upper() for value in symbols or [] if str(value).strip()))


def _run_selected_domain(name: str, invoke: Callable[[], dict[str, Any]], report: dict[str, Any]) -> None:
    try:
        report["domains"][name] = invoke()
    except Exception as exc:
        report["domains"][name] = {"status": "failed", "reason": exc.__class__.__name__, "error": str(exc)}
        report["failures"].append(name)


@flow(name="dataops-event-observations-daily", retries=0, log_prints=True)
def dataops_event_observations_daily_flow(
    *, symbols: list[str] | None = None,
    run_corporate_actions: bool = False, run_filings: bool = False,
    run_investor_events: bool = False, run_guidance: bool = False,
    run_equity_capital_events: bool = False, run_fund_distributions: bool = False,
    run_fund_rebalances: bool = False, dry_run: bool = False, refresh: bool = False,
) -> dict[str, Any]:
    """Run selected existing shadow runners; providers remain independently fail-closed."""
    requested, settings = _symbols(symbols), load_settings()
    report: dict[str, Any] = {"status": "completed", "mode": "shadow", "symbols": requested, "dry_run": dry_run, "refresh": refresh, "domains": {}, "failures": []}
    logger = _get_logger()

    def needs_symbols(name: str, invoke: Callable[[], dict[str, Any]]) -> None:
        if not requested:
            report["domains"][name] = {"status": "skipped", "reason": "no_symbols_requested"}
        elif not dry_run and not settings.database_dsn:
            report["domains"][name] = {"status": "failed", "reason": "database_url_missing"}; report["failures"].append(name)
        else:
            _run_selected_domain(name, invoke, report)

    if run_corporate_actions:
        needs_symbols("corporate_actions", lambda: run_corporate_actions_shadow(symbols=requested, repository=None if dry_run else PostgresCorporateActionsShadowRepository(database_dsn=settings.database_dsn), dry_run=dry_run, refresh=refresh))
    if run_filings:
        needs_symbols("filings", lambda: run_sec_filings_shadow(symbols=requested, repository=None if dry_run else PostgresSecFilingsShadowRepository(database_dsn=settings.database_dsn), dry_run=dry_run, refresh=refresh))
    if run_investor_events:
        needs_symbols("investor_events", lambda: run_investor_events_shadow(symbols=requested, repository=None if dry_run else PostgresInvestorEventsRepository(database_dsn=settings.database_dsn), dry_run=dry_run, refresh=refresh))
    if run_guidance:
        needs_symbols("guidance", lambda: run_guidance_shadow(symbols=requested, repository=None if dry_run else PostgresGuidanceRepository(database_dsn=settings.database_dsn), dry_run=dry_run, refresh=refresh))
    if run_equity_capital_events:
        needs_symbols("equity_capital_events", lambda: run_equity_capital_events_shadow(symbols=requested, repository=None if dry_run else PostgresEquityCapitalEventsRepository(database_dsn=settings.database_dsn), dry_run=dry_run))
    if run_fund_distributions:
        needs_symbols("fund_distributions", lambda: run_fund_distributions_shadow(symbols=requested, repository=None if dry_run else PostgresFundDistributionRepository(database_dsn=settings.database_dsn), dry_run=dry_run, refresh=refresh))
    if run_fund_rebalances:
        if not dry_run and not settings.database_dsn:
            report["domains"]["fund_rebalances"] = {"status": "failed", "reason": "database_url_missing"}; report["failures"].append("fund_rebalances")
        else:
            _run_selected_domain("fund_rebalances", lambda: run_fund_rebalances_shadow(repository=None if dry_run else PostgresFundRebalanceRepository(database_dsn=settings.database_dsn), dry_run=dry_run, refresh=refresh), report)
    if report["failures"]:
        report["status"] = "failed"; logger.error("event observations failures: %s", json.dumps(report, default=str, sort_keys=True)); raise EventObservationsFlowError(report)
    logger.info("event observations summary: %s", json.dumps(report, default=str, sort_keys=True))
    return report
