"""Prefect wrapper for daily production inference."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

try:
    from prefect import flow
except Exception:  # pragma: no cover - CLI can run without prefect installed
    flow = None  # type: ignore[assignment]

from finance_data_ops.production.inference import run_daily_production_inference
from finance_data_ops.settings import load_settings


def _run(
    *,
    strategy_family: str,
    universe: str,
    environment: str = "production",
    target_date: str | None = None,
    orchestrator_command: str | None = None,
    orchestrator_config_ref: str | None = None,
    cache_root: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    settings.require_database()
    return run_daily_production_inference(
        database_dsn=settings.database_dsn,
        strategy_family=strategy_family,
        universe=universe,
        environment=environment,
        target_date=target_date,
        orchestrator_command=orchestrator_command or os.environ.get("DATA_OPS_DAILY_INFERENCE_COMMAND"),
        orchestrator_config_ref=orchestrator_config_ref or os.environ.get("DATA_OPS_DAILY_INFERENCE_CONFIG_REF"),
        dry_run=bool(dry_run),
    )


if flow is not None:

    @flow(name="daily-production-inference", retries=0, log_prints=True)
    def daily_production_inference_flow(
        *,
        strategy_family: str,
        universe: str,
        environment: str = "production",
        target_date: str | None = None,
        orchestrator_command: str | None = None,
        orchestrator_config_ref: str | None = None,
        cache_root: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        return _run(
            strategy_family=strategy_family,
            universe=universe,
            environment=environment,
            target_date=target_date,
            orchestrator_command=orchestrator_command,
            orchestrator_config_ref=orchestrator_config_ref,
            cache_root=cache_root,
            dry_run=dry_run,
        )

else:

    def daily_production_inference_flow(**kwargs: Any) -> dict[str, Any]:
        return _run(**kwargs)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run daily production inference.")
    parser.add_argument("--strategy-family", required=True)
    parser.add_argument("--universe", required=True)
    parser.add_argument("--environment", default="production")
    parser.add_argument("--target-date", default=None)
    parser.add_argument("--orchestrator-command", default=None)
    parser.add_argument("--orchestrator-config-ref", default=None)
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = daily_production_inference_flow(
        strategy_family=args.strategy_family,
        universe=args.universe,
        environment=args.environment,
        target_date=args.target_date,
        orchestrator_command=args.orchestrator_command,
        orchestrator_config_ref=args.orchestrator_config_ref,
        cache_root=args.cache_root,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
