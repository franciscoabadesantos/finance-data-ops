#!/usr/bin/env python3
"""Submit a ticker onboarding run to Prefect deployment."""

from __future__ import annotations

import argparse
import json
import re
import sys

TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Submit ticker onboarding run to Prefect.")
    parser.add_argument("input_symbol", type=str, help="Ticker symbol (for example: AAPL).")
    parser.add_argument("--region", type=str, default="us", help="Region key (us, eu, apac).")
    parser.add_argument("--exchange", type=str, default=None, help="Optional exchange hint (for example: ASX).")
    parser.add_argument(
        "--instrument-type-hint",
        type=str,
        default=None,
        help="Optional instrument type hint.",
    )
    parser.add_argument("--start", type=str, default=None, help="Optional market backfill start YYYY-MM-DD.")
    parser.add_argument("--end", type=str, default=None, help="Optional market backfill end YYYY-MM-DD.")
    parser.add_argument("--history-limit", type=int, default=24, help="Optional earnings history limit.")
    parser.add_argument(
        "--deployment-name",
        type=str,
        default="dataops_ticker_onboarding/ticker-onboarding",
        help="Target onboarding deployment name.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=120, help="Wait timeout for run submission/result.")
    return parser


def _normalize_ticker(raw: str) -> str:
    ticker = str(raw).strip().upper()
    if not ticker:
        raise ValueError("input_symbol is required.")
    if not TICKER_PATTERN.fullmatch(ticker):
        raise ValueError("Invalid ticker format.")
    return ticker


def _optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    return cleaned


def main() -> None:
    args = _build_parser().parse_args()
    ticker = _normalize_ticker(args.input_symbol)
    region = str(args.region or "us").strip().lower() or "us"
    exchange = _optional(args.exchange)
    instrument_type_hint = _optional(args.instrument_type_hint)
    start = _optional(args.start)
    end = _optional(args.end)

    try:
        from prefect.deployments import run_deployment
    except Exception as exc:  # pragma: no cover - optional dependency boundary
        raise RuntimeError("Prefect is required. Install with: pip install -e '.[orchestration]'") from exc

    kwargs = {
        "parameters": {
            "input_symbol": ticker,
            "region": region,
            "exchange": exchange,
            "instrument_type_hint": instrument_type_hint,
            "start": start,
            "end": end,
            "history_limit": int(args.history_limit),
        },
        "timeout": float(args.timeout_seconds),
        "poll_interval": 5,
        "flow_run_name": f"submit-{ticker.lower()}",
    }
    run = run_deployment(str(args.deployment_name).strip(), **kwargs)

    output = {
        "submitted": True,
        "deployment": str(args.deployment_name).strip(),
        "input_symbol": ticker,
        "region": region,
        "flow_run_id": str(getattr(run, "id", "")),
        "state": str(getattr(run, "state_name", "")),
    }
    sys.stdout.write(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
