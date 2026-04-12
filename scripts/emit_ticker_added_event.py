#!/usr/bin/env python3
"""Emit a Prefect custom event to trigger ticker backfill deployments."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime

TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit dataops.ticker.added event for Prefect deployment triggers.")
    parser.add_argument("ticker", type=str, help="Ticker symbol (for example: AAPL).")
    parser.add_argument(
        "--event-name",
        type=str,
        default="dataops.ticker.added",
        help="Custom event name expected by Prefect trigger.",
    )
    parser.add_argument("--region", type=str, default=None, help="Optional region hint (us, eu, apac).")
    parser.add_argument("--start", type=str, default=None, help="Optional market backfill start YYYY-MM-DD.")
    parser.add_argument("--end", type=str, default=None, help="Optional market backfill end YYYY-MM-DD.")
    parser.add_argument("--history-limit", type=int, default=None, help="Optional earnings history limit override.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    ticker = str(args.ticker).strip().upper()
    if not ticker:
        raise ValueError("ticker must be non-empty.")
    if not TICKER_PATTERN.fullmatch(ticker):
        raise ValueError(
            "Invalid ticker format. Expected plain uppercase symbol without prefixes (for example: AAPL, BRK.B)."
        )

    try:
        from prefect.events import emit_event
    except Exception as exc:  # pragma: no cover - optional dependency boundary
        raise RuntimeError("Prefect is required. Install with: pip install -e '.[orchestration]'") from exc

    payload: dict[str, object] = {
        "ticker": ticker,
        "requested_at": datetime.now(UTC).isoformat(),
        "region": (str(args.region).strip().lower() if args.region else None),
        "start": (str(args.start).strip() if args.start else None),
        "end": (str(args.end).strip() if args.end else None),
        "history_limit": (int(args.history_limit) if args.history_limit is not None else None),
    }

    event = emit_event(
        event=str(args.event_name).strip(),
        resource={
            "prefect.resource.id": ticker,
            "prefect.resource.role": "ticker",
            "dataops.resource.type": "ticker",
        },
        payload=payload,
    )
    output = {
        "emitted": True,
        "event_name": str(args.event_name).strip(),
        "ticker": ticker,
        "region": payload.get("region"),
        "event_id": str(getattr(event, "id", "")) if event is not None else "",
    }
    sys.stdout.write(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
