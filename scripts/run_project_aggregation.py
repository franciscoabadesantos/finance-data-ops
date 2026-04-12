#!/usr/bin/env python3
"""Script wrapper for repository aggregation outputs."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.ops.consolidation import main


if __name__ == "__main__":
    raise SystemExit(main())
