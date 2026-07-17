"""Version-controlled identity curation inputs for Entity Layer publication."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_CURATED_IDENTITY_PATH = Path(__file__).resolve().parents[3] / "data" / "entity_identity_curated.json"


def load_curated_identity_inputs(path: str | Path | None = None) -> dict[str, list[dict[str, Any]]]:
    resolved = Path(path) if path else DEFAULT_CURATED_IDENTITY_PATH
    if not resolved.exists():
        return {"curated_identities": [], "reviewed_safe_heuristics": []}
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {"curated_identities": [], "reviewed_safe_heuristics": []}
    curated = payload.get("curated_identities")
    reviewed = payload.get("reviewed_safe_heuristics")
    return {
        "curated_identities": curated if isinstance(curated, list) else [],
        "reviewed_safe_heuristics": reviewed if isinstance(reviewed, list) else [],
    }
