from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_backfill_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_foreign_symbol_backfill.py"
    spec = importlib.util.spec_from_file_location("run_foreign_symbol_backfill", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_replaced_bare_entity_pairs_only_marks_confirmed_replacements() -> None:
    module = _load_backfill_module()

    assert module._replaced_bare_entity_pairs(["600900", "600900.SS", "700", "0700.HK", "1234"]) == [
        ("600900", "600900.SS"),
        ("700", "0700.HK"),
    ]
