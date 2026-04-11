"""Canonical cache I/O for Data Ops outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def table_path(table_name: str, *, cache_root: str | Path) -> Path:
    root = Path(cache_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{str(table_name).strip()}.parquet"


def read_parquet_table(
    table_name: str,
    *,
    cache_root: str | Path,
    required: bool = False,
) -> pd.DataFrame:
    path = table_path(table_name, cache_root=cache_root)
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Canonical table missing: {path}")
        return pd.DataFrame()
    return pd.read_parquet(path)


def write_parquet_table(
    table_name: str,
    frame: pd.DataFrame,
    *,
    cache_root: str | Path,
    mode: str = "replace",
    dedupe_subset: list[str] | None = None,
) -> Path:
    path = table_path(table_name, cache_root=cache_root)
    if str(mode).strip().lower() not in {"replace", "append"}:
        raise ValueError("mode must be one of: replace, append")

    out = frame.copy()
    if str(mode).strip().lower() == "append" and path.exists():
        existing = pd.read_parquet(path)
        out = pd.concat([existing, out], ignore_index=True)

    if dedupe_subset:
        dedupe_cols = [col for col in dedupe_subset if col in out.columns]
        if dedupe_cols:
            out = out.drop_duplicates(subset=dedupe_cols, keep="last")

    out.to_parquet(path, index=False)
    return path
