from __future__ import annotations

import pandas as pd

from finance_data_ops.refresh.storage import read_parquet_table, table_path, write_parquet_table


def test_write_parquet_table_roundtrip_leaves_no_temp_files(tmp_path) -> None:
    frame = pd.DataFrame({"ticker": ["AAA", "BBB"], "value": [1, 2]})
    write_parquet_table("source_cache.market_price_daily", frame, cache_root=str(tmp_path))

    out = read_parquet_table("source_cache.market_price_daily", cache_root=str(tmp_path))
    assert list(out["ticker"]) == ["AAA", "BBB"]

    # Atomic write must not leave partial/temp files behind (the source of the concurrent-read corruption).
    leftovers = [p.name for p in table_path("source_cache.market_price_daily", cache_root=str(tmp_path)).parent.iterdir()]
    assert leftovers == ["source_cache.market_price_daily.parquet"]


def test_write_parquet_table_replaces_existing_atomically(tmp_path) -> None:
    write_parquet_table("t", pd.DataFrame({"a": [1]}), cache_root=str(tmp_path))
    write_parquet_table("t", pd.DataFrame({"a": [9, 9]}), cache_root=str(tmp_path), mode="replace")
    out = read_parquet_table("t", cache_root=str(tmp_path))
    assert list(out["a"]) == [9, 9]
    assert not any(p.name.endswith(".tmp") for p in tmp_path.iterdir())
