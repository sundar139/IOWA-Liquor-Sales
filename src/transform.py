"""
Stateless per-chunk transformation helpers.
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import List

def _clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # example numeric coercions – adapt as needed
    num_cols = [
        "pack", "bottle_volume_ml", "state_bottle_cost",
        "state_bottle_retail", "sale_bottles",
        "sale_dollars", "sale_liters", "sale_gallons",
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df


def transform_parquet_chunks(src_paths: List[Path],
                             dest_dir: Path) -> List[Path]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_paths: List[Path] = []

    for p in src_paths:
        df = pd.read_parquet(p)
        df_t = _clean_chunk(df)

        out = dest_dir / p.name           # keep chunk_<n>.parquet
        df_t.to_parquet(out, index=False)
        out_paths.append(out)

    print(f"✔ transformed {len(out_paths)} chunks")
    return out_paths
