"""
Stream-extract Iowa Liquor Sales → Parquet chunk files.
"""

from __future__ import annotations

import gzip
from io import StringIO
from pathlib import Path
from typing import Iterator, List

import pandas as pd
import requests

from src.config import IOWA_LIQUOR_API, CHUNK_ROWS, TMP_DIR


def _fetch_page(start: str, end: str, offset: int) -> pd.DataFrame:
    params = {
        "$select": "*",
        "$where": f"date BETWEEN '{start}T00:00:00' AND '{end}T23:59:59'",
        "$limit":  CHUNK_ROWS,
        "$offset": offset,
    }
    r = requests.get(IOWA_LIQUOR_API, params=params, timeout=60)
    r.raise_for_status()
    return pd.read_csv(
        StringIO(r.text),
        low_memory=False,
        parse_dates=["date"],
    )


def extract_to_parquet(start: str,
                       end: str,
                       dest_dir: Path = TMP_DIR / "raw") -> List[Path]:
    """
    Pull data page-by-page and write each page to Parquet.
    Returns the list of file paths created.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []

    offset = 0
    page_no = 0
    while True:
        df = _fetch_page(start, end, offset)
        if df.empty:
            break

        path = dest_dir / f"chunk_{page_no:05d}.parquet"
        df.to_parquet(path, index=False)
        paths.append(path)

        print(f"saved {len(df):>6,} rows → {path.name}")
        offset += len(df)
        page_no += 1

    print(f"✔ extracted {offset:,} rows in {len(paths)} chunks")
    return paths
