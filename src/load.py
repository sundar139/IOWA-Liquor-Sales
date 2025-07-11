"""
COPY-load Parquet chunks into PostgreSQL in O(chunk_size) memory.
"""

from __future__ import annotations

import io
import time
from pathlib import Path
from typing import List

import pandas as pd
import psycopg2
from psycopg2.extensions import connection, cursor

from src.config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
    CHUNK_ROWS,
)

_DDL_FILE = Path("include/sql/create_table.sql")


def _get_conn() -> connection:
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def _copy_df(cur: cursor, df: pd.DataFrame, table: str) -> None:
    buf = io.BytesIO()
    df.to_csv(buf, index=False, header=False,
              sep=",", na_rep="\\N")
    buf.seek(0)
    cur.copy_expert(
        f"COPY {table} ({','.join(df.columns)}) "
        "FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '\\N')",
        file=buf,
    )


def copy_parquet_chunks(paths: List[Path],
                        table: str = "iowa_liquor_sales") -> None:
    total = 0
    t0 = time.perf_counter()

    with _get_conn() as conn, conn.cursor() as cur:
        # create table once
        cur.execute(_DDL_FILE.read_text())
        conn.commit()

        for i, path in enumerate(paths, 1):
            df = pd.read_parquet(path)
            _copy_df(cur, df, table)
            total += len(df)
            print(f"COPY {i}/{len(paths)}  +{len(df):,} rows  "
                  f"(cum {total:,})")

        conn.commit()

    print(f"✔ loaded {total:,} rows in {time.perf_counter()-t0:.1f}s")
