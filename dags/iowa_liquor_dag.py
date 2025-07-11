from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from config   import CHUNK_ROWS, TMP_DIR
from extract  import extract_to_parquet
from transform import transform_parquet_chunks
from load     import copy_parquet_chunks

START_DATE = "2020-01-01"
END_DATE   = "2025-06-30"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

RAW_DIR      = TMP_DIR / "raw"
CLEAN_DIR    = TMP_DIR / "clean"

def extract_task(**_):
    """Page through Socrata and write raw Parquet chunks."""
    paths = extract_to_parquet(START_DATE, END_DATE, RAW_DIR)
    # return list of str, not Path (XCom must be JSON-serialisable)
    return [str(p) for p in paths]

def transform_task(ti, **_):
    raw_paths: List[str] = ti.xcom_pull(task_ids="extract")
    clean_paths = transform_parquet_chunks(
        [Path(p) for p in raw_paths], CLEAN_DIR
    )
    return [str(p) for p in clean_paths]

def load_task(ti, **_):
    clean_paths: List[str] = ti.xcom_pull(task_ids="transform")
    copy_parquet_chunks([Path(p) for p in clean_paths])

with DAG(
    dag_id="iowa_liquor_etl_pipeline",
    description="Stream ETL Iowa Liquor Sales → Postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["iowa", "liquor", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    extract >> transform >> load
