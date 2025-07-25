# Iowa Liquor Sales ETL Pipeline

A fully‑automated, streaming Extract–Transform–Load (ETL) workflow that ingests the **Iowa Liquor Sales** open dataset (14 M+ rows, \~6 GB), cleans it chunk‑by‑chunk, and loads it efficiently into PostgreSQL using Airflow orchestration.

## Contents

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Project layout](#project-layout)
- [Configuration](#configuration)
- [Module walk‑through](#module-walk-through)
- [Schema Extension for Analytical Star Schema](#schema-extension-for-analytical-star-schema)
- [Troubleshooting](#troubleshooting)
- [Performance notes](#performance-notes)

## Architecture

```mermaid
graph TD
    %% ETL pipeline
    A[Socrata API<br/>IOWA_LIQUOR_API] --|streaming CSV|--> B{extract.py<br/>50,000‑row pages}
    B --|Raw Parquet chunks|--> C[TMP_DIR/raw/chunk_*.parquet]
    C --> D[transform.py<br/>_clean_chunk]
    D --|Clean Parquet chunks|--> E[TMP_DIR/clean/chunk_*.parquet]
    E --> F[load.py<br/>COPY to PG]
    F --|14.2 M rows|--> G[(PostgreSQL)]

    %% Airflow DAG
    subgraph "Airflow DAG"
        A1[extract task] --> A2[transform task] --> A3[load task]
    end
```

**Key design points**

| Concern            | Approach                                                                          |
| ------------------ | --------------------------------------------------------------------------------- |
| _Scalability_      | Stream‐fetch via `$offset` paging; never holds the full dataset in memory.        |
| _Memory footprint_ | Processes fixed‑size chunks (default = 50 000).                                   |
| _Throughput_       | Uses `pandas.to_parquet()` + `pyarrow` for columnar I/O; `COPY` for PG bulk load. |
| _Reproducibility_  | Deterministic chunks; all parameters in a single `.env`/`config.py`.              |
| _Observability_    | Airflow logs + per‑stage progress counters (rows / chunks).                       |

## Quick start

### 1. Clone & install

```bash
git clone https://github.com/your‑org/iowa‑liquor‑etl.git
cd iowa‑liquor‑etl
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` → `.env` and fill in credentials:

```bash
# Socrata API token (optional but avoids throttling)
IOWA_LIQUOR_API=https://data.iowa.gov/resource/s2nq‑tq4e.csv

# Postgres target (local or RDS)
POSTGRES_HOST=<your‑rds‑endpoint>
POSTGRES_PORT=5432
POSTGRES_DB=<your-db-name>
POSTGRES_USER=<your-db-username>
POSTGRES_PASSWORD=<password-for-db>

# Runtime tuning
CHUNK_ROWS=50000        # rows per page / parquet chunk
TMP_DIR=/tmp/iowa_liquor_etl
```

### 3. Run unit tests

| File                | Purpose                           |
| ------------------- | --------------------------------- |
| `test_extract.py`   | Smoke‑test Socrata fetch & schema |
| `test_transform.py` | Verify cleaning, null‑handling    |
| `test_load.py`      | Round‑trip into test PG instance  |

Run all with:

```bash
pytest -q
```

### 4. Execute the pipeline

#### a) **Ad‑hoc (local)**

```bash
# one‑shot from 2020‑01‑01 to 2025‑06‑30
python -m src.extract
python -m src.transform
python -m src.load
```

#### b) **Scheduled (Airflow)**

```bash
airflow standalone
airflow webserver ‑D & airflow scheduler ‑D

airflow dags list
airflow dags trigger iowa_liquor_etl_pipeline
```

Monitor via **Airflow UI** → _DAGs_ → `iowa_liquor_etl_pipeline`.

## Project layout

```
.
├── dags/                     # Airflow DAGs
│   └── iowa_liquor_dag.py
├── include/sql/              # DDL & SQL helpers
│   └── create_table.sql
├── src/                      # Pure‑Python ETL library (import‑friendly)
│   ├── config.py             # env var ingestion & tunables
│   ├── extract.py            # streaming downloader → Parquet
│   ├── transform.py          # per‑chunk cleaners
│   └── load.py               # COPY loader into PostgreSQL
├── tests/                    # Pytest unit tests
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── requirements.txt
└── README.md
```

## Configuration

All runtime knobs live in **`src/config.py`** and can be overridden via environment variables:

| Variable            | Default              | Description                    |
| ------------------- | -------------------- | ------------------------------ |
| `IOWA_LIQUOR_API`   | _none_ (required)    | Socrata CSV endpoint           |
| `POSTGRES_HOST`     | \<your‑rds‑endpoint> | Target PG                      |
| `POSTGRES_PORT`     | 5432                 | —                              |
| `POSTGRES_DB`       | <your-db-name>       | —                              |
| `POSTGRES_USER`     | <your-db-username>   | —                              |
| `POSTGRES_PASSWORD` | <password-for-db>    | —                              |
| `CHUNK_ROWS`        | 50 000               | Rows per extraction page       |
| `TMP_DIR`           | /tmp/iowa_liquor_etl | Staging dir for Parquet chunks |

## Module walk‑through

| Stage           | File / Function(s)                        | Highlights                                                                                                  |
| --------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| **Extract**     | `src/extract.py` → `extract_to_parquet()` | • Streams pages with `$limit`+`$offset`.<br>• Writes `chunk_<n>.parquet` (columnar, compressed).            |
| **Transform**   | `src/transform.py` → `_clean_chunk()`     | • Enforces `datetime64[ns]` on `date` column.<br>• Performs numeric coercion & `NaN→0` for sales metrics.   |
| **Load**        | `src/load.py` → `copy_parquet_chunks()`   | • Streams each chunk through an in‑memory CSV buffer.<br>• Executes `COPY … FROM STDIN` (≈1.2 M rows/min).  |
| **Orchestrate** | `dags/iowa_liquor_dag.py`                 | • Three `PythonOperator`s wired `extract → transform → load`.<br>• No schedule by default (manual trigger). |

## Schema Extension for Analytical Star Schema

Here’s a high‑level narrative of how the analytical star schema was created and populated:

1. **Designed a Star Schema**: The single, wide sales table was decomposed into one **fact table** (recording each invoice‑line’s numeric sales measures) and five **dimension tables** (storing descriptive attributes for Store, Date, Item, Vendor, and Category). This arrangement supports flexible slicing and dicing of sales data by any combination of dimensions, following industry‑standard data warehousing practices.

2. **Created Dimension Tables**: Each business entity—stores, calendar dates, liquor items, vendors, and categories—was given its own table. The primary key in each dimension is the original code or date. Only the necessary descriptive attributes were retained (for example, store code plus name, address, city, zip, county; or date plus year, quarter, month, day‑of‑week, and weekend flag).

3. **Created the Fact Table**: The fact table uses the original invoice‑line number as its primary key and includes foreign‑key columns referencing each dimension. Alongside these keys are the quantitative measures (bottles sold, sales dollars, liters, and gallons) for each transaction line. Foreign‑key constraints ensure referential integrity between facts and dimensions.

4. **Upserted Dimension Data**: To load each dimension from the raw table, every distinct key‑attribute combination was selected. Null keys were filtered out to satisfy NOT NULL constraints. An “upsert” pattern (`INSERT ... ON CONFLICT DO NOTHING`) was used so that new dimension rows could be added safely without failing on duplicates, allowing the process to be re‑run as new codes appear over time.

5. **Loaded the Fact Table**: With dimensions in place, all invoice lines were loaded into the fact table. Timestamps were truncated to dates to match the date dimension. The invoice‑line ID was ensured non‑null, and the same `ON CONFLICT DO NOTHING` approach prevented duplicate loads, making the process idempotent for incremental updates.

6. **Resulting Structure**:

   - Dimension tables now hold clean, deduplicated master lists of stores, dates, items, vendors, and categories.
   - The fact table links these dimensions at the transaction level, storing all numeric sales measures.
   - Referential integrity is enforced via foreign‑key constraints.
   - The upsert approach makes the pipeline safe to run repeatedly without manual de‑duplication.

## Troubleshooting

| Symptom                | Cause                         | Fix                  |
| ---------------------- | ----------------------------- | -------------------- |
| `SIGKILL` / task exits | Worker OOM (killed by kernel) | Reduce `CHUNK_ROWS`. |

## Performance notes

- **Chunk size** (`CHUNK_ROWS`) – 50 000 rows was empirically the sweet spot between API latency & PG COPY buffer size.
- **I/O** – Parquet/Arrow yields \~4× smaller on‑disk size vs CSV and avoids parser cost on reload.
- **Parallelism** – The Airflow tasks are serial by design to keep memory bounded, but the pipeline is embarrassingly parallel if you shard the date range.
