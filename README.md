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

To support advanced analytics, we’ve added a **star schema** alongside the raw table. It consists of five dimension tables and one fact table:

```sql
-- Dimension Tables
CREATE TABLE IF NOT EXISTS dim_store (
    store          TEXT PRIMARY KEY,
    name           TEXT,
    address        TEXT,
    city           TEXT,
    zipcode        TEXT,
    store_location TEXT,
    county_number  TEXT,
    county         TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date        DATE PRIMARY KEY,
    year        SMALLINT,
    quarter     SMALLINT,
    month       SMALLINT,
    day_of_week SMALLINT,
    is_weekend  BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_item (
    itemno             TEXT PRIMARY KEY,
    im_desc            TEXT,
    pack               INTEGER,
    bottle_volume_ml   INTEGER,
    state_bottle_cost  NUMERIC,
    state_bottle_retail NUMERIC
);

CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_no   TEXT PRIMARY KEY,
    vendor_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_category (
    category      TEXT PRIMARY KEY,
    category_name TEXT
);

-- Fact Table
CREATE TABLE IF NOT EXISTS fact_sales (
    invoice_line_no TEXT PRIMARY KEY,
    "date"          DATE,
    store           TEXT,
    itemno          TEXT,
    vendor_no       TEXT,
    category        TEXT,
    sale_bottles    INTEGER,
    sale_dollars    NUMERIC,
    sale_liters     NUMERIC,
    sale_gallons    NUMERIC,
    CONSTRAINT fk_store    FOREIGN KEY (store)   REFERENCES dim_store(store),
    CONSTRAINT fk_date     FOREIGN KEY ("date")  REFERENCES dim_date(date),
    CONSTRAINT fk_item     FOREIGN KEY (itemno)  REFERENCES dim_item(itemno),
    CONSTRAINT fk_vendor   FOREIGN KEY (vendor_no) REFERENCES dim_vendor(vendor_no),
    CONSTRAINT fk_category FOREIGN KEY (category) REFERENCES dim_category(category)
);
```

**Population Scripts** (using `ON CONFLICT DO NOTHING` to safely upsert):

```sql
-- dim_store
INSERT INTO dim_store (store,name,address,city,zipcode,store_location,county_number,county)
SELECT DISTINCT store,name,address,city,zipcode,store_location,county_number,county
FROM original_sales_table
WHERE store IS NOT NULL
ON CONFLICT (store) DO NOTHING;

-- dim_date
INSERT INTO dim_date (date,year,quarter,month,day_of_week,is_weekend)
SELECT DISTINCT (date_trunc('day',date))::DATE,EXTRACT(YEAR FROM date)::SMALLINT,
       EXTRACT(QUARTER FROM date)::SMALLINT,EXTRACT(MONTH FROM date)::SMALLINT,
       EXTRACT(DOW FROM date)::SMALLINT,(EXTRACT(DOW FROM date) IN (0,6))
FROM original_sales_table
WHERE date IS NOT NULL
ON CONFLICT (date) DO NOTHING;

-- dim_item
INSERT INTO dim_item (itemno,im_desc,pack,bottle_volume_ml,state_bottle_cost,state_bottle_retail)
SELECT DISTINCT itemno,im_desc,pack,bottle_volume_ml,state_bottle_cost,state_bottle_retail
FROM original_sales_table
WHERE itemno IS NOT NULL
ON CONFLICT (itemno) DO NOTHING;

-- dim_vendor
INSERT INTO dim_vendor (vendor_no,vendor_name)
SELECT DISTINCT vendor_no,vendor_name
FROM original_sales_table
WHERE vendor_no IS NOT NULL
ON CONFLICT (vendor_no) DO NOTHING;

-- dim_category
INSERT INTO dim_category (category,category_name)
SELECT DISTINCT category,category_name
FROM original_sales_table
WHERE category IS NOT NULL
ON CONFLICT (category) DO NOTHING;

-- fact_sales
INSERT INTO fact_sales (invoice_line_no,"date",store,itemno,vendor_no,category,sale_bottles,sale_dollars,sale_liters,sale_gallons)
SELECT invoice_line_no,(date_trunc('day',date))::DATE,store,itemno,vendor_no,category,
       sale_bottles,sale_dollars,sale_liters,sale_gallons
FROM original_sales_table
WHERE invoice_line_no IS NOT NULL
ON CONFLICT (invoice_line_no) DO NOTHING;
```

## Troubleshooting

| Symptom                | Cause                         | Fix                  |
| ---------------------- | ----------------------------- | -------------------- |
| `SIGKILL` / task exits | Worker OOM (killed by kernel) | Reduce `CHUNK_ROWS`. |

## Performance notes

- **Chunk size** (`CHUNK_ROWS`) – 50 000 rows was empirically the sweet spot between API latency & PG COPY buffer size.
- **I/O** – Parquet/Arrow yields \~4× smaller on‑disk size vs CSV and avoids parser cost on reload.
- **Parallelism** – The Airflow tasks are serial by design to keep memory bounded, but the pipeline is embarrassingly parallel if you shard the date range.
