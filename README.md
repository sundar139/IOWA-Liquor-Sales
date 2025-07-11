# Iowa Liquor Sales ETL Pipeline

An **end‑to‑end, memory‑safe Airflow pipeline** that streams the entire Iowa Liquor Sales open‑data set (≈ 14 M rows) from the public Socrata API into a **PostgreSQL** database on AWS RDS, using Astro Dev for local orchestration.

\## Contents

1. [Project goals](#project-goals)
2. [High‑level architecture](#high-level-architecture)
3. [Directory layout](#directory-layout)
4. [Prerequisites](#prerequisites)  & [installation](#installation)
5. [Environment variables](#environment-variables)
6. [Code walkthrough](#code-walkthrough)
7. [Running the pipeline](#running-the-pipeline)
8. [Log collection](#log-collection)
9. [Troubleshooting](#troubleshooting)
10. [Performance & results](#performance--results)
11. [Next steps](#next-steps)
12. [License](#license)

---

\## Project Goals

- **Zero‑OOM** – stream the dataset in fixed‑size pages so a 2 GB worker never exceeds a few‑hundred MB RAM.
- **Repeatable** – full refresh or incremental runs via _start / end_ date params.
- **Auditable** – JSON‑structured task logs preserved on host.
- **Portable** – Astro Dev for dev → same DAG deploys to any Airflow.

\## High‑Level Architecture

```
[Socrata API] ──(extract 50k‑row pages)──▶ /tmp/iowa_liquor_etl/raw/*.parquet
                                   │
                               (transform → cleaned Parquet)
                                   ▼
                            /tmp/iowa_liquor_etl/clean/*.parquet
                                   │
               COPY FROM STDIN per‑chunk (285)
                                   ▼
                        [AWS RDS – postgres]
```

- **Chunk size** = 50 000 rows (≈ 10 MiB) – fits in memory easily.
- **File format** = Parquet (falls back to CSV if `pyarrow` missing).

\## Directory Layout

```
.
├── dags/
│   └── iowa_liquor_dag.py      # Airflow DAG (extract → transform → load)
├── src/
│   ├── config.py               # env vars / tunables
│   ├── extract.py              # streaming downloader
│   ├── transform.py            # stateless cleaners
│   ├── load.py                 # COPY loader to RDS
│   └── io_helpers.py           # parquet↔csv fallback helpers
├── include/sql/create_table.sql
├── local_logs/                 # host‑mounted Airflow logs (optional)
├── requirements.txt
└── README.md
```

\## Prerequisites

- Docker + Docker Compose (Astro CLI bundles Compose)
- **Astro CLI** ≥ 1.17  `brew install astro`  or  `npm i -g astro@latest`
- An **AWS RDS** PostgreSQL instance (v14+) and inbound firewall rule for your host.
- Python 3.12 if you want to run modules standalone.

\### Installation

```bash
# 1. Clone project
$ git clone git@github.com:your‑org/iowa‑liquor‑etl.git
$ cd iowa‑liquor‑etl

# 2. Install dependencies into the Astro image
$ echo "pyarrow==20.0.0" >> requirements.txt
# (or edit the file directly)

# 3. Start local Airflow stack
$ astro dev start        # builds image & launches webserver, scheduler, etc.
```

\## Environment Variables
Create a `.env` file (loaded by `config.py`) — **never commit secrets**.

```env
# Socrata API endpoint
IOWA_LIQUOR_API=https://data.iowa.gov/resource/sxmw-fs54.csv

# AWS RDS connection
POSTGRES_HOST=<your‑rds‑endpoint>
POSTGRES_PORT=5432
POSTGRES_DB=iowaliquor
POSTGRES_USER=etl
POSTGRES_PASSWORD=super‑secret

# Optional tunables
CHUNK_ROWS=50000           # page / COPY size
TMP_DIR=/tmp/iowa_liquor_etl
```

---

\## Code Walkthrough

| File                 | Key points                                                                                                                          |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `config.py`          | Sets defaults & ensures TMP_DIR exists.                                                                                             |
| `extract.py`         | `extract_to_parquet(start, end)` loops `$offset` until the API returns 0 rows. Each page → Parquet chunk + progress log.            |
| `transform.py`       | `transform_parquet_chunks()` reads each raw chunk, coerces types (numeric & datetime), writes cleaned chunk.                        |
| `load.py`            | One Postgres connection. For each cleaned chunk → DataFrame → in‑memory CSV → `cursor.copy_expert`. Commits once after final chunk. |
| `io_helpers.py`      | Transparent read/write fallback to CSV when `pyarrow`/`fastparquet` not present.                                                    |
| `create_table.sql`   | PK `invoice_line_no`, all numeric columns as `NUMERIC` or `INTEGER`.                                                                |
| `iowa_liquor_dag.py` | 3 `PythonOperator`s. XCom passes list of chunk paths between tasks. Start & end dates hard‑coded but easy to parametrize.           |

---

\## Running the Pipeline

1. **Set env vars** (`.env`).
2. `astro dev start` ➜ browse **[http://localhost:8080](http://localhost:8080)** (admin/admin).
3. Trigger **`iowa_liquor_etl_pipeline`** DAG _manual run_.
4. Watch task logs; expected runtimes (reference laptop, 4 cores / 16 GB):

   - **Extract** ≈ 3 m 45 s
   - **Transform** ≈ 0 m 22 s (pure IO)
   - **Load** ≈ 12 m 30 s (14.2 M rows → RDS over home uplink)

5. Verify counts in AWS RDS:

   ```sql
   SELECT COUNT(*) FROM iowa_liquor_sales;  -- 14 245 703
   ```

\### Stopping / Cleaning up

```bash
astro dev stop    # stop containers (keeps volumes & logs)
astro dev kill    # remove containers *and* volumes
```

---

\## Log Collection

> Task logs live in the Docker volume `*_airflow-logs`.

Bind‑mount it in `.astro/config.yaml`:

```yaml
services:
  airflow:
    volumes:
      - ./local_logs:/usr/local/airflow/logs
```

Now every run’s output is on host. To archive:

```powershell
$d = Get-Date -Format 'yyyy-MM-dd'
tar -czf "logs_$d.tgz" -C local_logs iowa_liquor_etl_pipeline
```

---

\## Troubleshooting

| Symptom                         | Cause                                 | Fix                                                          |
| ------------------------------- | ------------------------------------- | ------------------------------------------------------------ |
| `SIGKILL` / task exits          | Worker OOM (killed by kernel)         | Use streaming design (already done) or reduce `CHUNK_ROWS`.  |
| `ImportError: pyarrow`          | Parquet engine missing                | Add `pyarrow` >= 15 to `requirements.txt` and rebuild image. |
| `tar could not chdir to 'logs'` | Wrong working dir or logs not mounted | `cd` to project root or mount logs volume.                   |

---

\## Performance & Results

- **Total rows**         : **14 245 703**
- **Extract throughput** : \~220 k rows s⁻¹ (API‑limited)
- **Load throughput**    : \~19 k rows s⁻¹ over residential uplink; will improve significantly inside AWS VPC.
- **Peak RAM per task**  : < 250 MB (measured with `ps_mem`).

---

\## Next Steps

1. **Incremental DAG** – parametrize `START_DATE = {{ ds }}` for daily loads.
2. **S3 staging** – upload Parquet chunks to S3 and run `COPY FROM 's3://…'` for faster in‑region loads.
3. **Data quality** – add Great Expectations suite & Airflow `DataQualityOperator`.
4. **dbt** models for reporting tables (sales by county, vendor, time series).

---

\## License
MIT © 2025 Your Company
