# Oracle → S3 → Snowflake DEF Framework

A metadata-driven data engineering pipeline I built to demonstrate enterprise-style ETL patterns. The pipeline extracts insurance claims data from Oracle, stages it in S3, and loads it into Snowflake — orchestrated by Apache Airflow.

---

## Why I Built This

In enterprise data platforms, pipelines aren't one-off scripts. They're driven by metadata tables, tracked with batch IDs, and orchestrated by schedulers. I wanted to build something that reflects that — not just a "read CSV, write to warehouse" tutorial, but a framework that could realistically scale to dozens of tables with zero code changes.

---

## How It Works

The data flows through four stages:

**Oracle (source)** → **S3 (landing zone)** → **Snowflake RAW (as-is copy)** → **Snowflake CURATED (transformed)**

Airflow triggers the pipeline by calling shell scripts. The shell scripts call into the Python DEF framework. Airflow never touches business logic — it's purely a traffic controller.

The pipeline processes four normalized tables per run: CUSTOMERS, POLICIES, INCIDENTS, and CLAIMS. Adding a new table means adding one entry to `metadata.py` — no other code changes needed.

---

## Tech Stack

- **Oracle XE 21c** — runs in Docker on EC2, acts as the legacy source system
- **Python 3.9** — custom DEF framework using oracledb, boto3, snowflake-connector-python
- **AWS S3** — data lake with landing/, processed/, and logs/ zones
- **Snowflake** — cloud warehouse with RAW and CURATED schemas
- **Apache Airflow 2.8.4** — orchestrates the pipeline via BashOperators
- **AWS EC2** (Amazon Linux 2023) — hosts everything except Snowflake
- **Docker** — containerizes Oracle XE

---

## Dataset

I used the [Auto Insurance Claims dataset from Kaggle](https://www.kaggle.com/datasets/buntyshah/auto-insurance-claims-data) — 1,000 records across 39 columns covering P&C insurance: customer demographics, policy details, incident reports, and claim amounts.

The original flat CSV is normalized into four Oracle tables with proper foreign key relationships, simulating what you'd actually find in a production OLTP system.

---

## Framework Structure

The Python framework is split into three layers, similar to how enterprise data teams organize their codebase:

```
src/
├── core/                    
│   ├── config.py            # Connection strings, S3 paths, table list
│   ├── logger.py            # Writes to console + daily log files
│   ├── metadata.py          # Defines which tables to process and how
│   ├── batch_manager.py     # Opens/closes batches, tracks per-table status
│   └── controller.py        # Main driver — loops through tables, runs the pipeline
├── ingest/                  
│   ├── oracle_reader.py     # Connects to Oracle, extracts full table to CSV
│   └── s3_writer.py         # Uploads CSV to S3 landing/, moves to processed/ after load
└── curate/                  
    └── snowflake_loader.py  # Truncates + COPY INTO for RAW, CREATE OR REPLACE for CURATED
```

---

## Airflow DAG

The DAG runs four tasks in sequence:

`create_batch` → `ingest` → `curate` → `close_batch`

Each task calls a shell script in `scripts/`. The shell scripts activate the Python venv and call into the framework. This keeps the DAG file clean and puts all logic where it belongs — in Python.

Schedule is set to manual trigger (`schedule_interval=None`) since this is a demo project, but switching to `@daily` is a one-line change.

---

## S3 Layout

```
s3://def-insurance-data-lake-divya/
├── landing/      # Oracle extracts land here as CSVs
├── processed/    # Files get moved here after Snowflake loads them
└── logs/         # Pipeline logs
```

---

## Snowflake Layout

```
DEF_INSURANCE
├── RAW         # Direct copy of Oracle tables + LOAD_TIMESTAMP
└── CURATED     # Transformed tables + CURATED_TIMESTAMP
```

Both schemas contain the same four tables: CUSTOMERS, POLICIES, INCIDENTS, CLAIMS. The RAW layer is a full refresh (truncate + reload) every run. CURATED is rebuilt from RAW with a CREATE OR REPLACE, which can be extended with deduplication, data quality checks, or business transformations.

---

## Running the Pipeline

Start the services:
```bash
sudo systemctl start docker
docker start oracle-xe
cd ~/def-platform && source venv/bin/activate
export AIRFLOW_HOME=~/def-platform/airflow
airflow webserver --host 0.0.0.0 --port 8080 &
airflow scheduler &
```

Trigger the pipeline:
```bash
# Through Airflow
airflow dags trigger insurance_daily

# Or directly via Python
cd ~/def-platform/oracle-to-snowflake-def
python -m src.core.controller
```

---

## What I'd Add Next

- Incremental loads using watermark columns instead of full refresh
- Data quality checks between RAW and CURATED (row counts, null checks)
- Snowflake metadata tables (MD_SOURCE_SYSTEM, MD_TABLES, ETL_RUN_LOG) to replace the Python-side metadata
- Alerting on batch failures via Airflow email or SNS
- Parquet format for S3 instead of CSV for better performance
