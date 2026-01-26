# oracle-to-snowflake-def
Enterprise-style metadata-driven Oracle to Snowflake pipeline on AWS using DEF framework

# Oracle to Snowflake DEF Framework (Apache Airflow Orchestrated)

## Overview

This project demonstrates an **enterprise-style, metadata-driven data engineering platform**
built on **AWS**, sourcing data from **Oracle (legacy system)** and delivering curated data
into **Snowflake**.

The pipeline follows a **DEF (Data Engineering Framework)** pattern. 
The DEF framework, shell scripts, and Python wrappers in this project are
independently implemented, following common enterprise data engineering
patterns and best practices and have used **Apache Airflow** as the orchestration layer.

---

## High-Level Architecture

**Source Systems**
- Oracle (legacy insurance data)

**Orchestration**
- Apache Airflow (DAG-based scheduling, dependencies, retries)

**Processing Layer**
- AWS EC2 (Application Server)
- Python-based DEF framework
- Shell scripts invoked by Airflow tasks

**Storage**
- AWS S3 (Data Lake – landing & curated zones)

**Target**
- Snowflake (RAW and CURATED schemas)
- Metadata-driven control tables

**Monitoring**
- AWS CloudWatch (logs and execution visibility)

---

## Key Concepts Demonstrated
- Metadata-driven ingestion (no hardcoded table logic)
- Batch lifecycle management (Create Batch → Ingest → Curate → Close Batch)
- Apache Airflow DAG-based orchestration (AutoSys-style design)
- Oracle to Snowflake data movement via AWS S3
- Enterprise-style logging and auditing

---

## Project Structure
```text
oracle-to-snowflake-def/
├── docs/
│   ├── step-01-ec2/
│   ├── step-02-oracle/
│   ├── step-03-s3/
│   ├── step-04-snowflake/
│   ├── step-05-def-framework/
│   └── step-06-airflow/
├── src/
│   └── def_framework/
├── scripts/
├── metadata/
└── README.md
