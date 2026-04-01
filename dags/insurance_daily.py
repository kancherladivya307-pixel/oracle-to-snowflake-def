"""
DEF Insurance Pipeline DAG
Orchestrates the full Oracle -> S3 -> Snowflake pipeline.
Airflow acts as traffic controller only - calls shell scripts.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

SCRIPTS_DIR = "/home/ec2-user/def-platform/oracle-to-snowflake-def/scripts"

default_args = {
    "owner": "def_framework",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="insurance_daily",
    default_args=default_args,
    description="DEF Pipeline: Oracle -> S3 -> Snowflake (RAW -> CURATED)",
    schedule_interval='*/2 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["def", "insurance", "pipeline"],
) as dag:

    create_batch = BashOperator(
        task_id="create_batch",
        bash_command=f"bash {SCRIPTS_DIR}/create_batch.sh ",
    )

    ingest = BashOperator(
        task_id="ingest",
        bash_command=f"bash {SCRIPTS_DIR}/ingest.sh ",
    )

    curate = BashOperator(
        task_id="curate",
        bash_command=f"bash {SCRIPTS_DIR}/curate.sh ",
    )

    close_batch = BashOperator(
        task_id="close_batch",
        bash_command=f"bash {SCRIPTS_DIR}/close_batch.sh ",
    )

    create_batch >> ingest >> curate >> close_batch
