"""
Snowflake Data Quality DAG
Runs SQL checks directly in Snowflake using SnowflakeOperator.
Demonstrates Airflow-native Snowflake integration.
"""
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "def_framework",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="snowflake_data_quality",
    default_args=default_args,
    description="Run data quality checks directly in Snowflake",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "data-quality"],
) as dag:

    # Task 1: Count rows in all RAW tables
    check_raw_counts = SnowflakeOperator(
        task_id="check_raw_counts",
        snowflake_conn_id="snowflake_default",
        sql="""
            SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS row_count FROM DEF_INSURANCE.RAW.CUSTOMERS
            UNION ALL
            SELECT 'POLICIES', COUNT(*) FROM DEF_INSURANCE.RAW.POLICIES
            UNION ALL
            SELECT 'INCIDENTS', COUNT(*) FROM DEF_INSURANCE.RAW.INCIDENTS
            UNION ALL
            SELECT 'CLAIMS', COUNT(*) FROM DEF_INSURANCE.RAW.CLAIMS;
        """,
    )

    # Task 2: Check for NULL primary keys
    check_null_keys = SnowflakeOperator(
        task_id="check_null_keys",
        snowflake_conn_id="snowflake_default",
        sql="""
            SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS null_keys FROM DEF_INSURANCE.RAW.CUSTOMERS WHERE CUSTOMER_ID IS NULL
            UNION ALL
            SELECT 'POLICIES', COUNT(*) FROM DEF_INSURANCE.RAW.POLICIES WHERE POLICY_ID IS NULL
            UNION ALL
            SELECT 'INCIDENTS', COUNT(*) FROM DEF_INSURANCE.RAW.INCIDENTS WHERE INCIDENT_ID IS NULL
            UNION ALL
            SELECT 'CLAIMS', COUNT(*) FROM DEF_INSURANCE.RAW.CLAIMS WHERE CLAIM_ID IS NULL;
        """,
    )

    # Task 3: Check RAW vs CURATED row counts match
    check_raw_vs_curated = SnowflakeOperator(
        task_id="check_raw_vs_curated",
        snowflake_conn_id="snowflake_default",
        sql="""
            SELECT
                r.table_name,
                r.raw_count,
                c.curated_count,
                CASE WHEN r.raw_count = c.curated_count THEN 'PASS' ELSE 'FAIL' END AS status
            FROM (
                SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS raw_count FROM DEF_INSURANCE.RAW.CUSTOMERS
                UNION ALL SELECT 'POLICIES', COUNT(*) FROM DEF_INSURANCE.RAW.POLICIES
                UNION ALL SELECT 'INCIDENTS', COUNT(*) FROM DEF_INSURANCE.RAW.INCIDENTS
                UNION ALL SELECT 'CLAIMS', COUNT(*) FROM DEF_INSURANCE.RAW.CLAIMS
            ) r
            JOIN (
                SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS curated_count FROM DEF_INSURANCE.CURATED.CUSTOMERS
                UNION ALL SELECT 'POLICIES', COUNT(*) FROM DEF_INSURANCE.CURATED.POLICIES
                UNION ALL SELECT 'INCIDENTS', COUNT(*) FROM DEF_INSURANCE.CURATED.INCIDENTS
                UNION ALL SELECT 'CLAIMS', COUNT(*) FROM DEF_INSURANCE.CURATED.CLAIMS
            ) c ON r.table_name = c.table_name;
        """,
    )

    # Task 4: Log completion
    check_fraud_distribution = SnowflakeOperator(
        task_id="check_fraud_distribution",
        snowflake_conn_id="snowflake_default",
        sql="""
            SELECT
                FRAUD_REPORTED,
                COUNT(*) AS claim_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
            FROM DEF_INSURANCE.CURATED.CLAIMS
            GROUP BY FRAUD_REPORTED;
        """,
    )

    check_raw_counts >> check_null_keys >> check_raw_vs_curated >> check_fraud_distribution
