"""
DEF Framework - Snowflake Loader
Loads data from S3 stage into Snowflake RAW tables,
then transforms RAW to CURATED.
"""
import snowflake.connector
from src.core.config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_RAW_SCHEMA, SNOWFLAKE_CURATED_SCHEMA, SNOWFLAKE_STAGE
)
from src.core.logger import get_logger

log = get_logger("snowflake_loader")

# Column lists for each table (excludes LOAD_TIMESTAMP)
TABLE_COLUMNS = {
    "CUSTOMERS": "CUSTOMER_ID, MONTHS_AS_CUSTOMER, AGE, INSURED_ZIP, INSURED_SEX, INSURED_EDUCATION, INSURED_OCCUPATION, INSURED_HOBBIES, INSURED_RELATIONSHIP, CAPITAL_GAINS, CAPITAL_LOSS",
    "POLICIES": "POLICY_ID, CUSTOMER_ID, POLICY_NUMBER, POLICY_BIND_DATE, POLICY_STATE, POLICY_CSL, POLICY_DEDUCTABLE, POLICY_ANNUAL_PREMIUM, UMBRELLA_LIMIT",
    "INCIDENTS": "INCIDENT_ID, POLICY_ID, INCIDENT_DATE, INCIDENT_TYPE, COLLISION_TYPE, INCIDENT_SEVERITY, AUTHORITIES_CONTACTED, INCIDENT_STATE, INCIDENT_CITY, INCIDENT_LOCATION, INCIDENT_HOUR, NUM_VEHICLES_INVOLVED, PROPERTY_DAMAGE, BODILY_INJURIES, WITNESSES, POLICE_REPORT_AVAILABLE",
    "CLAIMS": "CLAIM_ID, INCIDENT_ID, TOTAL_CLAIM_AMOUNT, INJURY_CLAIM, PROPERTY_CLAIM, VEHICLE_CLAIM, AUTO_MAKE, AUTO_MODEL, AUTO_YEAR, FRAUD_REPORTED"
}

def get_connection():
    """Create a Snowflake connection."""
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE
    )

def load_to_raw(table_name, s3_file_name):
    """Truncate RAW table and COPY INTO from S3 stage."""
    log.info(f"Loading {s3_file_name} into {SNOWFLAKE_RAW_SCHEMA}.{table_name}")
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"USE SCHEMA {SNOWFLAKE_RAW_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")
        log.info(f"  Truncated {table_name}")
        columns = TABLE_COLUMNS.get(table_name, "")
        copy_sql = f"""
            COPY INTO {table_name} ({columns})
            FROM @{SNOWFLAKE_STAGE}/{s3_file_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_sql)
        result = cur.fetchone()
        rows_loaded = result[3] if result else 0
        log.info(f"  Loaded {rows_loaded} rows into {SNOWFLAKE_RAW_SCHEMA}.{table_name}")
        return rows_loaded
    except Exception as e:
        log.error(f"  FAILED loading {table_name}: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

def curate_table(table_name):
    """Transform RAW to CURATED. Creates clean, analytics-ready tables."""
    log.info(f"Curating {SNOWFLAKE_RAW_SCHEMA}.{table_name} to {SNOWFLAKE_CURATED_SCHEMA}.{table_name}")
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"USE SCHEMA {SNOWFLAKE_CURATED_SCHEMA}")
        curate_sql = f"""
            CREATE OR REPLACE TABLE {SNOWFLAKE_CURATED_SCHEMA}.{table_name} AS
            SELECT *, CURRENT_TIMESTAMP() AS CURATED_TIMESTAMP
            FROM {SNOWFLAKE_RAW_SCHEMA}.{table_name}
        """
        cur.execute(curate_sql)
        cur.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_CURATED_SCHEMA}.{table_name}")
        row_count = cur.fetchone()[0]
        log.info(f"  Curated {row_count} rows into {SNOWFLAKE_CURATED_SCHEMA}.{table_name}")
        return row_count
    except Exception as e:
        log.error(f"  FAILED curating {table_name}: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()
