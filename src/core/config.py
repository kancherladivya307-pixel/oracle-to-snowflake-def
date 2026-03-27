"""
DEF Framework - Configuration
All connection details and paths in one place.
"""

# --- Oracle (source system) ---
ORACLE_USER = "system"
ORACLE_PASSWORD = "DefProject123"
ORACLE_DSN = "localhost:1521/XEPDB1"

# --- S3 (data lake) ---
S3_BUCKET = "def-insurance-data-lake-divya"
S3_LANDING_PREFIX = "landing/"
S3_PROCESSED_PREFIX = "processed/"
S3_LOGS_PREFIX = "logs/"

# --- Snowflake (warehouse) ---
SNOWFLAKE_ACCOUNT = "FPQOIJP-SF97945"
SNOWFLAKE_USER = "Divya307"
SNOWFLAKE_PASSWORD = "PLACEHOLDER"
SNOWFLAKE_DATABASE = "DEF_INSURANCE"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_RAW_SCHEMA = "RAW"
SNOWFLAKE_CURATED_SCHEMA = "CURATED"
SNOWFLAKE_STAGE = "def_s3_stage"

# --- Tables to process (metadata-driven) ---
SOURCE_TABLES = ["CUSTOMERS", "POLICIES", "INCIDENTS", "CLAIMS"]
