"""
DEF Framework - Oracle Reader
Extracts data from Oracle source tables into CSV files.
"""
import csv
import os
import oracledb
from src.core.config import ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN
from src.core.logger import get_logger

log = get_logger("oracle_reader")

def extract_table(table_name, source_schema, output_dir="/tmp/def_extracts"):
    """Extract a full table from Oracle and write to a local CSV file."""
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{table_name.lower()}.csv")

    log.info(f"Extracting {source_schema}.{table_name} from Oracle...")

    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {source_schema}.{table_name}")

    # Get column names
    columns = [col[0] for col in cur.description]

    # Write to CSV
    row_count = 0
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in cur:
            writer.writerow(row)
            row_count += 1

    cur.close()
    conn.close()

    log.info(f"  Extracted {row_count} rows → {output_file}")
    return output_file, row_count
