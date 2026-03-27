"""
DEF Framework - Controller
Main pipeline driver. Orchestrates the full flow:
Oracle extract -> S3 upload -> Snowflake load -> Curate
"""
import sys
sys.path.insert(0, "/home/ec2-user/def-platform/oracle-to-snowflake-def")

from src.core.logger import get_logger
from src.core.batch_manager import BatchManager
from src.core.metadata import get_active_tables, get_table_config
from src.ingest.oracle_reader import extract_table
from src.ingest.s3_writer import upload_to_s3, move_to_processed
from src.curate.snowflake_loader import load_to_raw, curate_table

log = get_logger("controller")

def run_pipeline():
    """Run the full DEF pipeline for all active tables."""
    batch = BatchManager()
    batch_id = batch.open_batch()

    tables = get_active_tables()
    log.info(f"Processing {len(tables)} tables: {tables}")

    for table_name in tables:
        config = get_table_config(table_name)
        try:
            log.info(f"--- Processing {table_name} ---")

            # Step 1: Extract from Oracle
            local_file, row_count = extract_table(
                table_name=config["source_table"],
                source_schema=config["source_schema"]
            )

            # Step 2: Upload to S3
            s3_path = upload_to_s3(local_file, config["s3_file"])

            # Step 3: Load into Snowflake RAW
            rows_loaded = load_to_raw(config["target_table"], config["s3_file"])

            # Step 4: Curate (RAW to CURATED)
            rows_curated = curate_table(config["target_table"])

            # Step 5: Move S3 file to processed
            move_to_processed(config["s3_file"])

            batch.log_table_result(table_name, "SUCCESS", rows=row_count)

        except Exception as e:
            log.error(f"FAILED processing {table_name}: {str(e)}")
            batch.log_table_result(table_name, "FAILED", message=str(e))

    result = batch.close_batch()
    return result

if __name__ == "__main__":
    run_pipeline()
