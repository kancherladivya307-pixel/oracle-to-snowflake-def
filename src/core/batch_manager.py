"""
DEF Framework - Batch Manager
Opens and closes batch runs, tracks pipeline execution status.
"""
import uuid
from datetime import datetime
from src.core.logger import get_logger

log = get_logger("batch_manager")

class BatchManager:
    def __init__(self):
        self.batch_id = None
        self.start_time = None
        self.status = None
        self.table_results = {}

    def open_batch(self):
        """Start a new batch run."""
        self.batch_id = datetime.now().strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:6]
        self.start_time = datetime.now()
        self.status = "RUNNING"
        log.info(f"=== BATCH OPENED: {self.batch_id} ===")
        return self.batch_id

    def log_table_result(self, table_name, status, rows=0, message=""):
        """Record the result of processing one table."""
        self.table_results[table_name] = {
            "status": status,
            "rows": rows,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        log.info(f"  {table_name}: {status} ({rows} rows) {message}")

    def close_batch(self):
        """Close the batch and log summary."""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        failed = [t for t, r in self.table_results.items() if r["status"] == "FAILED"]
        self.status = "FAILED" if failed else "SUCCESS"

        log.info(f"=== BATCH CLOSED: {self.batch_id} ===")
        log.info(f"  Status:   {self.status}")
        log.info(f"  Duration: {duration:.1f} seconds")
        log.info(f"  Tables:   {len(self.table_results)} processed")
        if failed:
            log.info(f"  Failed:   {', '.join(failed)}")

        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "duration": duration,
            "tables": self.table_results
        }
