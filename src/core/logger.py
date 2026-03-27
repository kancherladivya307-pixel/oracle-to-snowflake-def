"""
DEF Framework - Logger
Centralized logging for all pipeline steps.
"""
import logging
import os
from datetime import datetime

def get_logger(name):
    """Create a logger that writes to console and log file."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)

    # File handler
    log_dir = os.path.expanduser("~/def-platform/oracle-to-snowflake-def/logs")
    os.makedirs(log_dir, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    file_handler = logging.FileHandler(f"{log_dir}/def_pipeline_{today}.log")
    file_handler.setLevel(logging.INFO)

    # Format
    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
    console.setFormatter(fmt)
    file_handler.setFormatter(fmt)

    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger
