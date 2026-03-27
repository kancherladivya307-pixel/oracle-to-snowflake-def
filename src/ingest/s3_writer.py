"""
DEF Framework - S3 Writer
Uploads local CSV files to S3 landing zone.
"""
import boto3
from src.core.config import S3_BUCKET, S3_LANDING_PREFIX
from src.core.logger import get_logger

log = get_logger("s3_writer")

def upload_to_s3(local_file, s3_file_name):
    """Upload a local file to S3 landing zone."""
    s3_key = f"{S3_LANDING_PREFIX}{s3_file_name}"
    log.info(f"Uploading {local_file} → s3://{S3_BUCKET}/{s3_key}")

    s3 = boto3.client('s3')
    s3.upload_file(local_file, S3_BUCKET, s3_key)

    log.info(f"  Upload complete: s3://{S3_BUCKET}/{s3_key}")
    return f"s3://{S3_BUCKET}/{s3_key}"

def move_to_processed(s3_file_name):
    """Move a file from landing/ to processed/ after successful load."""
    s3 = boto3.resource('s3')
    source_key = f"{S3_LANDING_PREFIX}{s3_file_name}"
    target_key = f"processed/{s3_file_name}"

    s3.Object(S3_BUCKET, target_key).copy_from(
        CopySource=f"{S3_BUCKET}/{source_key}"
    )
    s3.Object(S3_BUCKET, source_key).delete()

    log.info(f"  Moved {source_key} → {target_key}")
