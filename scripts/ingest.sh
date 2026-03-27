#!/bin/bash
echo "=== DEF Pipeline: Ingest (Oracle -> S3) ==="
cd /home/ec2-user/def-platform/oracle-to-snowflake-def
source /home/ec2-user/def-platform/venv/bin/activate
python3 -c "
import sys
sys.path.insert(0, '.')
from src.core.metadata import get_active_tables, get_table_config
from src.ingest.oracle_reader import extract_table
from src.ingest.s3_writer import upload_to_s3

for table_name in get_active_tables():
    config = get_table_config(table_name)
    local_file, row_count = extract_table(config['source_table'], config['source_schema'])
    upload_to_s3(local_file, config['s3_file'])
    print(f'{table_name}: {row_count} rows extracted and uploaded')
"
echo "=== Ingest Complete ==="
