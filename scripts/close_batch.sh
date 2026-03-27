#!/bin/bash
echo "=== DEF Pipeline: Close Batch ==="
cd /home/ec2-user/def-platform/oracle-to-snowflake-def
source /home/ec2-user/def-platform/venv/bin/activate
python3 -c "
import sys
sys.path.insert(0, '.')
from src.ingest.s3_writer import move_to_processed
from src.core.metadata import get_active_tables, get_table_config

for table_name in get_active_tables():
    config = get_table_config(table_name)
    move_to_processed(config['s3_file'])
    print(f'{table_name}: moved to processed')
print('=== Batch Closed ===')
"
