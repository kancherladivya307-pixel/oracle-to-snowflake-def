#!/bin/bash
echo "=== DEF Pipeline: Curate (S3 -> Snowflake RAW -> CURATED) ==="
cd /home/ec2-user/def-platform/oracle-to-snowflake-def
source /home/ec2-user/def-platform/venv/bin/activate
python3 -c "
import sys
sys.path.insert(0, '.')
from src.core.metadata import get_active_tables, get_table_config
from src.curate.snowflake_loader import load_to_raw, curate_table

for table_name in get_active_tables():
    config = get_table_config(table_name)
    rows_loaded = load_to_raw(config['target_table'], config['s3_file'])
    rows_curated = curate_table(config['target_table'])
    print(f'{table_name}: {rows_loaded} loaded to RAW, {rows_curated} curated')
"
echo "=== Curate Complete ==="
