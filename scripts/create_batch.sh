#!/bin/bash
echo "=== DEF Pipeline: Create Batch ==="
cd /home/ec2-user/def-platform/oracle-to-snowflake-def
source /home/ec2-user/def-platform/venv/bin/activate
python3 -c "
import sys
sys.path.insert(0, '.')
from src.core.batch_manager import BatchManager
batch = BatchManager()
batch_id = batch.open_batch()
print(f'BATCH_ID={batch_id}')
"
echo "=== Batch Created ==="
