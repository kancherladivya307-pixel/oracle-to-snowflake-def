"""
DEF Framework - Metadata
Metadata-driven table configuration. Controls what gets extracted and loaded.
"""

TABLE_METADATA = {
    "CUSTOMERS": {
        "source_schema": "SYSTEM",
        "source_table": "CUSTOMERS",
        "primary_key": "CUSTOMER_ID",
        "s3_file": "customers.csv",
        "target_schema": "RAW",
        "target_table": "CUSTOMERS",
        "active": True
    },
    "POLICIES": {
        "source_schema": "SYSTEM",
        "source_table": "POLICIES",
        "primary_key": "POLICY_ID",
        "s3_file": "policies.csv",
        "target_schema": "RAW",
        "target_table": "POLICIES",
        "active": True
    },
    "INCIDENTS": {
        "source_schema": "SYSTEM",
        "source_table": "INCIDENTS",
        "primary_key": "INCIDENT_ID",
        "s3_file": "incidents.csv",
        "target_schema": "RAW",
        "target_table": "INCIDENTS",
        "active": True
    },
    "CLAIMS": {
        "source_schema": "SYSTEM",
        "source_table": "CLAIMS",
        "primary_key": "CLAIM_ID",
        "s3_file": "claims.csv",
        "target_schema": "RAW",
        "target_table": "CLAIMS",
        "active": True
    }
}

def get_active_tables():
    """Return list of table names where active=True."""
    return [name for name, meta in TABLE_METADATA.items() if meta["active"]]

def get_table_config(table_name):
    """Return metadata config for a specific table."""
    return TABLE_METADATA.get(table_name)
