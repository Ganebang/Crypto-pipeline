"""
db.py — Centralized Database, Storage & Logging Configuration
=============================================================
Single source of truth for all infrastructure connections in the pipeline.
Every other script imports from here instead of re-defining credentials.

Design decisions:
- Logging is configured once here so all modules share the same format.
- get_duckdb_conn() uses a context-aware string for the DuckDB path so it
  works both inside Airflow containers and in local unit-test environments.
- Credentials are read exclusively from environment variables; no hard-coded
  fallbacks with real secrets.
"""

import os
import logging
import boto3
import duckdb

# ---------------------------------------------------------------------------
# Logging — configure once, import `logger` in every other module
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('crypto_pipeline')

# ---------------------------------------------------------------------------
# Configuration — read exclusively from environment variables
# ---------------------------------------------------------------------------
MINIO_ENDPOINT: str  = os.environ['MINIO_ENDPOINT']
MINIO_ACCESS_KEY: str = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY: str = os.environ['MINIO_SECRET_KEY']
COINCAP_API_KEY: str  = os.environ['COINCAP_API_KEY']

DUCKDB_PATH: str = os.getenv(
    'DUCKDB_PATH',
    '/usr/local/airflow/include/warehouse/crypto.duckdb',
)

BRONZE_BUCKET: str = 'bronze'
SILVER_BUCKET: str = 'silver'


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------

def get_s3_client():
    """Return a pre-configured boto3 S3 client targeting the MinIO endpoint."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    """
    Return a DuckDB connection pre-configured with httpfs/S3 settings.

    The connection automatically installs and loads the httpfs extension so
    downstream queries can read Parquet files directly from MinIO via the
    s3:// URI scheme.
    """
    con = duckdb.connect(DUCKDB_PATH)
    # Install & load the HTTP filesystem extension (idempotent)
    con.execute('INSTALL httpfs; LOAD httpfs;')
    # Point DuckDB at our local MinIO instance
    s3_endpoint = MINIO_ENDPOINT.replace('http://', '').replace('https://', '')
    con.execute(f"""
        SET s3_endpoint      = '{s3_endpoint}';
        SET s3_access_key_id = '{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key = '{MINIO_SECRET_KEY}';
        SET s3_use_ssl       = false;
        SET s3_url_style     = 'path';
    """)
    return con
