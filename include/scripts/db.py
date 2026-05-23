import os
import boto3
import duckdb
import logging

# Centralized Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
)
logger = logging.getLogger('crypto_pipeline')

# Centralized Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
DUCKDB_PATH = os.getenv('DUCKDB_PATH', '/usr/local/airflow/include/warehouse/crypto.duckdb')
COINCAP_API_KEY = os.getenv('COINCAP_API_KEY')
BRONZE_BUCKET = 'bronze'

def get_s3_client():
    """Instantiate a pre-configured boto3 S3 client for MinIO access."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

def get_duckdb_conn():
    """Establish and configure a DuckDB connection with S3/httpfs settings."""
    con = duckdb.connect(DUCKDB_PATH)
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute(
        f"SET s3_endpoint='{MINIO_ENDPOINT.replace('http://', '')}';"
        f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';"
        f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';"
        f"SET s3_use_ssl=false;"
        f"SET s3_url_style='path';"
    )
    return con
