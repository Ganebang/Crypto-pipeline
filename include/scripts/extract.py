"""
extract.py — Bronze Layer: Raw Ingestion
=========================================
Calls the CoinCap API v3 and stores the raw JSON response in MinIO under
the Bronze bucket, partitioned by today's date.

Partition path: bronze/assets/YYYY-MM-DD/assets.json

Idempotency: put_object overwrites the key if the task is retried on the
same day — no duplicate records.
"""

import json
import requests
from datetime import date

from db import BRONZE_BUCKET, COINCAP_API_KEY, get_s3_client, logger

# CoinCap v3 assets endpoint
COINCAP_URL = 'https://rest.coincap.io/v3/assets'


def extract_assets() -> str:
    """
    Fetch cryptocurrency asset data from CoinCap and upload it to MinIO.

    Returns
    -------
    str
        The S3 object key where the raw data was stored.

    Raises
    ------
    requests.HTTPError
        If the API returns a non-2xx response.
    """
    today = date.today().isoformat()
    key   = f'assets/{today}/assets.json'

    # --- 1. Fetch from CoinCap API -------------------------------------------
    logger.info('Fetching asset data from CoinCap API...')
    response = requests.get(
        COINCAP_URL,
        headers={'Authorization': f'Bearer {COINCAP_API_KEY}'},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    record_count = len(data.get('data', data))
    logger.info(f'Received {record_count} records from CoinCap API')

    # --- 2. Upload to Bronze bucket -------------------------------------------
    s3 = get_s3_client()

    # Ensure required MinIO buckets exist (self-healing architecture)
    for bucket in [BRONZE_BUCKET, 'silver']:
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            logger.info(f"Bucket '{bucket}' not found. Creating it...")
            s3.create_bucket(Bucket=bucket)

    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=json.dumps(data))
    logger.info(f'Raw data uploaded to s3://{BRONZE_BUCKET}/{key}')

    return key


if __name__ == '__main__':
    extract_assets()
