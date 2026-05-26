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
from datetime import date, datetime

from db import BRONZE_BUCKET, COINCAP_API_KEY, get_s3_client, logger

import time

# CoinCap v3 assets endpoint
COINCAP_URL = 'https://rest.coincap.io/v3/assets'


def last_run_timestamp(s3) -> int:
    """
    Retrieve the last run timestamp from S3 (Bronze bucket) using last_run.json.
    Supports parsing both epoch milliseconds and ISO-8601 datetime strings.
    """
    try:
        obj = s3.get_object(Bucket=BRONZE_BUCKET, Key='metadata/last_run.json')
        meta_data = json.loads(obj['Body'].read().decode('utf-8'))
        val = meta_data.get('last_run', 0)
        if isinstance(val, str):
            dt_str = val.replace('Z', '')
            dt = datetime.fromisoformat(dt_str)
            return int(dt.timestamp() * 1000)
        return int(val)
    except Exception:
        logger.info("No last run timestamp found in S3. Performing full extract.")
        return 0


def save_last_run_timestamp(s3, now) -> None:
    """
    Save the last run timestamp to S3 (Bronze bucket) as last_run.json.
    """
    try:
        meta_data = {'last_run': now}
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key='metadata/last_run.json',
            Body=json.dumps(meta_data).encode('utf-8')
        )
        logger.info(f"Saved last run timestamp: {now} to metadata/last_run.json on S3.")
    except Exception as e:
        logger.error(f"Failed to save last run timestamp to S3: {e}")


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

    # --- 1. Initialize S3 and Ensure Required Buckets Exist (Self-Healing) --
    s3 = get_s3_client()
    for bucket in [BRONZE_BUCKET, 'silver']:
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            logger.info(f"Bucket '{bucket}' not found. Creating it...")
            s3.create_bucket(Bucket=bucket)

    # --- 2. Retrieve last run timestamp and build params -------------------
    params = {}
    last_run = last_run_timestamp(s3)
    if last_run > 0:
        params['updateSince'] = last_run

    # --- 3. Fetch from CoinCap API (Fail Fast) -------------------------------
    logger.info(f'Fetching asset data from CoinCap API with params: {params}...')
    response = requests.get(
        COINCAP_URL,
        headers={'Authorization': f'Bearer {COINCAP_API_KEY}'},
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    new_records = data.get('data', data)
    record_count = len(new_records)
    logger.info(f'Received {record_count} records from CoinCap API')

    # --- 4. Inject fetch date for processing history --------------
    now = datetime.utcnow().isoformat()
    today = date.today().isoformat()
    key = f'assets/{today}/assets.json'
    
    # --- 5. Upload raw payload to Bronze bucket -----------------------------
    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=json.dumps(data))
    logger.info(f'Raw data uploaded to s3://{BRONZE_BUCKET}/{key}')

    # --- 6. Save current timestamp as the last run in S3 ---------------------
    save_last_run_timestamp(s3, now)

    return key



if __name__ == '__main__':
    extract_assets()
