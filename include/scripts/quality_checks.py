"""
quality_checks.py — Data Quality Gates
=======================================
One validation function per Medallion layer.  Each function is wired as a
separate Airflow task that sits between ETL steps and raises AirflowException
on failure — causing the DAG run to fail fast with a clear error message.

Bronze checks  → MinIO (boto3) — raw JSON record count & required fields
Silver checks  → DuckDB + httpfs — Parquet row count & null price validation
Gold checks    → DuckDB          — table existence & row count per Gold table
"""

import json
from datetime import date

from airflow.exceptions import AirflowException

from db import BRONZE_BUCKET, SILVER_BUCKET, get_duckdb_conn, get_s3_client, logger

# Thresholds
MIN_RECORD_COUNT = 10
REQUIRED_BRONZE_FIELDS = ('id', 'priceUsd')
GOLD_TABLES = ['gold_asset_rankings', 'gold_market_summary', 'gold_price_aggregations']


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

def check_bronze_quality() -> None:
    """
    Validate raw JSON data in the Bronze bucket.

    Checks
    ------
    1. Record count >= MIN_RECORD_COUNT
    2. First 5 records each contain 'id' and 'priceUsd'
    """
    today = date.today().isoformat()
    key   = f'assets/{today}/assets.json'

    s3   = get_s3_client()
    obj  = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    data = json.loads(obj['Body'].read())
    records = data.get('data', data)

    # Check 1 — minimum row count
    if len(records) < MIN_RECORD_COUNT:
        raise AirflowException(
            f'Bronze quality check failed: only {len(records)} records found '
            f'(minimum {MIN_RECORD_COUNT} required)'
        )

    # Check 2 — required fields present in sample
    for record in records[:5]:
        for field in REQUIRED_BRONZE_FIELDS:
            if not record.get(field):
                raise AirflowException(
                    f"Bronze quality check failed: field '{field}' missing or empty"
                )

    logger.info(f'Bronze quality check passed: {len(records)} records validated')


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

def check_silver_quality() -> None:
    """
    Validate cleaned Parquet data in the Silver bucket via DuckDB + httpfs.

    Checks
    ------
    1. Row count >= MIN_RECORD_COUNT
    2. Zero rows with NULL priceUsd
    """
    today       = date.today().isoformat()
    parquet_uri = f's3://{SILVER_BUCKET}/assets/{today}/*.parquet'
    con         = get_duckdb_conn()

    try:
        # Check 1 — minimum row count
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_uri}')").fetchone()[0]
        if count < MIN_RECORD_COUNT:
            raise AirflowException(
                f'Silver quality check failed: only {count} records found '
                f'(minimum {MIN_RECORD_COUNT} required)'
            )

        # Check 2 — no null prices
        null_prices = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_uri}') WHERE priceUsd IS NULL"
        ).fetchone()[0]
        if null_prices > 0:
            raise AirflowException(
                f'Silver quality check failed: {null_prices} records have NULL priceUsd'
            )

        logger.info(f'Silver quality check passed: {count} valid records')

    finally:
        con.close()


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

def check_gold_quality() -> None:
    """
    Verify that all Gold tables exist in DuckDB and are populated.

    Checks (per table)
    ------------------
    1. Table exists in information_schema
    2. Table has at least one row
    """
    con = get_duckdb_conn()

    try:
        for table in GOLD_TABLES:
            # Check 1 — table exists
            exists = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_name = '{table}'"
            ).fetchone()[0]
            if not exists:
                raise AirflowException(f'Gold quality check failed: table {table} does not exist')

            # Check 2 — table has rows
            row_count = con.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
            if row_count == 0:
                raise AirflowException(f'Gold quality check failed: table {table} is empty')

            logger.info(f'{table}: {row_count} rows — OK')

        logger.info('Gold quality check passed: all tables exist and are populated')

    finally:
        con.close()
