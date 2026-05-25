"""
transform.py — Silver Layer: Cleaning & Schema Enforcement
===========================================================
Reads today's raw JSON from the Bronze bucket, applies type casting and
null-filtering via PySpark, and writes the result as columnar Parquet files
to the Silver bucket — partitioned by date.

Partition path: silver/assets/YYYY-MM-DD/*.parquet

Idempotency: write.mode('overwrite') replaces the date partition on each
run, so retries are safe.
"""

import os
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import DoubleType, LongType

from db import BRONZE_BUCKET, MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY, SILVER_BUCKET, logger

# Columns to retain in the Silver layer
SILVER_COLUMNS = ['id', 'rank', 'symbol', 'name', 'priceUsd', 'marketCapUsd', 'volumeUsd24Hr', 'changePercent24Hr']


def _build_spark_session() -> SparkSession:
    """
    Create a SparkSession wired to the local MinIO instance via the S3A
    connector. The driver is bound to localhost to prevent Docker networking
    issues with Spark's heartbeater.
    """
    # Bind Spark driver to loopback to avoid Docker heartbeater issues
    os.environ['SPARK_LOCAL_HOSTNAME'] = '127.0.0.1'

    return (
        SparkSession.builder
        .appName('CryptoTransform')
        .config('spark.jars.packages',            'org.apache.hadoop:hadoop-aws:3.4.2')
        .config('spark.driver.host',              '127.0.0.1')
        .config('spark.driver.bindAddress',       '127.0.0.1')
        .config('spark.hadoop.fs.s3a.endpoint',   MINIO_ENDPOINT)
        .config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY)
        .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY)
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .getOrCreate()
    )


def transform_assets() -> None:
    """
    Read Bronze JSON, apply schema enforcement, and write Silver Parquet.

    Steps
    -----
    1. Build a PySpark session connected to MinIO.
    2. Read the raw JSON file for today's date partition.
    3. Flatten the nested ``data`` array if present.
    4. Cast numeric fields and drop rows missing ``id`` or ``priceUsd``.
    5. Write overwrite-mode Parquet to the Silver bucket.
    """
    today = date.today().isoformat()
    bronze_path = f's3a://{BRONZE_BUCKET}/assets/{today}/assets.json'
    silver_path = f's3a://{SILVER_BUCKET}/assets/{today}/'

    spark = _build_spark_session()
    try:
        # --- 1. Read raw Bronze JSON -----------------------------------------
        logger.info(f'Reading Bronze data from {bronze_path}')
        df = spark.read.json(bronze_path)

        # --- 2. Flatten nested 'data' array if present -----------------------
        if 'data' in df.columns:
            assets_df = df.select(explode(col('data')).alias('asset')).select('asset.*')
        else:
            assets_df = df

        # --- 3. Cast & filter ------------------------------------------------
        clean_df = (
            assets_df.select(
                col('id'),
                col('rank').cast(LongType()),
                col('symbol'),
                col('name'),
                col('priceUsd').cast(DoubleType()),
                col('marketCapUsd').cast(DoubleType()),
                col('volumeUsd24Hr').cast(DoubleType()),
                col('changePercent24Hr').cast(DoubleType()),
            )
            .dropna(subset=['id', 'priceUsd'])
        )

        # --- 4. Write Silver Parquet -----------------------------------------
        clean_df.write.mode('overwrite').parquet(silver_path)
        logger.info(f'Silver Parquet written to {silver_path} ({clean_df.count()} rows)')

    finally:
        spark.stop()


if __name__ == '__main__':
    transform_assets()
