import os
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import DoubleType, LongType
from db import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, logger


def transform_assets():
    # Use today's date to locate the correct bronze partition
    today = date.today().isoformat()

    # Set local hostname to loopback to avoid heartbeater registration errors in Docker
    os.environ['SPARK_LOCAL_HOSTNAME'] = '127.0.0.1'

    # Create a Spark session configured to read/write MinIO via S3A
    spark = SparkSession.builder \
        .appName('CryptoTransform') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.4.2') \
        .config('spark.driver.host', '127.0.0.1') \
        .config('spark.driver.bindAddress', '127.0.0.1') \
        .config('spark.hadoop.fs.s3a.endpoint', MINIO_ENDPOINT) \
        .config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY) \
        .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY) \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .getOrCreate()




    # Read raw JSON from the bronze layer
    df = spark.read.json(f's3a://bronze/assets/{today}/assets.json')

    # Flatten nested 'data' array field if present in the API response
    if 'data' in df.columns:
        assets_df = df.select(explode(col('data')).alias('asset')).select('asset.*')
    else:
        assets_df = df


    # Select and cast columns to proper numeric types
    clean_df = assets_df.select(
        col('id'),
        col('rank').cast(LongType()),
        col('symbol'),
        col('name'),
        col('priceUsd').cast(DoubleType()),
        col('marketCapUsd').cast(DoubleType()),
        col('volumeUsd24Hr').cast(DoubleType()),
        col('changePercent24Hr').cast(DoubleType())
    ).dropna(subset=['id', 'priceUsd'])

    # Write cleaned data as Parquet to the silver layer
    clean_df.write.mode('overwrite').parquet(f's3a://silver/assets/{today}/')
    logger.info(f'Wrote Parquet to silver/assets/{today}/')
    spark.stop()

if __name__ == '__main__':
    transform_assets()
