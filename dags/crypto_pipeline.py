"""
crypto_pipeline.py — Main Airflow DAG
=======================================
Orchestrates a daily Medallion ETL pipeline for cryptocurrency market data.

Pipeline stages (in order)
--------------------------
extract_assets       → Pull raw JSON from CoinCap API → Bronze (MinIO)
check_bronze_quality → Validate raw record count and required fields
transform_assets     → PySpark: cast types, drop nulls → Silver (Parquet, MinIO)
check_silver_quality → Validate Silver row count and null prices
load_gold_tables     → DuckDB: materialise analytical aggregations → Gold
check_gold_quality   → Verify all Gold tables exist and are populated

Schedule: @daily (UTC midnight)
Start:    2026-05-15
Retries:  2 per task (with default 5-minute retry delay)
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make the include/scripts package importable inside the Airflow container
sys.path.insert(0, '/usr/local/airflow/include/scripts')

from extract import extract_assets
from load import load_gold_tables
from quality_checks import check_bronze_quality, check_gold_quality, check_silver_quality
from transform import transform_assets

# ---------------------------------------------------------------------------
# Default arguments applied to every task in the DAG
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    'retries':           2,
    'retry_delay':       timedelta(minutes=5),
    'email_on_failure':  False,
    'email_on_retry':    False,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id='crypto_pipeline',
    description='Daily Medallion ETL: CoinCap API → Bronze → Silver → Gold → Metabase',
    start_date=datetime(2026, 5, 15),
    schedule='@daily',
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['crypto', 'etl', 'medallion'],
) as dag:

    # --- Bronze --------------------------------------------------------------
    extract_task = PythonOperator(
        task_id='extract_assets',
        python_callable=extract_assets,
    )
    check_bronze_task = PythonOperator(
        task_id='check_bronze_quality',
        python_callable=check_bronze_quality,
    )

    # --- Silver --------------------------------------------------------------
    transform_task = PythonOperator(
        task_id='transform_assets',
        python_callable=transform_assets,
    )
    check_silver_task = PythonOperator(
        task_id='check_silver_quality',
        python_callable=check_silver_quality,
    )

    # --- Gold ----------------------------------------------------------------
    load_task = PythonOperator(
        task_id='load_gold_tables',
        python_callable=load_gold_tables,
    )
    check_gold_task = PythonOperator(
        task_id='check_gold_quality',
        python_callable=check_gold_quality,
    )

    # --- Dependency chain ----------------------------------------------------
    (
        extract_task
        >> check_bronze_task
        >> transform_task
        >> check_silver_task
        >> load_task
        >> check_gold_task
    )
