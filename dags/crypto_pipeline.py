from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.insert(0, '/usr/local/airflow/include/scripts')
from extract import extract_assets
from transform import transform_assets
from load import load_gold_tables
from quality_checks import check_bronze_quality, check_silver_quality, check_gold_quality

with DAG(
    dag_id='crypto_pipeline',
    start_date=datetime(2026,5, 15),
    schedule='@daily',
    catchup=False,
    default_args={'retries': 2},
    tags=['crypto']
) as dag:
    extract_task = PythonOperator(task_id='extract_assets', python_callable=extract_assets)
    check_bronze_task = PythonOperator(task_id='check_bronze_quality', python_callable=check_bronze_quality)
    transform_task = PythonOperator(task_id='transform_assets', python_callable=transform_assets)
    check_silver_task = PythonOperator(task_id='check_silver_quality', python_callable=check_silver_quality)
    load_task = PythonOperator(task_id='load_gold_tables', python_callable=load_gold_tables)
    check_gold_task = PythonOperator(task_id='check_gold_quality', python_callable=check_gold_quality)

    extract_task >> check_bronze_task >> transform_task >> check_silver_task >> load_task >> check_gold_task

