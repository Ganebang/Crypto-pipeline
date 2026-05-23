import json
from datetime import date
from airflow.exceptions import AirflowException
from db import get_s3_client, get_duckdb_conn, logger

def check_bronze_quality():
    today = date.today().isoformat()
    # Connect to MinIO and read today's raw JSON
    s3 = get_s3_client()
    obj = s3.get_object(Bucket='bronze', Key=f'assets/{today}/assets.json')
    data = json.loads(obj['Body'].read())
    records = data.get('data', data)
    # Check minimum row count threshold
    if len(records) < 10:
        raise AirflowException(f'Bronze quality check failed: only {len(records)} records found')
    # Check for required fields in a sample of records
    for record in records[:5]:
        if not record.get('id') or not record.get('priceUsd'):
            raise AirflowException('Bronze quality check failed: missing required fields')
    logger.info(f'Bronze quality check passed: {len(records)} records')

def check_silver_quality():
    today = date.today().isoformat()
    # Connect to DuckDB to query today's Silver Parquet files
    con = get_duckdb_conn()
    
    # Query to count total rows in silver parquet
    res = con.execute(f"SELECT COUNT(*) FROM read_parquet('s3://silver/assets/{today}/*.parquet');").fetchone()
    count = res[0]
    if count < 10:
        con.close()
        raise AirflowException(f'Silver quality check failed: only {count} records found in silver layer')
        
    # Check that prices are non-null
    null_prices = con.execute(f"SELECT COUNT(*) FROM read_parquet('s3://silver/assets/{today}/*.parquet') WHERE priceUsd IS NULL;").fetchone()[0]
    if null_prices > 0:
        con.close()
        raise AirflowException(f'Silver quality check failed: {null_prices} null price records found')
        
    logger.info(f'Silver quality check passed: {count} valid records in silver layer')
    con.close()

def check_gold_quality():
    # Connect to DuckDB database and verify gold tables exist and are populated
    con = get_duckdb_conn()
    
    tables = ['gold_asset_rankings', 'gold_market_summary', 'gold_price_aggregations']
    for table in tables:
        # Check if table exists
        exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table}';").fetchone()[0]
        if exists == 0:
            con.close()
            raise AirflowException(f'Gold quality check failed: Table {table} does not exist')
            
        # Check if table has rows
        count = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        if count == 0:
            con.close()
            raise AirflowException(f'Gold quality check failed: Table {table} is empty')
            
    logger.info('Gold quality check passed: all tables exist and are successfully populated')
    con.close()

