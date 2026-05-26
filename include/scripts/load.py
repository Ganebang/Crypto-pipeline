"""
load.py — Gold Layer: Analytical Aggregations & SCD Type 2 Tracking
===================================================================
Reads the cleaned Silver Parquet files from MinIO and materialises analytical
tables inside DuckDB that power Metabase dashboards. Maintains a transactional
Slowly Changing Dimension (SCD) Type 2 table tracking asset price and rank history.
"""

import os
import shutil
from datetime import date, datetime
from db import DUCKDB_PATH, SILVER_BUCKET, get_duckdb_conn, logger


def load_gold_tables() -> None:
    """
    Materialise Gold aggregation tables in DuckDB from today's Silver Parquet.
    """
    today = date.today().isoformat()
    now   = datetime.utcnow().isoformat()
    
    # Staging path in the same directory as the production DuckDB file
    db_dir = os.path.dirname(DUCKDB_PATH)
    staging_path = os.path.join(db_dir, 'crypto_staging.duckdb')
    
    # 1. Copy the existing production DB to staging to preserve SCD history
    if os.path.exists(DUCKDB_PATH):
        logger.info(f"Copying production DB {DUCKDB_PATH} to staging {staging_path}...")
        shutil.copy2(DUCKDB_PATH, staging_path)
    
    logger.info(f"Connecting to staging DB at {staging_path}...")
    con   = get_duckdb_conn(staging_path)

    success = False
    try:
        logger.info('Creating gold_asset_rankings...')
        con.execute(
            f"CREATE OR REPLACE TABLE gold_asset_rankings AS "
            f"SELECT id, rank, symbol, name, priceUsd, marketCapUsd "
            f"FROM read_parquet('s3://{SILVER_BUCKET}/assets/{today}/*.parquet') "
            f"ORDER BY rank ASC;"
        )
        
        logger.info('Creating gold_market_summary...')
        con.execute(
            f"CREATE OR REPLACE TABLE gold_market_summary AS "
            f"SELECT COUNT(*) as total_assets, SUM(marketCapUsd) as total_market_cap, "
            f"SUM(volumeUsd24Hr) as total_volume_24h, AVG(changePercent24Hr) as avg_change_24h "
            f"FROM read_parquet('s3://{SILVER_BUCKET}/assets/{today}/*.parquet');"
        )
        
        logger.info('Creating gold_price_aggregations...')
        con.execute(
            f"CREATE OR REPLACE TABLE gold_price_aggregations AS "
            f"SELECT id, symbol, name, priceUsd, marketCapUsd, volumeUsd24Hr, changePercent24Hr "
            f"FROM read_parquet('s3://{SILVER_BUCKET}/assets/{today}/*.parquet') "
            f"ORDER BY volumeUsd24Hr DESC;"
        )
        
        logger.info('Updating gold_price_history SCD Type 2...')
        con.execute(
            "CREATE TABLE IF NOT EXISTS gold_price_history ("
            "id VARCHAR, symbol VARCHAR, name VARCHAR, priceUsd DOUBLE, rank BIGINT, "
            "valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN"
            ")"
        )
        
        # Expire old versions of records whose price or rank changed
        con.execute(
            f"UPDATE gold_price_history SET valid_to = '{now}', is_current = false "
            f"WHERE is_current = true AND id IN ("
            f"  SELECT n.id "
            f"  FROM read_parquet('s3://{SILVER_BUCKET}/assets/{today}/*.parquet') n "
            f"  JOIN gold_price_history h ON n.id = h.id "
            f"  WHERE h.is_current = true AND (n.priceUsd != h.priceUsd OR n.rank != h.rank)"
            f");"
        )
        
        # Insert completely new records and new active versions of changed records
        con.execute(
            f"INSERT INTO gold_price_history "
            f"SELECT n.id, n.symbol, n.name, n.priceUsd, n.rank, '{now}' as valid_from, "
            f"NULL as valid_to, true as is_current "
            f"FROM read_parquet('s3://{SILVER_BUCKET}/assets/{today}/*.parquet') n "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM gold_price_history h "
            f"  WHERE h.id = n.id AND h.is_current = true AND h.priceUsd = n.priceUsd AND h.rank = n.rank"
            f");"
        )
        
        success = True
        logger.info('Gold tables created/updated including SCD Type 2 gold_price_history')

    finally:
        con.close()
        if success:
            logger.info(f"Performing atomic swap: moving {staging_path} to {DUCKDB_PATH}...")
            os.replace(staging_path, DUCKDB_PATH)
            logger.info("Atomic database swap completed successfully!")


if __name__ == '__main__':
    load_gold_tables()
