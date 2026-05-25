"""
load.py — Gold Layer: Analytical Aggregations
=============================================
Reads the cleaned Silver Parquet files from MinIO and materialises three
analytical tables inside DuckDB that power Metabase dashboards.

Gold tables created
-------------------
gold_asset_rankings      — all assets ordered by market-cap rank (ascending)
gold_market_summary      — one-row aggregate: totals and averages for the day
gold_price_aggregations  — all assets ordered by 24h trading volume (descending)

Idempotency: CREATE OR REPLACE TABLE replaces the tables on every run, so
retries never create duplicate rows.
"""

from datetime import date

from db import SILVER_BUCKET, get_duckdb_conn, logger

# SQL templates — keeps execute() calls short and readable
_SQL_RANKINGS = """
    CREATE OR REPLACE TABLE gold_asset_rankings AS
    SELECT id, rank, symbol, name, priceUsd, marketCapUsd
    FROM   read_parquet('s3://{bucket}/assets/{date}/*.parquet')
    ORDER  BY rank ASC;
"""

_SQL_SUMMARY = """
    CREATE OR REPLACE TABLE gold_market_summary AS
    SELECT
        COUNT(*)                  AS total_assets,
        SUM(marketCapUsd)         AS total_market_cap,
        SUM(volumeUsd24Hr)        AS total_volume_24h,
        AVG(changePercent24Hr)    AS avg_change_24h
    FROM read_parquet('s3://{bucket}/assets/{date}/*.parquet');
"""

_SQL_PRICE_AGG = """
    CREATE OR REPLACE TABLE gold_price_aggregations AS
    SELECT id, symbol, name, priceUsd, marketCapUsd, volumeUsd24Hr, changePercent24Hr
    FROM   read_parquet('s3://{bucket}/assets/{date}/*.parquet')
    ORDER  BY volumeUsd24Hr DESC;
"""

GOLD_TABLES = {
    'gold_asset_rankings':     _SQL_RANKINGS,
    'gold_market_summary':     _SQL_SUMMARY,
    'gold_price_aggregations': _SQL_PRICE_AGG,
}


def load_gold_tables() -> None:
    """
    Materialise Gold aggregation tables in DuckDB from today's Silver Parquet.

    Raises
    ------
    duckdb.Error
        If any SQL statement fails (connection is always closed via finally).
    """
    today = date.today().isoformat()
    con   = get_duckdb_conn()

    try:
        for table_name, sql_template in GOLD_TABLES.items():
            sql = sql_template.format(bucket=SILVER_BUCKET, date=today)
            logger.info(f'Creating {table_name}...')
            con.execute(sql)
            row_count = con.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]
            logger.info(f'{table_name} created with {row_count} rows')
    finally:
        con.close()


if __name__ == '__main__':
    load_gold_tables()
