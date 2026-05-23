from datetime import date
from db import get_duckdb_conn, logger

def load_gold_tables():
    today = date.today().isoformat()

    # Connect to the pre-configured DuckDB database file
    con = get_duckdb_conn()

    # Gold table 1: Asset rankings ordered by market cap rank
    con.execute(f"CREATE OR REPLACE TABLE gold_asset_rankings AS SELECT id, rank, symbol, name, priceUsd, marketCapUsd FROM read_parquet('s3://silver/assets/{today}/*.parquet') ORDER BY rank ASC;")

    # Gold table 2: Aggregated market summary stats
    con.execute(f"CREATE OR REPLACE TABLE gold_market_summary AS SELECT COUNT(*) as total_assets, SUM(marketCapUsd) as total_market_cap, SUM(volumeUsd24Hr) as total_volume_24h, AVG(changePercent24Hr) as avg_change_24h FROM read_parquet('s3://silver/assets/{today}/*.parquet');")

    # Gold table 3: Price aggregations ordered by 24h volume
    con.execute(f"CREATE OR REPLACE TABLE gold_price_aggregations AS SELECT id, symbol, name, priceUsd, marketCapUsd, volumeUsd24Hr, changePercent24Hr FROM read_parquet('s3://silver/assets/{today}/*.parquet') ORDER BY volumeUsd24Hr DESC;")

    logger.info('Gold tables created: gold_asset_rankings, gold_market_summary, gold_price_aggregations')
    con.close()

if __name__ == '__main__':
    load_gold_tables()
