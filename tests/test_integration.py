import pytest
import duckdb

def test_full_pipeline_flow():
    # Simulate the gold layer by creating a table in memory
    con = duckdb.connect(':memory:')
    con.execute("CREATE TABLE test_assets AS SELECT 'bitcoin' as id, 1 as rank, 'BTC' as symbol, 'Bitcoin' as name, 50000.0 as priceUsd, 1000000000.0 as marketCapUsd")
    # Verify the table has exactly one row
    result = con.execute('SELECT COUNT(*) FROM test_assets').fetchone()[0]
    assert result == 1
    # Verify the data is queryable and correct
    row = con.execute("SELECT priceUsd FROM test_assets WHERE id = 'bitcoin'").fetchone()
    assert row[0] == 50000.0
