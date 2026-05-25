"""
test_integration.py — Integration tests for the Gold layer
===========================================================
Uses an in-memory DuckDB database to simulate the full Gold table creation
and querying flow without requiring a running MinIO or on-disk DuckDB file.
"""

import duckdb
import pytest


@pytest.fixture
def gold_db():
    """Provide a fresh in-memory DuckDB connection for each test."""
    con = duckdb.connect(':memory:')
    yield con
    con.close()


def test_gold_table_creation_and_query(gold_db):
    """
    Simulate Gold layer: create a table, assert row count, and verify data.
    """
    gold_db.execute("""
        CREATE TABLE test_assets AS
        SELECT
            'bitcoin'       AS id,
            1               AS rank,
            'BTC'           AS symbol,
            'Bitcoin'       AS name,
            50000.0         AS priceUsd,
            1000000000.0    AS marketCapUsd
    """)

    # Row count assertion
    count = gold_db.execute('SELECT COUNT(*) FROM test_assets').fetchone()[0]
    assert count == 1, f'Expected 1 row, got {count}'

    # Data integrity assertion
    row = gold_db.execute("SELECT priceUsd, rank FROM test_assets WHERE id = 'bitcoin'").fetchone()
    assert row[0] == 50000.0, f'Expected priceUsd=50000.0, got {row[0]}'
    assert row[1] == 1,       f'Expected rank=1, got {row[1]}'


def test_gold_table_is_empty_without_data(gold_db):
    """An empty table should return a row count of zero."""
    gold_db.execute("""
        CREATE TABLE empty_assets (
            id VARCHAR, rank INTEGER, priceUsd DOUBLE
        )
    """)
    count = gold_db.execute('SELECT COUNT(*) FROM empty_assets').fetchone()[0]
    assert count == 0
