"""
test_load.py — Unit tests for the Gold SCD Type 2 loading layer
==============================================================
Validates that our SCD Type 2 merging logic inside DuckDB properly
initializes, tracks, and expires historical data over time. Runs
offline using an in-memory DuckDB connection.
"""

import sys
import pytest
import duckdb
from unittest.mock import MagicMock, patch

sys.path.insert(0, '/usr/local/airflow/include/scripts')
from load import load_gold_tables


def test_load_gold_tables_lifecycle():
    """
    Test the complete load lifecycle, ensuring general Gold tables are created
    and the SCD Type 2 gold_price_history table successfully expires changed records
    and tracks active token history.
    """
    # 1. Initialize an in-memory DuckDB connection
    con = duckdb.connect(':memory:')

    try:
        # Mock today's Silver data (Bitcoin and Ethereum initially loaded)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE today_silver AS
            SELECT 'bitcoin' AS id, 1::BIGINT AS rank, 'BTC' AS symbol, 'Bitcoin' AS name, 50000.0::DOUBLE AS priceUsd, 1000000000.0::DOUBLE AS marketCapUsd, 500000.0::DOUBLE AS volumeUsd24Hr, 1.5::DOUBLE AS changePercent24Hr
            UNION ALL
            SELECT 'ethereum' AS id, 2::BIGINT AS rank, 'ETH' AS symbol, 'Ethereum' AS name, 3000.0::DOUBLE AS priceUsd, 300000000.0::DOUBLE AS marketCapUsd, 200000.0::DOUBLE AS volumeUsd24Hr, 2.0::DOUBLE AS changePercent24Hr;
        """)

        # Mock tomorrow's Silver data (Bitcoin changes rank & price, Ethereum changes rank, Cardano is new)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE tomorrow_silver AS
            SELECT 'bitcoin' AS id, 2::BIGINT AS rank, 'BTC' AS symbol, 'Bitcoin' AS name, 55000.0::DOUBLE AS priceUsd, 1100000000.0::DOUBLE AS marketCapUsd, 600000.0::DOUBLE AS volumeUsd24Hr, 1.0::DOUBLE AS changePercent24Hr
            UNION ALL
            SELECT 'ethereum' AS id, 1::BIGINT AS rank, 'ETH' AS symbol, 'Ethereum' AS name, 3000.0::DOUBLE AS priceUsd, 300000000.0::DOUBLE AS marketCapUsd, 250000.0::DOUBLE AS volumeUsd24Hr, 2.5::DOUBLE AS changePercent24Hr
            UNION ALL
            SELECT 'cardano' AS id, 3::BIGINT AS rank, 'ADA' AS symbol, 'Cardano' AS name, 0.50::DOUBLE AS priceUsd, 15000000.0::DOUBLE AS marketCapUsd, 10000.0::DOUBLE AS volumeUsd24Hr, 0.5::DOUBLE AS changePercent24Hr;
        """)

        # Custom DuckDB execution wrapper to intercept s3:// paths and route to memory tables
        class DuckDBMockWrapper:
            def __init__(self, real_con):
                self.real_con = real_con
            def execute(self, sql, *args, **kwargs):
                # Replace the read_parquet URI with our mock temp tables depending on the dates
                if "read_parquet(" in sql:
                    if "2026-05-25" in sql:
                        sql = sql.replace("read_parquet('s3://silver/assets/2026-05-25/*.parquet')", "today_silver")
                    elif "2026-05-26" in sql:
                        sql = sql.replace("read_parquet('s3://silver/assets/2026-05-26/*.parquet')", "tomorrow_silver")
                return self.real_con.execute(sql, *args, **kwargs)
            def close(self):
                pass  # Keep in-memory DB alive for asserts in the test block

        mock_conn = DuckDBMockWrapper(con)

        # Patch dependencies to run completely offline
        with (
            patch('load.get_duckdb_conn', return_value=mock_conn),
            patch('load.logger'),
            patch('load.date') as mock_date,
            patch('load.datetime') as mock_datetime,
        ):
            # --- RUN 1: Initial load on 2026-05-25 ---
            mock_date.today.return_value.isoformat.return_value = '2026-05-25'
            mock_datetime.utcnow.return_value.isoformat.return_value = '2026-05-25T12:00:00'

            load_gold_tables()

            # Verify general Gold tables
            rankings = con.execute("SELECT * FROM gold_asset_rankings ORDER BY rank").fetchall()
            assert len(rankings) == 2
            assert rankings[0][0] == 'bitcoin'
            assert rankings[0][1] == 1
            assert rankings[1][0] == 'ethereum'
            assert rankings[1][1] == 2

            summary = con.execute("SELECT * FROM gold_market_summary").fetchone()
            assert summary[0] == 2 # total assets
            assert summary[1] == 1300000000.0 # total market cap

            # Verify initial SCD records
            history = con.execute("SELECT * FROM gold_price_history ORDER BY id").fetchall()
            assert len(history) == 2
            
            btc = [r for r in history if r[0] == 'bitcoin'][0]
            assert btc[3] == 50000.0  # priceUsd
            assert btc[4] == 1        # rank
            assert str(btc[5]) == '2026-05-25 12:00:00' # valid_from
            assert btc[6] is None     # valid_to
            assert btc[7] is True     # is_current

            # --- RUN 2: Next run on 2026-05-26 ---
            # Bitcoin changes price & rank. Ethereum changes rank. Cardano is introduced.
            mock_date.today.return_value.isoformat.return_value = '2026-05-26'
            mock_datetime.utcnow.return_value.isoformat.return_value = '2026-05-26T12:00:00'

            load_gold_tables()

            # Verify updated SCD state
            all_history = con.execute("SELECT * FROM gold_price_history ORDER BY id, valid_from").fetchall()
            
            # Total records should be 5:
            # - Bitcoin (expired old version + active new version) -> 2
            # - Ethereum (expired old version + active new version because rank changed) -> 2
            # - Cardano (active new version) -> 1
            assert len(all_history) == 5

            # Bitcoin history
            btc_records = [r for r in all_history if r[0] == 'bitcoin']
            assert len(btc_records) == 2
            
            # Expired version
            old_btc = btc_records[0]
            assert old_btc[3] == 50000.0
            assert old_btc[4] == 1
            assert str(old_btc[5]) == '2026-05-25 12:00:00'
            assert str(old_btc[6]) == '2026-05-26 12:00:00'
            assert old_btc[7] is False

            # Active version
            new_btc = btc_records[1]
            assert new_btc[3] == 55000.0
            assert new_btc[4] == 2
            assert str(new_btc[5]) == '2026-05-26 12:00:00'
            assert new_btc[6] is None
            assert new_btc[7] is True

            # Ethereum history (rank changed from 2 to 1)
            eth_records = [r for r in all_history if r[0] == 'ethereum']
            assert len(eth_records) == 2
            
            # Expired version
            old_eth = eth_records[0]
            assert old_eth[3] == 3000.0
            assert old_eth[4] == 2
            assert str(old_eth[6]) == '2026-05-26 12:00:00'
            assert old_eth[7] is False

            # Active version
            new_eth = eth_records[1]
            assert new_eth[3] == 3000.0
            assert new_eth[4] == 1
            assert str(new_eth[5]) == '2026-05-26 12:00:00'
            assert new_eth[6] is None
            assert new_eth[7] is True

            # Cardano history (new record)
            ada_records = [r for r in all_history if r[0] == 'cardano']
            assert len(ada_records) == 1
            assert ada_records[0][3] == 0.5
            assert ada_records[0][4] == 3
            assert str(ada_records[0][5]) == '2026-05-26 12:00:00'
            assert ada_records[0][6] is None
            assert ada_records[0][7] is True

    finally:
        con.close()
