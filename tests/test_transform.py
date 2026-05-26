"""
test_transform.py — Unit tests for the Silver transform layer
=============================================================
Verifies cleaning, casting, null-filtering, and simplified dropDuplicates deduplication
in PySpark. Runs completely offline by mocking read and write operations.
"""

import sys
import os
import json
import tempfile
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, '/usr/local/airflow/include/scripts')
from transform import transform_assets


def test_transform_assets_applies_deduplication():
    """
    Test that transform_assets successfully flattens, casts, and
    deduplicates overlapping records (using dropDuplicates).
    """
    # Mock data with duplicate records for 'bitcoin'
    mock_raw_data = [
        # First record (should be kept by dropDuplicates)
        {
            'id': 'bitcoin',
            'rank': '2',
            'symbol': 'BTC',
            'name': 'Bitcoin',
            'priceUsd': '45000.00',
            'marketCapUsd': '900000000.00',
            'volumeUsd24Hr': '400000.00',
            'changePercent24Hr': '0.5',
            'fetched_at': 1000000000,
        },
        # Second duplicate record (should be dropped)
        {
            'id': 'bitcoin',
            'rank': '1',
            'symbol': 'BTC',
            'name': 'Bitcoin',
            'priceUsd': '50000.00',
            'marketCapUsd': '1000000000.00',
            'volumeUsd24Hr': '500000.00',
            'changePercent24Hr': '1.5',
            'fetched_at': 2000000000,
        },
        # Unique record
        {
            'id': 'ethereum',
            'rank': '2',
            'symbol': 'ETH',
            'name': 'Ethereum',
            'priceUsd': '3000.00',
            'marketCapUsd': '300000000.00',
            'volumeUsd24Hr': '200000.00',
            'changePercent24Hr': '2.0',
            'fetched_at': 1500000000,
        }
    ]

    # Write mock data to a local temporary JSON file
    temp_fd, temp_path = tempfile.mkstemp(suffix='.json')
    try:
        with os.fdopen(temp_fd, 'w') as f:
            json.dump({'data': mock_raw_data}, f)

        with (
            patch('transform._build_spark_session') as mock_build_spark,
            patch('transform.date') as mock_date,
        ):
            mock_date.today.return_value.isoformat.return_value = '2026-05-25'
            
            # Set up a real local Spark Session for actual data manipulation
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.master('local[1]').appName('Test').getOrCreate()
            mock_build_spark.return_value = spark

            collected_rows = []
            
            # Intercept read.json to redirect to our local temp file
            original_json = spark.read.json
            def spy_json(path):
                return original_json(temp_path)

            # Spy on parquet write and eagerly collect rows before spark.stop() runs
            def spy_parquet(writer_self, path):
                nonlocal collected_rows
                collected_rows = writer_self._df.collect()
            
            # Patch both Spark read and Spark write methods
            with (
                patch('pyspark.sql.readwriter.DataFrameReader.json', side_effect=spy_json),
                patch('pyspark.sql.readwriter.DataFrameWriter.parquet', spy_parquet),
            ):
                transform_assets()

            # Validate the deduplication and schema casting
            assert len(collected_rows) == 2
            
            # Convert rows to a dictionary by id
            row_dict = {r['id']: r for r in collected_rows}
            
            assert 'bitcoin' in row_dict
            assert 'ethereum' in row_dict
            
            # Verify that the first duplicate record was kept (priceUsd = 45000.0, rank = 2)
            bitcoin_row = row_dict['bitcoin']
            assert bitcoin_row['priceUsd'] == 45000.0
            assert bitcoin_row['rank'] == 2
            
            # Verify schema casts
            ethereum_row = row_dict['ethereum']
            assert isinstance(ethereum_row['priceUsd'], float)
            assert isinstance(ethereum_row['rank'], int)
            
            spark.stop()
            
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
