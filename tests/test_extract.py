"""
test_extract.py — Unit tests for the Bronze ingestion layer
============================================================
All external dependencies (HTTP, S3) are mocked so these tests run
completely offline without requiring a live MinIO or CoinCap account.
"""

import sys
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, '/usr/local/airflow/include/scripts')
from extract import extract_assets

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MOCK_API_PAYLOAD = {
    'data': [
        {
            'id':               'bitcoin',
            'rank':             '1',
            'symbol':           'BTC',
            'name':             'Bitcoin',
            'priceUsd':         '50000.00',
            'marketCapUsd':     '1000000000.00',
            'volumeUsd24Hr':    '500000.00',
            'changePercent24Hr': '1.5',
        }
    ]
}


@pytest.fixture
def mock_api_response():
    """Return a MagicMock that mimics a successful requests.Response."""
    resp = MagicMock()
    resp.json.return_value = MOCK_API_PAYLOAD
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_extract_assets_returns_s3_key(mock_api_response):
    """Happy path: function must return a non-empty S3 key ending in assets.json."""
    with (
        patch('requests.get', return_value=mock_api_response),
        patch('boto3.client') as mock_boto,
    ):
        mock_boto.return_value.put_object = MagicMock()
        result = extract_assets()

    assert result is not None
    assert result.endswith('assets.json')


def test_extract_assets_uploads_to_s3(mock_api_response):
    """Verify that put_object is called to upload the raw data to the bronze bucket."""
    with (
        patch('requests.get', return_value=mock_api_response),
        patch('boto3.client') as mock_boto,
    ):
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        extract_assets()

    # Should have called put_object for the raw data + last_run.json
    assert mock_s3.put_object.call_count >= 1


def test_extract_assets_uses_update_since_param(mock_api_response):
    """Verify that updateSince is injected into requests.get when a last run exists in S3."""
    with (
        patch('requests.get', return_value=mock_api_response) as mock_get,
        patch('boto3.client') as mock_boto,
        patch('extract.last_run_timestamp', return_value=12345678),
        patch('extract.save_last_run_timestamp') as mock_save,
    ):
        extract_assets()
        
        # Verify requests.get was called with updateSince param
        mock_get.assert_called_once()
        called_kwargs = mock_get.call_args[1]
        assert 'params' in called_kwargs
        assert called_kwargs['params']['updateSince'] == 12345678


def test_extract_assets_propagates_api_error():
    """An HTTP error from CoinCap must propagate and not be swallowed."""
    with (
        patch('requests.get') as mock_get,
        patch('boto3.client') as mock_boto,
    ):
        mock_get.return_value.raise_for_status.side_effect = Exception('API Error')
        with pytest.raises(Exception, match='API Error'):
            extract_assets()
