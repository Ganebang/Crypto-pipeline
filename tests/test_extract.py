import pytest
from unittest.mock import patch, MagicMock
import sys
sys.path.insert(0, '/usr/local/airflow/include/scripts')
from extract import extract_assets

def test_extract_assets_success():
    # Mock a successful API response with one Bitcoin record
    mock_response = MagicMock()
    mock_response.json.return_value = {'data': [{'id': 'bitcoin', 'priceUsd': '50000', 'rank': '1', 'symbol': 'BTC', 'name': 'Bitcoin', 'marketCapUsd': '1000000', 'volumeUsd24Hr': '50000', 'changePercent24Hr': '1.5'}]}
    mock_response.raise_for_status = MagicMock()
    with patch('requests.get', return_value=mock_response), patch('boto3.client') as mock_s3:
        mock_s3.return_value.put_object = MagicMock()
        result = extract_assets()
        # Verify the function returns a valid S3 key
        assert result is not None
        assert 'assets.json' in result

def test_extract_assets_api_error():
    # Verify that an API error propagates as an exception
    with patch('requests.get') as mock_get:
        mock_get.return_value.raise_for_status.side_effect = Exception('API Error')
        with pytest.raises(Exception):
            extract_assets()
