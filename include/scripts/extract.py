import json
import requests
from datetime import date
from db import get_s3_client, COINCAP_API_KEY, BRONZE_BUCKET, logger

def extract_assets():
    # Call CoinCap API v3 with bearer token authentication
    url = 'https://rest.coincap.io/v3/assets'
    headers = {'Authorization': f'Bearer {COINCAP_API_KEY}'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    # Upload raw JSON to MinIO bronze bucket
    s3 = get_s3_client()
    today = date.today().isoformat()
    key = f'assets/{today}/assets.json'
    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=json.dumps(data))
    logger.info(f'Uploaded raw data to bronze/{key}')
    return key

if __name__ == '__main__':
    extract_assets()
