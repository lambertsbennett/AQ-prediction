import requests
import pandas as pd
import yaml
from minio import Minio

with open ("config.yaml", "r") as stream:
    config = yaml.safe_load(stream)


BASE_URL = "https://u50g7n0cbj.execute-api.us-east-1.amazonaws.com"


def healthy_connection():
    r = requests.get(f"{BASE_URL}/ping")
    if r.status_code == 200:
        return True
    else:
        return False


# Get historical data for Vienna 
def get_historical_data(start, end, data_category="pm25", all=False, limit=10000):
    if all:
        params = {"country":"AT", "city": "Wien", "date_from": start, "date_to": end, "limit": limit}
        r = requests.get(f"{BASE_URL}/v2/measurements", params=params)
        response_data = r.json()["results"]
        df = pd.json_normalize(response_data)
        daily_median = df.groupby(by=["date.utc", "parameter"]).median()

    else:
        params = {"country":"AT", "city": "Wien", "parameter": data_category, "date_from": start, "date_to": end, "limit": limit}
        r = requests.get(f"{BASE_URL}/v2/measurements", params=params)
        response_data = r.json()["results"]
        df = pd.json_normalize(response_data)
        daily_median = df.groupby(by=["date.utc", "parameter"]).median()
    
    return daily_median


def put_file_minio():
    client = Minio(
        "localhost:9000",
        access_key=config["MINIO_USER"],
        secret_key=config["MINIO_PASSWORD"],
        secure=False
    )
    found = client.bucket_exists("openaq")
    if not found:
        client.make_bucket("openaq")
    else:
        print("Bucket 'openaq' already exists")

    client.fput_object(
        "openaq", "historical_data.parquet", "./historical_data.parquet", content_type="application/parquet"
    )
    print("File successfully uploaded!")   

historical_df = get_historical_data(all=True, start="2022-01-01", end="2022-04-03")
historical_df.to_parquet("./historical_data.parquet")
try:
    put_file_minio()
except Exception as e:
    print(e)