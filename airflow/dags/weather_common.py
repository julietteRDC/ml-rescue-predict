import os
import time
import logging
import pandas as pd
import requests
from airflow.hooks.S3_hook import S3Hook # type: ignore
from airflow.models import Variable

# Shared Airflow/S3/variable constants
OPENWEATHERMAP_API = Variable.get("OPENWEATHERMAP_API")
S3_BUCKET_NAME = Variable.get("S3BucketName")
S3_PATH = Variable.get("S3Path")
S3_CONNECTION_ID = "aws_default"

COMMUNES_FILENAME = "weather_all_communes.csv"
GEOCODED_COMMUNES_FILENAME = "weather_all_communes_geocoded.csv"

def get_s3_hook():
    return S3Hook(aws_conn_id=S3_CONNECTION_ID)

def retry_request(url, max_retries=5, backoff=2):
    delay = 1
    for attempt in range(max_retries):
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                return resp
            else:
                logging.warning(f"Request failed ({resp.status_code}), retrying in {delay}s...")
        except Exception as e:
            logging.warning(f"Request exception: {e}, retrying in {delay}s...")
        time.sleep(delay)
        delay *= backoff
    return None

def load_geocoded_communes():
    s3_path = S3_PATH
    s3_hook = get_s3_hook()
    geocoded_communes_s3_key = f"{s3_path}/meta/{GEOCODED_COMMUNES_FILENAME}"
    local_geocoded_path = f"/tmp/{GEOCODED_COMMUNES_FILENAME}"
    s3_hook.get_key(key=geocoded_communes_s3_key, bucket_name=S3_BUCKET_NAME).download_file(local_geocoded_path)
    df_communes = pd.read_csv(local_geocoded_path)
    return df_communes.to_dict(orient="records")

def fetch_communes():
    s3_hook = get_s3_hook()
    s3_key = f"{S3_PATH}/meta/{COMMUNES_FILENAME}"
    if s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
        return
    base_url = "https://geo.api.gouv.fr/departements"
    save_dir = "/tmp"
    os.makedirs(save_dir, exist_ok=True)

    all_communes = []
    # Loop through all departments (1 to 95 for metropolitan France)
    # for dept in range(1, 96):
    for dept in [77]:
        url = f"{base_url}/{dept}/communes"
        params = {"fields": "nom,codeRegion,code,population", "format": "json"}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            communes = response.json()
            all_communes.extend(communes)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for department {dept}: {e}")

    # Extract commune names and postal codes
    data = []
    for commune in all_communes:
        nom_commune = commune["nom"]
        code_region = commune["codeRegion"]
        code = commune["code"]
        population = commune["population"]
        data.append([nom_commune, code_region, code, population])

    # Create DataFrame
    df_communes = pd.DataFrame(
        data, columns=["commune", "code_region", "code", "population"]
    )

    # Save as CSV
    save_path = os.path.join(save_dir, COMMUNES_FILENAME)
    df_communes.to_csv(save_path, index=False, encoding="utf-8")

    # Upload to S3
    s3_hook.load_file(
        filename=save_path,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    logging.info(f"Communes file saved successfully: {save_path} -> {s3_key}")

def fetch_geocodes():
    s3_hook = get_s3_hook()
    communes_s3_key = f"{S3_PATH}/meta/{COMMUNES_FILENAME}"
    geocoded_communes_s3_key = f"{S3_PATH}/meta/{GEOCODED_COMMUNES_FILENAME}"
    local_communes_path = f"/tmp/{COMMUNES_FILENAME}"
    local_geocoded_path = f"/tmp/{GEOCODED_COMMUNES_FILENAME}"
    s3_hook.get_key(key=communes_s3_key, bucket_name=S3_BUCKET_NAME).download_file(local_communes_path)
    df_communes = pd.read_csv(local_communes_path)
    if not ("lat" in df_communes.columns and "lon" in df_communes.columns):
        df_communes["lat"] = None
        df_communes["lon"] = None
    api_key = OPENWEATHERMAP_API
    for idx, row in df_communes.iterrows():
        if pd.isnull(row.get("lat")) or pd.isnull(row.get("lon")):
            city = row["commune"]
            geo_url = f"http://api.openweathermap.org/geo/1.0/direct?q={city},FR&limit=1&appid={api_key}"
            geo_resp = retry_request(geo_url)
            if geo_resp:
                try:
                    geo_data = geo_resp.json()
                    if geo_data:
                        df_communes.at[idx, "lat"] = geo_data[0]["lat"]
                        df_communes.at[idx, "lon"] = geo_data[0]["lon"]
                        logging.info(f"Got geocode for {city}")
                    else:
                        logging.warning(f"Could not geocode {city}")
                except Exception as e:
                    logging.warning(f"Geocoding failed for {city}: {e}")
            else:
                logging.warning(f"Geocoding failed for {city} after retries.")
    df_communes.to_csv(local_geocoded_path, index=False)
    s3_hook.load_file(
        filename=local_geocoded_path,
        key=geocoded_communes_s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    logging.info(f"Geocoded communes file saved: {local_geocoded_path} -> {geocoded_communes_s3_key}")
