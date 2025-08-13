from __future__ import annotations
import os
import time
import logging
import pandas as pd
import requests
from airflow.hooks.S3_hook import S3Hook # type: ignore
from airflow.models import Variable
from datetime import date, datetime, time as dtime, timezone
from typing import Iterable, List, Tuple, Dict, Any, Optional

# Shared Airflow/S3/variable constants
OPENWEATHERMAP_API = Variable.get("OPENWEATHERMAP_API")
S3_BUCKET_NAME = Variable.get("S3BucketName")
S3_PATH = Variable.get("S3Path")
S3_CONNECTION_ID = "aws_default"

COMMUNES_FILENAME = "weather_all_communes.csv"
GEOCODED_COMMUNES_FILENAME = "weather_all_communes_geocoded.csv"

ONECALL_TIMEMACHINE = "https://api.openweathermap.org/data/3.0/onecall/timemachine"

def retry_request(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 5,
    backoff: int = 2,
    timeout: int = 30,
    verbose: bool = True
) -> Optional[requests.Response]:
    """Retry GET with exponential backoff. Supports query params."""
    delay = 1
    logging.info(f"Trying request to {url} with params {params} up to {max_retries} times with backoff factor {backoff}")
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                if verbose:
                    logging.info(f"Request succeeded: {url}/{params} ({resp.status_code})")
                return resp
            else:
                logging.warning(
                    f"Request failed {url}/{params}({resp.status_code}, {resp.text[:200]}), "
                    f" attempt {attempt + 1}/{max_retries} retrying in {delay}s..."
                )
        except Exception as e:
            logging.warning(f"Request exception: {e}, retrying in {delay}s...")
        time.sleep(delay)
        delay *= backoff
    return None

def _as_ymd(d: Any) -> str:
    if isinstance(d, date) and not isinstance(d, datetime):
        return d.isoformat()
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, str):
        return d
    raise ValueError(f"Unsupported date type: {type(d)}")

def _noon_utc_unix(d: Any) -> int:
    """Return UNIX timestamp for 12:00 UTC on the given calendar date."""
    if isinstance(d, str):
        d = date.fromisoformat(d)
    if isinstance(d, datetime):
        d = d.date()
    dt = datetime.combine(d, dtime(12, 0, 0, tzinfo=timezone.utc))
    return int(dt.timestamp())

def fetch_weather_rows_for_dates_and_communes(
    dates: Iterable[Any],
    communes: Iterable[Tuple[str, float, float]],
    api_key: str,
    *,
    units: str = "metric",
    lang: Optional[str] = None,
    per_call_sleep: float = 0.25,
) -> List[Dict[str, Any]]:
    """
    For each date and (commune, lat, lon), call One Call 3.0 timemachine at 12:00 UTC,
    and return rows with the requested schema.
    """
    weather_rows: List[Dict[str, Any]] = []

    for d in dates:
        curr_date_rows = []
        date_str = _as_ymd(d)
        ts = _noon_utc_unix(d)
        communes_ = list(communes)[:1]  # For testing, limit to 1 commune
        for commune, lat, lon in communes_ :
            params = {
                "lat": f"{lat:.6f}",
                "lon": f"{lon:.6f}",
                "dt": ts,
                "appid": api_key,
                "units": units,  # "standard"|"metric"|"imperial"
            }
            if lang:
                params["lang"] = lang

            resp = retry_request(ONECALL_TIMEMACHINE, params=params)
            if not resp:
                logging.error(f"Giving up for {commune} {lat},{lon} {date_str}")
                continue

            try:
                payload = resp.json()
            except Exception as e:
                logging.error(f"JSON decode error for {commune} {date_str}: {e}")
                continue

            # One Call 3.0 timemachine returns an object with "data": [hourly entries].
            # We'll take the first/only hourly entry (12:00 UTC).
            wx = None
            if isinstance(payload, dict):
                if "data" in payload and isinstance(payload["data"], list) and payload["data"]:
                    wx = payload["data"][0]
                # Fallbacks if the structure differs (defensive)
                elif "current" in payload and isinstance(payload["current"], dict):
                    wx = payload["current"]

            weather_data = wx if isinstance(wx, dict) else {}

            curr_date_rows.append({
                "commune": commune,
                "date": date_str,
                "temp": weather_data.get("temp"),
                "feels_like": weather_data.get("feels_like"),
                "temp_min": weather_data.get("temp", None),   # as requested
                "temp_max": weather_data.get("temp", None),   # as requested
                "pressure": weather_data.get("pressure"),
                "humidity": weather_data.get("humidity"),
                "wind_speed": weather_data.get("wind_speed"),
                "clouds_all": weather_data.get("clouds"),
            })

            time.sleep(per_call_sleep)

        # Expand to all communes, copy weather data for each commune
        communes_copy = list(communes)[1:]
        curr_date_rows.extend([
            {**row, "commune": commune}
            for row in curr_date_rows for commune, _, _ in communes_copy
        ])
        weather_rows.extend(curr_date_rows)

    return weather_rows

def get_s3_hook():
    return S3Hook(aws_conn_id=S3_CONNECTION_ID)

def load_geocoded_communes():
    s3_path = S3_PATH
    s3_hook = get_s3_hook()
    geocoded_communes_s3_key = f"{s3_path}/meta/{GEOCODED_COMMUNES_FILENAME}"
    local_geocoded_path = f"/tmp/s3_files/{GEOCODED_COMMUNES_FILENAME}"
    s3_hook.get_key(key=geocoded_communes_s3_key, bucket_name=S3_BUCKET_NAME).download_file(local_geocoded_path)
    df_communes = pd.read_csv(local_geocoded_path)
    return df_communes.to_dict(orient="records")

def fetch_communes():
    s3_hook = get_s3_hook()
    s3_key = f"{S3_PATH}/meta/{COMMUNES_FILENAME}"
    if s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
        return
    base_url = "https://geo.api.gouv.fr/departements"
    save_dir = "/tmp/s3_files"
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
    df_communes.to_csv(save_path, index=False, encoding="ISO-8859-1")

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
    local_communes_path = f"/tmp/s3_files/{COMMUNES_FILENAME}"
    local_geocoded_path = f"/tmp/s3_files/{GEOCODED_COMMUNES_FILENAME}"
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

