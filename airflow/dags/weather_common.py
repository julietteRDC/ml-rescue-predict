from __future__ import annotations
import time
import logging
import pandas as pd
import requests
from airflow.hooks.S3_hook import S3Hook  # type: ignore
from airflow.models import Variable
from datetime import date, datetime, time as dtime, timezone
from typing import Iterable, List, Tuple, Dict, Any, Optional

# Shared Airflow/S3/variable constants
OPENWEATHERMAP_API = Variable.get("OPENWEATHERMAP_API")
S3_BUCKET_NAME = Variable.get("S3BucketName")
S3_PATH = Variable.get("S3Path")
S3_CONNECTION_ID = "aws_default"

COMMUNES_FILENAME = "all_communes.csv"
GEOCODED_COMMUNES_FILENAME = "weather_all_communes_geocoded.csv"

ONECALL_TIMEMACHINE = "https://api.openweathermap.org/data/3.0/onecall/timemachine"


def retry_request(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 5,
    backoff: int = 2,
    timeout: int = 30,
    verbose: bool = True,
) -> Optional[requests.Response]:
    """Retry GET with exponential backoff. Supports query params."""
    delay = 1
    logging.info(
        f"Trying request to {url} with params {params} up to {max_retries} times with backoff factor {backoff}"
    )
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                if verbose:
                    logging.info(
                        f"Request succeeded: {url}/{params} ({resp.status_code})"
                    )
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
    communes: List[Tuple[str, float, float]],  # Changed to List for indexing
    api_key: str,
    *,
    units: str = "metric",
    lang: Optional[str] = None,
    per_call_sleep: float = 0.25,
) -> List[Dict[str, Any]]:
    """
    For each date, fetches weather for the FIRST commune, then applies this
    weather to ALL communes, ensuring each has its correct name and code.
    """
    s3_hook = get_s3_hook()
    communes_s3_key = f"{S3_PATH}/meta/{COMMUNES_FILENAME}"
    local_communes_path = f"/tmp/s3_files/{COMMUNES_FILENAME}"
    if s3_hook.check_for_key(communes_s3_key, S3_BUCKET_NAME):
        logging.info(f"Fichier trouvé : {communes_s3_key}")
        s3_hook.get_key(key=communes_s3_key, bucket_name=S3_BUCKET_NAME).download_file(
            local_communes_path
        )
    else:
        logging.error(f"Fichier non trouvé : {communes_s3_key}")
        raise FileNotFoundError(
            f"L'objet {communes_s3_key} n'existe pas dans le bucket {S3_BUCKET_NAME}"
        )
    s3_hook.get_key(key=communes_s3_key, bucket_name=S3_BUCKET_NAME).download_file(
        local_communes_path
    )
    df_communes = pd.read_csv(local_communes_path)
    commune_code_map = pd.Series(
        df_communes.code.values, index=df_communes.commune
    ).to_dict()

    weather_rows: List[Dict[str, Any]] = []

    for d in dates:
        date_str = _as_ymd(d)
        ts = _noon_utc_unix(d)

        # --- ÉTAPE 1: Récupérer la météo UNE SEULE FOIS (pour la première commune) ---
        if not communes:
            logging.warning("The communes list is empty. Skipping date.")
            continue

        first_commune_name, lat, lon = communes[0]
        params = {
            "lat": f"{lat:.6f}",
            "lon": f"{lon:.6f}",
            "dt": ts,
            "appid": api_key,
            "units": units,
        }
        if lang:
            params["lang"] = lang

        logging.info(
            f"Fetching reference weather for {first_commune_name} on {date_str}"
        )
        resp = retry_request(ONECALL_TIMEMACHINE, params=params)

        # Extraire les données météo de la réponse
        try:
            payload = resp.json() if resp else {}
            weather_data = payload.get("data", [{}])[0] if payload.get("data") else {}
        except Exception as e:
            logging.error(f"Could not get reference weather for {date_str}: {e}")
            continue  # Skip this entire date if the reference weather API call fails

        if not weather_data:
            logging.warning(
                f"Reference weather data is empty for {date_str}. Skipping."
            )
            continue

        # --- ÉTAPE 2: Construire la liste finale en parcourant TOUTES les communes ---
        logging.info(f"Applying reference weather to all communes for {date_str}")
        for commune_name, _, _ in communes:
            # Récupérer le code de la commune. Si non trouvé, on ignore cette commune.
            commune_code = commune_code_map.get(commune_name)
            if not commune_code:
                logging.warning(
                    f"No code found for commune '{commune_name}'. Skipping."
                )
                continue

            # Construire la ligne finale en combinant la météo unique et les infos de la commune ACTUELLE
            new_row = {
                "commune": commune_name,
                "com": commune_code,
                "date": date_str,
                "temp": weather_data.get("temp"),
                "feels_like": weather_data.get("feels_like"),
                "temp_min": weather_data.get("temp"),
                "temp_max": weather_data.get("temp"),
                "pressure": weather_data.get("pressure"),
                "humidity": weather_data.get("humidity"),
                "wind_speed": weather_data.get("wind_speed"),
                "clouds_all": weather_data.get("clouds"),
            }
            weather_rows.append(new_row)

        time.sleep(per_call_sleep)

    return weather_rows


def get_s3_hook():
    return S3Hook(aws_conn_id=S3_CONNECTION_ID)


def load_geocoded_communes():
    s3_hook = get_s3_hook()
    geocoded_communes_s3_key = f"{S3_PATH}/meta/{GEOCODED_COMMUNES_FILENAME}"
    local_geocoded_path = f"/tmp/s3_files/{GEOCODED_COMMUNES_FILENAME}"
    s3_hook.get_key(
        key=geocoded_communes_s3_key, bucket_name=S3_BUCKET_NAME
    ).download_file(local_geocoded_path)
    df_communes = pd.read_csv(local_geocoded_path)
    return df_communes.to_dict(orient="records")


def fetch_geocodes():
    s3_hook = get_s3_hook()
    communes_s3_key = f"{S3_PATH}/meta/{COMMUNES_FILENAME}"
    geocoded_communes_s3_key = f"{S3_PATH}/meta/{GEOCODED_COMMUNES_FILENAME}"
    local_communes_path = f"/tmp/s3_files/{COMMUNES_FILENAME}"
    local_geocoded_path = f"/tmp/s3_files/{GEOCODED_COMMUNES_FILENAME}"
    if s3_hook.check_for_key(communes_s3_key, S3_BUCKET_NAME):
        logging.info(f"Fichier trouvé : {communes_s3_key}")
        s3_hook.get_key(key=communes_s3_key, bucket_name=S3_BUCKET_NAME).download_file(
            local_communes_path
        )
    else:
        logging.error(f"Fichier non trouvé : {communes_s3_key}")
        raise FileNotFoundError(
            f"L'objet {communes_s3_key} n'existe pas dans le bucket {S3_BUCKET_NAME}"
        )
    s3_hook.get_key(key=communes_s3_key, bucket_name=S3_BUCKET_NAME).download_file(
        local_communes_path
    )
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
    logging.info(
        f"Geocoded communes file saved: {local_geocoded_path} -> {geocoded_communes_s3_key}"
    )
