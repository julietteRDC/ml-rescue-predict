import os
import logging
import pandas as pd
import re
from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from weather_common import fetch_weather_rows_for_dates_and_communes, load_geocoded_communes, get_s3_hook, S3_PATH, S3_BUCKET_NAME, OPENWEATHERMAP_API, retry_request

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}

# Get missing dates. Look for /tmp/concatenated_accidents.csv and extract dates from it.
# if it doesn't exist, fetch dates from S3. Returns list of missing dates after 2020-01-01.
def get_missing_dates(s3_hook):
    if os.path.exists("/tmp/s3_files/concatenated_accidents.csv"):
        df = pd.read_csv("/tmp/s3_files/concatenated_accidents.csv")
        dates = pd.to_datetime(df[["an", "mois", "jour"]].astype(str).agg("-".join, axis=1), errors='coerce').dt.strftime("%Y-%m-%d").dropna().unique()
        logging.info(f"Found {len(dates)} dates in /tmp/s3_files/concatenated_accidents.csv")
        return sorted([d for d in dates if d >= "2020-01-01"])
    
    s3_path = S3_PATH
    accident_prefix = f"{s3_path}/accidents/"
    accident_keys = s3_hook.list_keys(
        bucket_name=S3_BUCKET_NAME, prefix=accident_prefix)
    accident_dates = set()
    for key in accident_keys:
        if key.endswith('.csv'):
            local_path = os.path.join("/tmp", os.path.basename(key))
            try:
                s3_hook.get_key(
                    key=key, bucket_name=S3_BUCKET_NAME).download_file(local_path)
                df = pd.read_csv(local_path, sep=None, engine='python', encoding='ISO-8859-1')
                for col_set in [("an", "mois", "jour"), ("date",)]:
                    if all(c in df.columns for c in col_set):
                        if "date" in col_set:
                            dates = pd.to_datetime(df["date"], errors='coerce').dt.strftime(
                                "%Y-%m-%d").dropna().unique()
                        else:
                            dates = pd.to_datetime(df[["an", "mois", "jour"]].astype(str).agg(
                                "-".join, axis=1), errors='coerce').dt.strftime("%Y-%m-%d").dropna().unique()
                        accident_dates.update(dates)
                        break
            except Exception as e:
                logging.warning(f"Could not parse accident file {key}: {e}")
            finally:
                try:
                    if os.path.exists(local_path):
                        os.remove(local_path)
                except Exception:
                    pass
    # Remove dates before 2020
    return sorted([d for d in accident_dates if d >= "2022-01-01" and d <="2023-12-31"])

def fetch_pending_weather_data():
    commune_rows = load_geocoded_communes()
    s3_path = S3_PATH
    s3_hook = get_s3_hook()
    api_key = OPENWEATHERMAP_API
    # 1. Get all accident dates needing weather data
    accident_dates = get_missing_dates(s3_hook)
    # 2. Check which weather files are missing for those dates
    weather_prefix = f"{s3_path}/weather/weather_history_"
    weather_keys = s3_hook.list_keys(
        bucket_name=S3_BUCKET_NAME, prefix=weather_prefix)
    weather_dates = set()
    for key in weather_keys:
        m = re.search(r"weather_history_(\d{4}-\d{2}-\d{2})\\.csv$", key)
        if m:
            weather_dates.add(m.group(1))
        else:
            m2 = re.search(
                r"weather_history_(\d{4})_(\d{2})_(\d{2})\\.csv$", key)
            if m2:
                weather_dates.add(f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}")

    missing_dates = sorted(set(accident_dates) - set(weather_dates))
    # Remove missing dates before 2020
    missing_dates = [d for d in missing_dates if d >= "2020-01-01"]
    logging.info(f"Accident dates needing weather: {sorted(accident_dates)}")
    logging.info(f"Weather already present for: {sorted(weather_dates)}")
    logging.info(f"Missing weather dates: {missing_dates}")
    # Fetch weather for all missing dates
    for date_str in missing_dates:
        s3_key = f"{s3_path}/weather/weather_history_{date_str}.csv"
        if s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
            logging.info(
                f"Weather file already exists in S3 for {date_str}, skipping fetch.")
            continue
        weather_rows = []
        dt = pd.to_datetime(date_str)
        unix_ts = int(dt.replace(hour=0, minute=0, second=0,
                      microsecond=0, tzinfo=pd.Timestamp.utcnow().tz).timestamp())
        commune_args = [(row["commune"], row.get("lat"), row.get("lon")) for row in commune_rows]
        weather_rows = fetch_weather_rows_for_dates_and_communes(
            [date_str], commune_args, api_key)
        local_weather_path = f"/tmp/weather_history_{date_str}.csv"
        df_weather = pd.DataFrame(weather_rows)
        df_weather.to_csv(local_weather_path, index=False)
        logging.info(f"Uploading weather data to S3 for {date_str}")
        s3_hook.load_file(
            filename=local_weather_path,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )
        logging.info(
            f"Weather data uploaded to S3: {local_weather_path} -> {s3_key}")


with DAG(
    "weather_pending_dag",
    default_args=default_args,
    description="Fetch all pending weather for accident dates (manual/one-off)",
    schedule_interval=None,
    tags=["weather", "pending"],
) as dag:
    fetch_pending_weather_task = PythonOperator(
        task_id="fetch_pending_weather_data",
        python_callable=fetch_pending_weather_data,
        retries=1,
        retry_delay=timedelta(minutes=10),
    )
