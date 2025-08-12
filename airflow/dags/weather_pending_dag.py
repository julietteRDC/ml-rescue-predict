import os
import logging
import pandas as pd
import re
from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from weather_common import load_geocoded_communes, get_s3_hook, S3_PATH, S3_BUCKET_NAME, OPENWEATHERMAP_API, retry_request

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}


def fetch_pending_weather_data():
    commune_rows = load_geocoded_communes()
    s3_path = S3_PATH
    s3_hook = get_s3_hook()
    api_key = OPENWEATHERMAP_API
    # 1. List all accident files in S3 and extract all unique accident dates
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
                df = pd.read_csv(local_path, sep=None, engine='python')
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
        for row in commune_rows:
            commune = row["commune"]
            lat = row.get("lat")
            lon = row.get("lon")
            if pd.isnull(lat) or pd.isnull(lon):
                logging.warning(f"Missing lat/lon for {commune}, skipping.")
                continue
            hist_url = f"https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={lat}&lon={lon}&dt={unix_ts}&appid={api_key}&units=metric"
            resp = retry_request(hist_url)
            if resp:
                try:
                    data = resp.json()
                    weather_data = None
                    if resp.status_code == 200 and "hourly" in data and data["hourly"]:
                        noon_hour = next((h for h in data["hourly"] if pd.to_datetime(
                            h["dt"], unit="s").hour == 12), data["hourly"][0])
                        weather_data = noon_hour
                    elif resp.status_code == 200 and "current" in data:
                        weather_data = data["current"]
                    if weather_data:
                        weather_rows.append({
                            "commune": commune,
                            "date": date_str,
                            "temp": weather_data.get("temp"),
                            "feels_like": weather_data.get("feels_like"),
                            "temp_min": weather_data.get("temp", None),
                            "temp_max": weather_data.get("temp", None),
                            "pressure": weather_data.get("pressure"),
                            "humidity": weather_data.get("humidity"),
                            "wind_speed": weather_data.get("wind_speed"),
                            "clouds_all": weather_data.get("clouds"),
                        })
                    else:
                        logging.warning(
                            f"No weather data for {commune} on {date_str}")
                except Exception as e:
                    logging.warning(
                        f"Failed to parse weather for {commune} on {date_str}: {e}")
            else:
                logging.warning(
                    f"Failed to fetch weather for {commune} on {date_str} after retries.")
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
