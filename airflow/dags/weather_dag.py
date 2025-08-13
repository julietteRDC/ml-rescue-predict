from datetime import datetime, timedelta
import time

import pandas as pd

import logging
import pandas as pd
import pytz
from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from weather_common import fetch_weather_rows_for_dates_and_communes, load_geocoded_communes, get_s3_hook, S3_PATH, S3_BUCKET_NAME, OPENWEATHERMAP_API, retry_request


def fetch_weather_data(execution_date=None):
    commune_rows = load_geocoded_communes()
    s3_path = S3_PATH
    s3_hook = get_s3_hook()
    api_key = OPENWEATHERMAP_API
    if execution_date is None:
        execution_date = datetime.now(pytz.UTC)
    elif isinstance(execution_date, str):
        execution_date = pd.to_datetime(execution_date).tz_localize("UTC")
    date_str = execution_date.strftime("%Y-%m-%d")
    s3_key = f"{s3_path}/weather/weather_history_{date_str}.csv"
    if s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
        logging.info(
            f"Weather file already exists in S3 for {date_str}, skipping fetch.")
        return
    weather_rows = []
    dt = pd.to_datetime(date_str)
    unix_ts = int(dt.replace(hour=0, minute=0, second=0,
                             microsecond=0, tzinfo=pd.Timestamp.utcnow().tz).timestamp())
    weather_rows = fetch_weather_rows_for_dates_and_communes(
        [date_str], [(row["commune"], row.get("lat"), row.get("lon")) for row in commune_rows], api_key)
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


dag_default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}

with DAG(
    "weather_dag",
    default_args=dag_default_args,
    description="Fetch and upload weather data for all communes to S3 (date-specific)",
    schedule_interval="@daily",
    tags=["weather"],
) as dag:
    fetch_weather_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
        op_kwargs={"execution_date": "{{ ds }}"},
        retries=1,
        retry_delay=timedelta(minutes=10),
    )
