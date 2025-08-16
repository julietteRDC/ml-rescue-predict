from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from weather_common import fetch_geocodes
# from weather_common import fetch_communes, fetch_geocodes

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}

with DAG(
    "weather_communes_dag",
    default_args=default_args,
    description="Populate S3 with all commune metadata and geocodes (manual/one-off)",
    schedule_interval=None,
    tags=["weather", "communes"],
) as dag:
    # fetch_communes_task = PythonOperator(
    #     task_id="fetch_communes",
    #     python_callable=fetch_communes,
    #     retries=1,
    #     retry_delay=timedelta(minutes=10),
    # )
    fetch_geocodes_task = PythonOperator(
        task_id="fetch_geocodes",
        python_callable=fetch_geocodes,
        retries=1,
        retry_delay=timedelta(minutes=10),
    )
    # _ = fetch_communes_task >> fetch_geocodes_task
    _ = fetch_geocodes_task
