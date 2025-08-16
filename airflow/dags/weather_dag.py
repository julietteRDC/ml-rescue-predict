import logging
from datetime import datetime, timedelta

import pandas as pd
import pytz
from s3_to_snowflake import S3ToSnowflakeOperator
from weather_common import (
    OPENWEATHERMAP_API,
    S3_BUCKET_NAME,
    S3_PATH,
    fetch_weather_rows_for_dates_and_communes,
    get_s3_hook,
    load_geocoded_communes,
)

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore


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

    weather_rows = []
    weather_rows = fetch_weather_rows_for_dates_and_communes(
        [date_str],
        [(row["commune"], row.get("lat"), row.get("lon")) for row in commune_rows],
        api_key,
    )
    local_weather_path = f"/tmp/s3_files/weather_history_{date_str}.csv"
    df_weather = pd.DataFrame(weather_rows)
    df_weather.to_csv(local_weather_path, index=False)
    logging.info(f"Uploading weather data to S3 for {date_str}")
    s3_hook.load_file(
        filename=local_weather_path,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    logging.info(f"Weather data uploaded to S3: {local_weather_path} -> {s3_key}")


def update_training_file_with_daily_weather(execution_date_str, training_file_path):
    """
    Met à jour le fichier d'entraînement (nouvelles données) en appliquant une météo unique à toutes les communes,
    en préservant la structure complète des colonnes.
    """
    logging.info(f"Début de la mise à jour pour le {execution_date_str}.")

    filename = "updates_for_training_ml.csv"

    # --- PARTIE 1 : Récupérer les données météo via la fonction helper ---
    # fetch_weather_data gère tout : S3, API, téléchargement et retourne un chemin local fiable.
    # Toute la logique S3 complexe est maintenant supprimée de cette fonction.
    fetch_weather_data(execution_date=execution_date_str)
    local_weather_path = f"/tmp/s3_files/weather_history_{execution_date_str}.csv"

    # On peut lire le fichier en toute confiance.
    df_weather_today = pd.read_csv(local_weather_path)
    if df_weather_today.empty:
        raise ValueError("Le fichier météo est vide.")

    # --- PARTIE 2 : Construire les nouvelles lignes sans perdre de données ---
    try:
        df_training = pd.read_csv(training_file_path)
        original_columns = df_training.columns.tolist()
    except FileNotFoundError:
        logging.error(f"Le fichier '{training_file_path}' est introuvable.")
        raise

    # Isoler la dernière journée de données du fichier d'entraînement
    df_training["date"] = pd.to_datetime(df_training["date"])
    last_available_date = df_training["date"].max()
    df_last_day = df_training[df_training["date"] == last_available_date].copy()

    # Définir dynamiquement les colonnes statiques et météo
    weather_cols = [
        "temp",
        "feels_like",
        "temp_min",
        "temp_max",
        "pressure",
        "humidity",
        "wind_speed",
        "clouds_all",
    ]
    # Les colonnes statiques sont toutes les colonnes sauf la date et la météo
    static_cols = [
        col for col in original_columns if col not in weather_cols and col != "date"
    ]

    # Créer un nouveau DataFrame avec uniquement les colonnes statiques
    df_today_new = df_last_day[static_cols].copy()

    # Ajouter les nouvelles colonnes de date
    date_str = pd.to_datetime(execution_date_str).strftime("%Y-%m-%d")
    df_today_new["date"] = date_str
    df_today_new["jour"] = pd.to_datetime(date_str).day
    df_today_new["mois"] = pd.to_datetime(date_str).month
    df_today_new["an"] = pd.to_datetime(date_str).year

    # Ajouter les colonnes de météo à partir de la première ligne météo
    first_weather_reading = df_weather_today.iloc[0]
    for col in weather_cols:
        df_today_new[col] = first_weather_reading[col]

    # S'assurer que le nouveau DataFrame a exactement les mêmes colonnes que l'original
    df_today_updated = df_today_new[original_columns]
    df_today_updated.drop(columns=["nombre_d_accidents"], inplace=True)

    logging.info(
        f"Dernière ligne ajoutée au DataFrame final :\n{df_today_updated.tail(1)}"
    )

    # --- PARTIE 3 : Sauvegarder et téléverser le fichier ---
    s3_hook = get_s3_hook()
    filename = "updates_for_training_ml.csv"
    full_path_to_file = f"/tmp/{filename}"
    df_today_updated.to_csv(full_path_to_file, index=False, sep=",", encoding="utf-8")

    s3_key_final = f"{S3_PATH}/db/{filename}"
    s3_hook.load_file(
        filename=full_path_to_file,
        key=s3_key_final,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    logging.info(f"Fichier final sauvegardé sur S3 : {s3_key_final}")


dag_default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}

with DAG(
    "update_training_data_dag",
    default_args=dag_default_args,
    description="Met à jour le fichier training_ml.csv avec les données météo du jour.",
    schedule_interval="0 4 * * *",
    tags=["training", "weather"],
    catchup=False,
) as dag:
    # Le chemin du fichier est maintenant défini une seule fois pour être réutilisé
    training_file = "/tmp/s3_files/training_ml.csv"

    # Tâches définies avec des noms de variables uniques
    update_task = PythonOperator(
        task_id="update_training_file_with_daily_weather",
        python_callable=update_training_file_with_daily_weather,
        op_kwargs={
            "execution_date_str": "{{ ds }}",
            "training_file_path": training_file,
        },
    )

    transfer_to_snowflake = S3ToSnowflakeOperator(
        task_id="transfer_s3_to_snowflake",
        bucket=S3_BUCKET_NAME,
        key=f"{S3_PATH}/db/updates_for_training_ml.csv",
        schema="public",
        table="accidents_predict",
        snowflake_conn_id="snowflake_default",
        aws_conn_id="aws_default",
    )

    (update_task >> transfer_to_snowflake)
