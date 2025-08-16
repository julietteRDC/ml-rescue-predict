import csv
import glob
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import product
from venv import logger

import pandas as pd
import requests
from bs4 import BeautifulSoup
from s3_to_snowflake import S3ToSnowflakeOperator

# from s3_to_postgres import S3ToPostgresOperator
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

# Globals variables
OPENWEATHERMAP_API = Variable.get("OPENWEATHERMAP_API")
S3_BUCKET_NAME = Variable.get("S3BucketName")
S3_PATH = Variable.get("S3Path")
DATASET_ACCIDENT_ID = "53698f4ca3a729239d2036df"
DATASET_ACCIDENT_URL = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ACCIDENT_ID}/"

dag_default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}


def _fetch_accident_data():
    """
    Fetch accident data from a dataset URL and upload it to an S3 bucket.

    This function logs the start and end of the data fetching process,
    sends a GET request to the dataset URL to retrieve the data,
    checks if the request was successful, iterates over each resource
    in the dataset to process the files, extracts the file title and URL
    from each resource, checks if the file title contains specific keywords
    and does not contain certain phrases, formats the filename to replace
    spaces with underscores and ensures it ends with .csv, downloads the
    file content and writes it to a local file, uses the S3 hook to upload
    the file to an S3 bucket. It also logs any errors that occur during file downloads.
    """
    logging.info("fetch accident data started")

    response = requests.get(DATASET_ACCIDENT_URL)

    if response.status_code == 200:
        data_ressources = response.json()
        logging.info("Start downloading")

        s3_hook = S3Hook(aws_conn_id="aws_default")

        for resource in data_ressources.get("resources", []):
            file_title = resource.get("title", "noname").lower()
            file_url = resource.get("url", "")
            file_format = resource.get("format", "")

            if (
                any(keyword in file_title for keyword in ["usa", "vehi", "lieu", "car"])
                and "vehicules-immatricules-baac" not in file_title
                and "description" not in file_title
                and file_format == "csv"
            ):
                filename = file_title.replace(" ", "_")
                if not filename.endswith(".csv"):
                    filename += ".csv"
                full_path_to_file = f"/tmp/{filename}"
                s3_key = f"{S3_PATH}/accidents/{filename}"

                if not s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
                    file_response = requests.get(file_url)

                    if file_response.status_code == 200:
                        with open(full_path_to_file, "wb") as f:
                            f.write(file_response.content)

                        s3_hook = S3Hook(aws_conn_id="aws_default")
                        s3_hook.load_file(
                            filename=full_path_to_file,
                            key=s3_key,
                            bucket_name=S3_BUCKET_NAME,
                            replace=True,
                        )
                        logging.info(f"Saved accidents data to S3 with name {filename}")
                    else:
                        logging.error(f"Failed to download {filename}")
                else:
                    logging.info(
                        f"File {filename} already exists in S3 ({s3_key}), skipping upload."
                    )

    logging.info("fetch accident data finished")


def _fetch_public_holidays_data(zone="metropole", current_year=None):
    logging.info("fetch public holidays")
    if current_year is None:
        current_year = datetime.now().year

    start_year = current_year - 20
    end_year = current_year + 5

    all_holidays = {}

    filename = "public_holidays.csv"
    full_path_to_file = f"/tmp/{filename}"

    for year in range(start_year, end_year + 1):
        url = f"https://calendrier.api.gouv.fr/jours-feries/{zone}/{year}.json"
        response = requests.get(url)

        if response.status_code == 200:
            holidays = response.json()
            all_holidays.update(holidays)

    if not all_holidays:
        logging.info("holydays dictionary is empty")
    else:
        # Save holidays data as csv
        with open(full_path_to_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["date", "public_holiday"])

            for date in sorted(all_holidays.keys()):
                formatted_date = date
                writer.writerow([formatted_date, all_holidays[date]])

        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_key = f"{S3_PATH}/meta/{filename}"
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_hook.load_file(
            filename=full_path_to_file,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )
        logging.info(f"Saved public holidays data to S3 with name {filename}")


def _fetch_fire_brigade():
    def fetch_all(filename, full_path_to_file):
        logging.info("fetch fire brigade")
        # departments = list(range(1, 96)) + list(range(971, 977))
        departments = [77]
        HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
        }

        fire_brigades = []

        for dept in departments:
            dept_str = str(dept).zfill(2)
            page = 0

            while True:
                API_URL = f"https://pompierama.com/views/ajax?_wrapper_format=drupal_ajax&recherche={dept_str}&view_name=recherche_caserne&view_display_id=block_1&page={page}"
                logging.info("fetch fire brigade")
                try:
                    response = requests.get(API_URL, headers=HEADERS)
                    response.raise_for_status()
                    time.sleep(1)

                    data = response.json()
                    html_data = next(
                        (
                            item["data"]
                            for item in data
                            if "data" in item
                            and isinstance(item["data"], str)
                            and item["data"].strip()
                        ),
                        None,
                    )

                    if not html_data:
                        break

                    soup = BeautifulSoup(html_data, "html.parser")
                    rows = soup.find_all("tr")

                    if len(rows) <= 1:
                        break

                    for row in rows[1:]:
                        cols = row.find_all("td")
                        if len(cols) >= 3:
                            fire_brigades.append(
                                {
                                    "name": cols[0].text.strip(),
                                    "city": cols[1].text.strip(),
                                    "department": cols[2].text.strip(),
                                }
                            )

                    if soup.find("a", rel="next"):
                        page += 1
                    else:
                        break

                except (requests.exceptions.RequestException, Exception):
                    break

        df = pd.DataFrame(fire_brigades)
        df.to_csv(full_path_to_file, index=False, encoding="utf-8")

    filename = "fire_brigade.csv"
    full_path_to_file = f"/tmp/{filename}"
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_key = f"{S3_PATH}/meta/{filename}"
    if not s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
        fetch_all(filename, full_path_to_file)
        s3_hook.load_file(
            filename=full_path_to_file,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )
        logging.info(f"Saved public holidays data to S3 with name {filename}")


def _fetch_school_holidays_data():
    def get_school_holidays_api(start_year, end_year):
        url = "https://data.education.gouv.fr/api/records/1.0/search/"
        params = {
            "dataset": "fr-en-calendrier-scolaire",
            "q": "",
            "rows": 10000,
            "facet": "annee_scolaire",
            "facet": "metropole",
        }

        response = requests.get(url, params=params)
        data = response.json()

        records = data.get("records", [])
        holidays = []

        for record in records:
            fields = record.get("fields", {})
            school_year = fields.get("annee_scolaire")
            zone = fields.get("zones")
            holiday_type = fields.get("description")
            start_date = fields.get("start_date")
            end_date = fields.get("end_date")

            if start_year <= int(school_year.split("-")[0]) <= end_year:
                holidays.append(
                    {
                        "school_year": school_year,
                        "zone": zone,
                        "type": holiday_type,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )

        return holidays

    filename = "school_holidays.csv"
    full_path_to_file = f"/tmp/{filename}"
    start_year = datetime.now().year - 4
    end_year = datetime.now().year + 1
    logging.info(f"Fetch school holidays for {start_year} to {end_year}")
    holidays_data = get_school_holidays_api(start_year, end_year)
    df = pd.DataFrame(holidays_data)
    df = df.drop("school_year", axis=1)
    df.to_csv(full_path_to_file, index=False)

    # Upload to S3
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_key = f"{S3_PATH}/meta/{filename}"

    s3_hook.load_file(
        filename=full_path_to_file,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    logging.info(f"Saved school holidays data to S3 with name {filename}")


# Function to fetch data from the API
def _fetch_communes():
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
    filename = "all_communes.csv"
    full_path_to_file = f"/tmp/{filename}"
    save_path = os.path.join(save_dir, "all_communes.csv")
    df_communes.to_csv(save_path, index=False, encoding="utf-8")

    # Upload to S3
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_key = f"{S3_PATH}/meta/{filename}"
    if not s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
        s3_hook.load_file(
            filename=full_path_to_file,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )
        logging.info(f"Communes file saved successfully: {save_path}")


def _download_file_from_s3(bucket_name, s3_key, local_dir):
    """
    Download a file from an S3 bucket to a local directory.

    :param bucket_name: The name of the S3 bucket.
    :param s3_key: The key of the file in the S3 bucket.
    :param local_dir: The local directory where the file will be saved.
    """
    # Initialize the S3 hook
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # Ensure the local directory exists
    os.makedirs(local_dir, exist_ok=True)

    # Define the full local path including the filename
    local_path = os.path.join(local_dir, os.path.basename(s3_key))

    # Check if the file already exists locally
    if os.path.exists(local_path):
        print(f"File already exists locally at {local_path}. Skipping download.")
        return

    # Check if the file exists in the S3 bucket
    if s3_hook.check_for_key(key=s3_key, bucket_name=bucket_name):
        # Download the file
        try:
            s3_hook.get_key(key=s3_key, bucket_name=bucket_name).download_file(
                local_path
            )
            print(f"File downloaded successfully to {local_path}")
        except PermissionError as e:
            print(f"Permission error: {e}")
    else:
        print(f"File {s3_key} does not exist in the bucket {bucket_name}")


def _fetch_files(type="accidents"):
    logging.info("Starting to fetch files...")
    s3_hook = S3Hook(aws_conn_id="aws_default")
    local_dir = "/tmp/s3_files"

    os.makedirs(local_dir, exist_ok=True)

    s3_keys = s3_hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=f"{S3_PATH}/{type}/")
    logging.info(f"S3 Keys: {s3_keys}")

    if not s3_keys:
        raise FileNotFoundError(
            f"No files found in {S3_PATH}/{type}/ in {S3_BUCKET_NAME}."
        )

    for s3_key in s3_keys:
        logging.info(f"Local_path: {local_dir}")
        _download_file_from_s3(S3_BUCKET_NAME, s3_key, local_dir)
        logging.info(f"Downloaded {s3_key} to {local_dir}")


def _group_accident_data(ti, nb_years=2):
    logging.info(f"Starting group accident files for {nb_years} years...")
    local_dir = "/tmp/s3_files"
    pattern = re.compile(r"car.*(\d{4})")
    files_by_year = defaultdict(list)

    files = []

    # Check if directory exists
    if not os.path.exists(local_dir):
        logging.error(f"Directory {local_dir} does not exist.")
        return files

    # Iterate over files in the directory
    for file in os.listdir(local_dir):
        match = pattern.search(file)
        if match:
            year = int(match.group(1))
            files_by_year[year].append(file)

    # Find the two most recent files
    if files_by_year:
        sorted_years = sorted(files_by_year.keys(), reverse=True)
        recent_files = []

        for year in sorted_years:
            recent_files.extend(files_by_year[year])
            if len(recent_files) >= nb_years:
                break

        files = recent_files[:nb_years]
        logging.info(f"Files list --> {files}")
        # Read and concatenate CSV files
        dataframes = []
        for file in files:
            file_path = os.path.join(local_dir, file)
            logging.info(f"Processing file: {file_path}")

            # Check if the path is a file
            if os.path.isfile(file_path):
                df = pd.read_csv(file_path, sep=";", dtype=str)
                dataframes.append(df)

        if dataframes:
            concatenated_df = pd.concat(dataframes, ignore_index=True)
            # Create a new 'id' column based on 'Accident_Id' or 'Num_Acc'
            concatenated_df["id"] = concatenated_df["Accident_Id"].combine_first(
                concatenated_df["Num_Acc"]
            )
            # Drop the original 'Accident_Id' and 'Num_Acc' columns
            concatenated_df.drop(columns=["Accident_Id", "Num_Acc"], inplace=True)

            df_grouped = (
                concatenated_df.groupby(["jour", "mois", "an", "dep", "com"])
                .size()
                .reset_index(name="nombre_d_accidents")
            )

            # Filter only on one department
            df_filtered = df_grouped[df_grouped["dep"] == "77"]

            # Save the concatenated DataFrame to a shared location or pass it using XCom
            output_path = "/tmp/s3_files/concatenated_accidents.csv"
            df_filtered.to_csv(output_path, index=False)

            logging.info(f"Concatenated CSV saved to {output_path}")
            # Push the output path to XCom for other tasks to use
            ti.xcom_push(key="concatenated_accidents_csv_path", value=output_path)
            logging.info(
                f"XCom push with key: 'concatenated_accidents_csv_path' and value: {output_path}"
            )

        else:
            logging.warning("No data to concatenate.")
    else:
        logging.warning("No files matched the pattern.")


def _group_data(ti):
    logging.info("Enrich data...")
    path_communes = "/tmp/s3_files/all_communes.csv"
    path_public_holidays = "/tmp/s3_files/public_holidays.csv"
    path_school_holidays = "/tmp/s3_files/school_holidays.csv"

    logging.info("Commune transformations")
    df_communes = pd.read_csv(path_communes)
    df_communes.rename(columns={"code": "com"}, inplace=True)

    logging.info("Public holidays transformations")
    df_public_holidays = pd.read_csv(path_public_holidays, sep=",")
    df_public_holidays["date"] = pd.to_datetime(df_public_holidays["date"])

    logging.info("School holidays transformations")
    df_school_holidays = pd.read_csv(path_school_holidays, sep=",")
    df_school_holidays["zone"] = (
        df_school_holidays["zone"]
        .str.lower()  # Convertir en minuscule
        .str.replace(" ", "_")  # Remplacer les espaces par des underscores
    )
    df_school_holidays["start_date"] = pd.to_datetime(
        df_school_holidays["start_date"]
    ).dt.tz_convert("UTC")
    df_school_holidays["end_date"] = pd.to_datetime(
        df_school_holidays["end_date"]
    ).dt.tz_convert("UTC")

    logging.info("Accidents transformations")
    all_accidents_filename = ti.xcom_pull(
        task_ids="transformation_branch.group_accident_data",
        key="concatenated_accidents_csv_path",
    )

    logging.info(f"Accidents transformations filename: {all_accidents_filename}")

    if all_accidents_filename is None:
        logging.error("XCom value for 'concatenated_accidents_csv_path' is None.")

    df = pd.read_csv(all_accidents_filename)
    df_grouped = (
        df.groupby(["jour", "mois", "an", "dep", "com"])
        .size()
        .reset_index(name="nombre_d_accidents")
    )

    # Generate all dates from existing years
    logging.info("Generate all dates from existing years")
    years = df_grouped["an"].unique()
    start_date = f"{min(years)}-01-01"
    end_date = f"{max(years)}-12-31"
    date_range = pd.date_range(start=start_date, end=end_date)

    # Create a DataFrame from the date range
    logging.info("Create a DataFrame from the date range")
    date_df = pd.DataFrame(
        {
            "date": date_range,
            "jour": date_range.day,
            "mois": date_range.month,
            "an": date_range.year,
        }
    )
    date_df["date"] = pd.to_datetime(date_df["date"]).dt.tz_localize("UTC")

    # Convertion in UTC date for compatibility
    df_public_holidays["date"] = df_public_holidays["date"].dt.tz_localize("UTC")

    # 📌 Mark public days
    date_df["public_holidays"] = date_df["date"].isin(df_public_holidays["date"])

    logging.warning(date_df["date"].dtype)
    logging.warning(df_school_holidays["start_date"].dtype)
    logging.warning(df_school_holidays["end_date"].dtype)

    logging.warning(print(df_school_holidays[["start_date", "end_date"]].head()))
    # 🏫 Mark vacation days for each zone
    logging.info("Mark vacation days for each zone")
    for zone in ["zone_a", "zone_b", "zone_c"]:
        date_df[zone] = date_df["date"].apply(
            lambda d: any(
                (d >= row["start_date"])
                and (d <= row["end_date"])
                and (zone in row["zone"])
                for _, row in df_school_holidays.iterrows()
            )
        )

    date_df.drop(columns=["date"], inplace=True)

    df_full = pd.DataFrame(
        product(
            date_df.itertuples(index=False),
            df_communes["com"],
        ),
        columns=["date_info", "com"],
    )

    df_full = df_full.merge(df_communes[["com", "population"]], on="com", how="left")

    logging.info(df_full.head())

    df_full[
        [
            "jour",
            "mois",
            "an",
            "public_holidays",
            "zone_a",
            "zone_b",
            "zone_c",
        ]
    ] = pd.DataFrame(df_full["date_info"].tolist(), index=df_full.index)
    df_full.drop(columns=["date_info"], inplace=True)
    df_full["dep"] = 77
    df_accidents = pd.read_csv("/tmp/s3_files/concatenated_accidents.csv", sep=",")
    df_accidents["com"] = df_accidents["com"].astype(str)
    df_full["com"] = df_full["com"].astype(str)
    df_accidents["mois"] = df_accidents["mois"].astype(str).str.zfill(2)
    df_full["mois"] = df_full["mois"].astype(str).str.zfill(2)
    df_full.to_csv("/tmp/s3_files/df_full.csv")
    df_accidents_all = df_full.merge(
        df_accidents, on=["jour", "mois", "an", "dep", "com"], how="left"
    )

    # 🔄 Remplacer les NaN par 0 pour les jours sans accident
    df_accidents_all["nombre_d_accidents"] = (
        df_accidents_all["nombre_d_accidents"].fillna(0).astype(int)
    )
    df_accidents_all.to_csv("/tmp/s3_files/df_accidents_all.csv", index=False)


def _add_weather_history():
    # Directory where weather files are downloaded
    weather_dir = "/tmp/s3_files"
    pattern = os.path.join(weather_dir, "weather_history_*.csv")
    files = glob.glob(pattern)

    # Load communes reference to map names to INSEE codes
    communes_path = os.path.join(weather_dir, "all_communes.csv")
    df_communes = pd.read_csv(communes_path)
    name_to_code = {
        row["commune"]: str(row["code"]) for _, row in df_communes.iterrows()
    }

    dfs = []
    for file in files:
        logging.info(f"Processing weather file: {file}")
        df = pd.read_csv(file)
        # Replace commune names with INSEE code
        if "commune" in df.columns:
            df["com"] = df["commune"].map(name_to_code)
            df = df.drop(columns=["commune"])
        # Add date from filename if not present
        if "date" not in df.columns:
            m = re.search(r"weather_history_(\d{4})_(\d{2})_(\d{2})", file)
            if m:
                date_str = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
                logging.info(f"Extracted date {date_str} from filename {file}")
                df["date"] = date_str
            else:
                logging.warning(f"Could not extract date from filename: {file}")
        else:
            logging.info(f"Date column already present in {file}")
        logging.info(f"File {file} rows: {len(df)}")
        dfs.append(df)
    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        # Ensure 'date' is in datetime format
        df_all["date"] = pd.to_datetime(df_all["date"])
        # Save merged file
        output_path = os.path.join(weather_dir, "weather_history_by_day.csv")
        df_all.to_csv(output_path, index=False)
        logging.info(
            f"Merged weather history written to {output_path} with {len(df_all)} rows."
        )
    else:
        logging.warning("No weather history files found to merge.")


def _create_final_dataset():
    # 📥 Charger les données
    df_accidents = pd.read_csv("/tmp/s3_files/df_accidents_all.csv", sep=",")
    df_meteo = pd.read_csv("/tmp/s3_files/weather_history_by_day.csv")

    # 🗓️ Créer une colonne "date" standardisée dans les deux DataFrames
    df_accidents["date"] = pd.to_datetime(
        df_accidents[["an", "mois", "jour"]].astype(str).agg("-".join, axis=1)
    )
    df_meteo["date"] = pd.to_datetime(df_meteo["date"])

    ### ### DÉBUT DE LA LOGIQUE CORRIGÉE ### ###

    # 1. Nettoyer le DataFrame météo pour garantir UNE seule ligne par commune et par jour
    logging.info(f"Taille de df_meteo avant nettoyage : {df_meteo.shape}")

    # Supprimer les lignes où les clés de fusion sont manquantes
    df_meteo.dropna(subset=["date", "com"], inplace=True)

    # Garder uniquement la PREMIÈRE ligne pour chaque combinaison de 'date' et 'com'
    df_meteo.drop_duplicates(subset=["date", "com"], keep="first", inplace=True)

    logging.info(f"Taille de df_meteo après nettoyage : {df_meteo.shape}")

    # 2. Préparer les clés de fusion et effectuer la fusion à gauche
    logging.info("Fusion des données accidents avec les données météo nettoyées...")

    # S'assurer que les types des clés sont identiques
    df_accidents.dropna(
        subset=["com"], inplace=True
    )  # On enlève les accidents sans commune
    df_accidents["com"] = df_accidents["com"].astype(int)
    df_meteo["com"] = df_meteo["com"].astype(int)

    # La fusion à gauche garantit que toutes les lignes de df_accidents sont conservées
    df_final = pd.merge(df_accidents, df_meteo, on=["date", "com"], how="left")

    logging.info(f"Taille de df_accidents original : {df_accidents.shape}")
    logging.info(f"Taille de df_final après fusion : {df_final.shape}")

    ### ### FIN DE LA LOGIQUE CORRIGÉE ### ###

    # 💾 Sauvegarder et téléverser le fichier final
    filename = "training_ml.csv"
    full_path_to_file = f"/tmp/s3_files/{filename}"
    df_final.to_csv(full_path_to_file, index=False, sep=",", encoding="utf-8")

    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_key = f"{S3_PATH}/db/{filename}"
    s3_hook.load_file(
        filename=full_path_to_file,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    logging.info(f"Fichier final pour l'entraînement ML sauvegardé sur S3 : {s3_key}")


with DAG(
    "elt_dag",
    default_args=dag_default_args,
    description="Extract, load and transform data for training",
    schedule_interval="0 0 30 12 *",
    tags=["ELT"],
) as dag:
    logging.info("transform_raw_data")

    start = BashOperator(task_id="start", bash_command="echo 'Start!'")

    with TaskGroup(
        group_id="extract_load_accident_branch"
    ) as extract_load_accident_branch:
        fetch_accident_data = PythonOperator(
            task_id="fetch_accident_data",
            python_callable=_fetch_accident_data,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        [fetch_accident_data]

    with TaskGroup(group_id="extract_load_meta_branch") as extract_load_meta_branch:
        fetch_public_holidays_data = PythonOperator(
            task_id="fetch_public_holidays_data",
            python_callable=_fetch_public_holidays_data,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        fetch_school_holidays_data = PythonOperator(
            task_id="fetch_school_holidays_data",
            python_callable=_fetch_school_holidays_data,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        fetch_fire_brigade = PythonOperator(
            task_id="fetch_fire_brigade",
            python_callable=_fetch_fire_brigade,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        fetch_communes = PythonOperator(
            task_id="fetch_communes",
            python_callable=_fetch_communes,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        # Ensure fetch_communes runs first, then the rest in parallel
        [
            fetch_communes,
            fetch_public_holidays_data,
            fetch_school_holidays_data,
            fetch_fire_brigade,
        ]

    with TaskGroup(group_id="fetch_data_branch") as fetch_data_branch:
        accident_data = PythonOperator(
            task_id="accident_data", python_callable=_fetch_files, op_args=["accidents"]
        )
        weather_data = PythonOperator(
            task_id="weather_data", python_callable=_fetch_files, op_args=["weather"]
        )
        meta_data = PythonOperator(
            task_id="meta_data", python_callable=_fetch_files, op_args=["meta"]
        )
        [accident_data, weather_data, meta_data]

    with TaskGroup(group_id="transformation_branch") as transformation_branch:
        group_accident_data = PythonOperator(
            task_id="group_accident_data",
            python_callable=_group_accident_data,
            provide_context=True,
        )
        group_data = PythonOperator(
            task_id="group_data", python_callable=_group_data, provide_context=True
        )
        add_weather_history = PythonOperator(
            task_id="add_weather_history", python_callable=_add_weather_history
        )
        create_final_dataset = PythonOperator(
            task_id="create_final_dataset", python_callable=_create_final_dataset
        )
        transfer_to_snowflake = S3ToSnowflakeOperator(
            task_id="transfer_s3_to_snowflake",
            bucket=S3_BUCKET_NAME,
            key=f"{S3_PATH}/db/training_ml.csv",
            schema="public",
            table="accidents",
            snowflake_conn_id="snowflake_default",
            aws_conn_id="aws_default",
        )
        (
            group_accident_data
            >> group_data
            >> add_weather_history
            >> create_final_dataset
            >> transfer_to_snowflake
        )

    end = BashOperator(task_id="end", bash_command="echo 'End!'")

    (
        start
        >> [extract_load_accident_branch, extract_load_meta_branch]
        >> fetch_data_branch
        >> transformation_branch
        >> end
    )
    # start >> [transformation_branch] >> end
