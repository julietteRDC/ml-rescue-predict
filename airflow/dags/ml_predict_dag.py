import logging
import os

import pandas as pd
import snowflake.connector

import mlflow
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago

# Airflow variables
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_ID = Variable.get("MLFLOW_EXPERIMENT_ID")
MLFLOW_LOGGED_MODEL = Variable.get("MLFLOW_LOGGED_MODEL")

# Airflow connections
aws_conn = BaseHook.get_connection("aws_default")
snowflake_conn = BaseHook.get_connection("snowflake_rescue_predict_db")


# Get AWS credentials
AWS_ACCESS_KEY_ID = aws_conn.login
AWS_SECRET_ACCESS_KEY = aws_conn.password
region_name = aws_conn.extra_dejson.get("region_name", "eu-west-3")

# Define default arguments for the DAG
default_args = {"owner": "airflow", "start_date": days_ago(1)}

# Define the DAG
dag = DAG(
    "mlflow_model_prediction_dag",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["predict"],
)


# Function to load data from the database
def load_data_from_db(ti):
    user = snowflake_conn.login
    password = snowflake_conn.password
    account = snowflake_conn.extra_dejson.get("account")
    warehouse = snowflake_conn.extra_dejson.get("warehouse")
    database = snowflake_conn.extra_dejson.get("database")
    schema = snowflake_conn.extra_dejson.get("schema") or snowflake_conn.schema
    role = snowflake_conn.extra_dejson.get("role")

    # Connexion
    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
        session_parameters={
            "CLIENT_TELEMETRY_ENABLED": False,
        },
    )

    query = """
        SELECT *
        FROM rescue_predict_db.public."accidents_predict"
        WHERE "prediction_score" IS NULL
        ORDER BY "an" ASC, "mois" ASC, "jour" ASC
    """
    # Exécution de la requête et conversion en DataFrame
    cs = ctx.cursor()
    try:
        cs.execute(query)
        df = cs.fetch_pandas_all()
        logging.info("Data successfully loaded from Snowflake.")
        ti.xcom_push(key="data", value=df.to_json())  # Stocker sous format JSON
    finally:
        cs.close()
        ctx.close()


# Function to make predictions and update the database
def make_and_update_predictions(ti):
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    # Load model
    model_uri = "models:/rescue-predict/2"

    ### ### DÉBUT DE LA CORRECTION 1/2 ### ###
    # Load the model using the scikit-learn flavor to get access to .predict_proba()
    model = mlflow.sklearn.load_model(model_uri)
    ### ### FIN DE LA CORRECTION 1/2 ### ###

    # Load data from XCom
    data_json = ti.xcom_pull(task_ids="load_data", key="data")
    df = pd.read_json(data_json)

    if df.empty:
        logging.info("No data to predict. Exiting prediction task.")
        return

    # Make predictions
    # The scikit-learn model expects the original column names (likely lowercase)
    probabilites = model.predict_proba(
        df.drop(columns=["nombre_d_accidents"], errors="ignore")
    )
    score_class = probabilites[:, 1]  # Probability of the positive class
    df["prediction_score"] = score_class
    df["prediction_score"] = (df["prediction_score"] * 100).round(4)

    ti.xcom_push(key="predict_data", value=df.to_json())

    # Now, convert column names to uppercase for the database update
    df.columns = [col.upper() for col in df.columns]

    logging.info(f"Dernière ligne avec prediction :\n{df.tail(1)}")

    # Update the database
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_rescue_predict_db")
    conn = snowflake_hook.get_conn()
    try:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                update_query = """
                    UPDATE rescue_predict_db.public."accidents_predict"
                    SET "prediction_score" = %s
                    WHERE "jour" = %s
                      AND "mois" = %s
                      AND "an" = %s
                      AND "dep" = %s
                      AND "com" = %s
                """
                cursor.execute(
                    update_query,
                    (
                        ### ### DÉBUT DE LA CORRECTION 2/2 ### ###
                        # The column name is PREDICTION_SCORE, not ACCIDENT_PREDICT
                        row["PREDICTION_SCORE"],
                        ### ### FIN DE LA CORRECTION 2/2 ### ###
                        int(row["JOUR"]),
                        int(row["MOIS"]),
                        int(row["AN"]),
                        str(row["DEP"]),
                        str(row["COM"]),
                    ),
                )
        conn.commit()
        logging.info(f"Successfully updated {len(df)} rows in Snowflake.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating Snowflake: {e}")
        raise
    finally:
        conn.close()


# Task to load the data
load_data_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data_from_db,
    provide_context=True,
    dag=dag,
)

# Task to make predictions and update the database
predict_and_update_task = PythonOperator(
    task_id="make_and_update_predictions",
    python_callable=make_and_update_predictions,
    provide_context=True,
    dag=dag,
)

# Define the task order
load_data_task >> predict_and_update_task
