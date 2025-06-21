import logging
import os

import snowflake.connector
from dotenv import load_dotenv
from sklearn.compose import ColumnTransformer  # Unused import
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, f1_score, make_scorer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier

import mlflow
from mlflow.models import infer_signature

from sklearn.model_selection import cross_validate

logging.basicConfig(level=logging.INFO)


def load_data_from_snowflake():
    # Variables d'environnement
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    role = os.getenv("SNOWFLAKE_ROLE")

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

    query_limit = 1000000
    query = f"""
        SELECT *
        FROM rescue_predict_db.public."accidents"
        ORDER BY "an" DESC, "mois" DESC, "jour" DESC
        LIMIT {query_limit}
    """

    # Exécution de la requête et conversion en DataFrame
    cs = ctx.cursor()
    try:
        cs.execute(query)
        df = cs.fetch_pandas_all()
        logging.info("Data successfully loaded from Snowflake.")
        return df
    finally:
        cs.close()
        ctx.close()


if __name__ == "__main__":
    logging.info("ML rescue predict start app")

    logging.info("init mlflow credentials")
    # load the environment variables
    load_dotenv()

    # load data
    logging.info("Load data for training")
    rescue_predict_df = load_data_from_snowflake()
    logging.info(rescue_predict_df.head(2))

    # Init for MLflow
    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI")
    logging.info(f"call mlflow uri: {MLFLOW_TRACKING_URI}")
    MLFLOW_LOGGED_MODEL = os.getenv("MLFLOW_LOGGED_MODEL")
    logging.info(f"call mlflow logged path for artifact: {MLFLOW_LOGGED_MODEL}")
    MLFLOW_EXPERIMENT_ID = os.getenv("MLFLOW_EXPERIMENT_ID")

    EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME")
    mlflow.set_experiment(EXPERIMENT_NAME)
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    experiment_id = experiment.experiment_id

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    logging.info(f"experiment name {EXPERIMENT_NAME}")
    mlflow.sklearn.autolog()

    with mlflow.start_run():
        logging.info("Preparing data")
        target_column = "nombre_d_accidents"

        # Binarize the target column: 0 if no accidents, 1 if one or more accidents
        rescue_predict_df[target_column] = (
            rescue_predict_df[target_column] > 0
        ).astype(int)

        # Delete uneeded columns (snowflake columns and others)
        uneeded_columns = [
            "task_id",
            "dag_id",
            "execution_date",
            "date",
        ]

        # drop uneeded columns
        rescue_predict_df = rescue_predict_df.drop(
            columns=uneeded_columns, errors="ignore"
        )

        # prepare data for training
        X = rescue_predict_df.drop(columns=[target_column])  # Features
        y = rescue_predict_df[target_column]  # Target

        numerical_columns = X.select_dtypes(include=["number"]).columns
        categorical_columns = X.select_dtypes(exclude=["number"]).columns

        preprocessor = ColumnTransformer(
            [
                ("num", StandardScaler(), numerical_columns),
                ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_columns),
            ]
        )

        pipeline = Pipeline(
            [
                ("preprocessing", preprocessor),
                (
                    "classifier",
                    XGBClassifier(
                        objective="binary:logistic",
                        eval_metric="logloss",
                        n_estimators=100,
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        )

        # Update int columns to float 64
        X = X.astype({col: "float64" for col in X.select_dtypes(include="int").columns})

        logging.info("Splitting data")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        logging.info("Training model")
        pipeline.fit(X_train, y_train)

        # Perform cross-validation
        logging.info("Performing cross-validation...")
        scoring = {
            "accuracy": "accuracy",
            "f1_score": make_scorer(f1_score, average="weighted"),
        }
        cv_results = cross_validate(
            pipeline, X, y, cv=5, scoring=scoring, return_train_score=False
        )

        # Log cross-validation metrics to MLflow
        logging.info("Logging cross-validation metrics to MLflow...")
        for metric in cv_results.keys():
            if metric.startswith("test_"):
                mlflow.log_metric(f"cv_{metric}", cv_results[metric].mean())
                logging.info(
                    f"Logged metric: cv_{metric} with value: {cv_results[metric].mean()}"
                )

        logging.info("Evaluating model")
        y_pred = pipeline.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(
            y_test, y_pred, average="weighted"
        )  # Use 'weighted' for F1-score in binary classification with potential imbalance

        logging.info(f"Accuracy: {acc:.4f}, F1 Score: {f1:.4f}")
        mlflow.log_metric("accuracy", acc)

        mlflow.log_metric("f1_score", f1)

        # Define input example and model signature
        # input_example = X_train.iloc[:1].to_dict(orient="list")
        input_example = X_train.sample(5, random_state=42).to_dict(orient="list")

        signature = infer_signature(X_train, pipeline.predict(X_train))

        # For mlflow V3 in server
        # mlflow.sklearn.log_model(
        #     sk_model=pipeline,
        #     name="rescue_predict_model",
        #     signature=signature,
        #     input_example=input_example,
        # )

        mlflow.sklearn.log_model(
            sk_model=pipeline,
            artifact_path="model",
            input_example=input_example,
            registered_model_name=None,  # descativate for mlflow V2 in server
        )

        logging.info("MLflow run completed")
