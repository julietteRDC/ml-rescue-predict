# 🚀 Jedha Lead

## Airflow Configuration 🔐

### Installation & Setup 🚀

1. Add AIRFLOW_UID env variable in .env file.

```bash
cd airflow
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

2. Start the Airflow services:

```bash
docker compose up airflow-init
docker-compose up -d --build
```

4. Access Airflow web interface:

````
http://localhost:8080
Username: airflow
Password: airflow

## Requirements

- docker
- poetry

## Links to services

mlflow : [mlflow](https://huggingface.co/spaces/littlerobinson/mlflow)

## Deployment

### MLFlow

To deploy the MLFlow project:

```bash
cd mlflow

# Create local python environment
poetry install

# Build docker image
docker build . mlflow_image_name

# Deploy images on your server
````

#### Variables

Set the following variables in the Airflow Admin Interface (Admin > Variables):

```
AWS_DEFAULT_REGION    # Your AWS region (e.g., eu-west-1)
WEATHERMAP_API        # Your WeatherMap API key
S3BucketName          # Your S3 bucket name
```

### Connections

Configure the following connections in Airflow (Admin > Connections):

1. AWS Connection (`aws_id`):

   - Conn Type: Amazon Web Services
   - Configure with your AWS credentials

2. Snowflake Connection (`snowflake_id`):
   - Conn Type: Snowflake
   - Configure with your database credentials

### Database structure

```sql
create or replace TABLE RESCUE_PREDICT_DB.PUBLIC."accidents" (
"com" NUMBER(38,0),
"population" NUMBER(38,0),
"jour" NUMBER(38,0),
"mois" NUMBER(38,0),
"an" NUMBER(38,0),
"public_holidays" BOOLEAN,
"zone_a" BOOLEAN,
"zone_b" BOOLEAN,
"zone_c" BOOLEAN,
"dep" NUMBER(38,0),
"nombre_d_accidents" NUMBER(38,0),
"date" VARCHAR(16777216),
"temp" FLOAT,
"feels_like" FLOAT,
"temp_min" FLOAT,
"temp_max" FLOAT,
"pressure" FLOAT,
"humidity" FLOAT,
"wind_speed" FLOAT,
"clouds_all" FLOAT,
"execution_date" VARCHAR(16777216),
"dag_id" VARCHAR(16777216),
"task_id" VARCHAR(16777216)
);
```

## API

### Installation & Setup 🚀

```bash
source secrets.sh
cd api
docker build -t rescue-predict-api .
chmod +x run.sh
./run.sh
```
