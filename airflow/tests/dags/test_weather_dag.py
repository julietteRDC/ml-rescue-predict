import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import json
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load test variables from config/variables.json
with open(os.path.join(os.path.dirname(__file__), '../../config/variables.json')) as f:
    config_vars = json.load(f)
OPENWEATHERMAP_API = config_vars["OPENWEATHERMAP_API"]
S3_BUCKET_NAME = config_vars["S3BucketName"]
S3_PATH = config_vars["S3Path"]


def fetch_weather_data():
    import requests
    import pandas as pd
    from datetime import datetime
    api_key = OPENWEATHERMAP_API
    city = "Paris"  # Change as needed
    s3_bucket = S3_BUCKET_NAME
    s3_key = f"{S3_PATH}meta/weather_history.csv"
    local_path = "/tmp/weather_history.csv"

    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame([
        {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "temp": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "temp_min": data["main"]["temp_min"],
            "temp_max": data["main"]["temp_max"],
            "pressure": data["main"]["pressure"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"],
            "clouds_all": data["clouds"]["all"],
        }
    ])

    return df


def test_weather_data():
    df = fetch_weather_data()

    logging.debug("Results!")
    logging.debug(df.head())  # For debugging purposes
