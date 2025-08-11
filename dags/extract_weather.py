from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv("/usr/local/airflow/.env", override=True)  # Adjust the path as needed

def fetch_weather():
    API_KEY = os.getenv("WEATHER_API_KEY")
    BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
    cities = ["London", "New York", "Mumbai", "Tokyo", "Paris", "Chennai", "Cuddalore", "Puducherry", "Bangalore", "Coimbatore", "Ooty"]
    weather_data = []

    for city in cities:
        params = {
            "q": city,
            "appid": API_KEY,
            "units": "metric"
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            weather_data.append({
                "city": city,
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather": data["weather"][0]["main"],
                "timestamp": datetime.utcnow().isoformat()
            })
        else:
            print(f"Failed for {city}: {response.status_code}")

    df = pd.DataFrame(weather_data)
    df.to_csv("/usr/local/airflow/include/weather_data.csv", mode='a', header=False, index=False)
    print("Weather data saved to CSV")

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="extract_weather_dag",
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["weather"],
) as dag:
    task1 = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather,
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_weather_to_snowflake",
        trigger_dag_id="weather_to_snowflake",
    )

    task1 >> trigger_next
