from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv("/usr/local/airflow/.env", override=True)  # Adjust path

def load_weather_to_snowflake():
    df = pd.read_csv('/usr/local/airflow/include/weather_data.csv')
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
            MERGE INTO WEATHER_RAW AS target
            USING (SELECT %s AS CITY, %s AS TEMPERATURE, %s AS HUMIDITY, %s AS WEATHER, %s AS TIMESTAMP) AS source
            ON target.CITY = source.CITY AND target.TIMESTAMP = source.TIMESTAMP
            WHEN MATCHED THEN UPDATE SET
                target.TEMPERATURE = source.TEMPERATURE,
                target.HUMIDITY = source.HUMIDITY,
                target.WEATHER = source.WEATHER
            WHEN NOT MATCHED THEN
                INSERT (CITY, TEMPERATURE, HUMIDITY, WEATHER, TIMESTAMP)
                VALUES (source.CITY, source.TEMPERATURE, source.HUMIDITY, source.WEATHER, source.TIMESTAMP);
            """, (row['city'], row['temperature'], row['humidity'], row['weather'], row['timestamp']))
    conn.commit()
    cursor.close()
    conn.close()

default_args = {'start_date': datetime(2023, 1, 1)}

with DAG(
    dag_id='weather_to_snowflake',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['weather', 'snowflake']
) as dag:

    load_task = PythonOperator(
        task_id='load_weather',
        python_callable=load_weather_to_snowflake,
    )

    trigger_etl = TriggerDagRunOperator(
        task_id='trigger_weather_etl_pipeline',
        trigger_dag_id='weather_etl_pipeline',
    )

    load_task >> trigger_etl
