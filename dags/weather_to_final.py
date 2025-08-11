from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='weather_final_etl_pipeline',
    default_args=default_args,
    schedule='@once',
    catchup=False,
) as dag:

    load_final = SnowflakeOperator(
        task_id='load_final',
        sql="""
        CALL FINAL.LOAD_WEATHER_FINAL();

        """,
        snowflake_conn_id='snowflake_conn'
    )
