from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule="@once",
    catchup=False
) as dag:
    load_stage = SnowflakeOperator(
        task_id='load_stage',
        sql="CALL STAGE.LOAD_WEATHER_STAGE();",
        snowflake_conn_id='snowflake_conn'
    )

    trigger_final = TriggerDagRunOperator(
        task_id='trigger_weather_final_etl_pipeline',
        trigger_dag_id='weather_final_etl_pipeline'
    )

    load_stage >> trigger_final
