# **Real-Time Weather Data Pipeline with Snowflake & Airflow (Astro)**

## **Overview**

This project is an **Apache Airflow–orchestrated ETL pipeline** that automates the **daily extraction, loading, and transformation** of weather data from the OpenWeather API into a Snowflake Data Warehouse, using the **Medallion Architecture** pattern:

- **RAW (Bronze)** – Raw, ingested CSV data from the API.
- **STAGE (Silver)** – Cleaned, validated and standardized data.
- **FINAL (Gold)** – Curated, analytics‑ready data.

The workflow is split into **four DAGs** connected with `TriggerDagRunOperator`, so they run sequentially in one daily chain starting from the first scheduled DAG.

***

## **Architecture**

```
extract_weather_dag (@daily)
    ↓ Trigger
weather_to_snowflake (RAW load & MERGE)
    ↓ Trigger
weather_etl_pipeline (RAW → STAGE)
    ↓ Trigger
weather_final_etl_pipeline (STAGE → FINAL)
```


***

## **DAGs**

### 1️ extract_weather_dag

**File:** `extract_weather.py`
**Schedule:** `@daily` (only DAG that is time‑scheduled)
**Purpose:** Pulls current weather data for multiple cities from the OpenWeather API.

**Steps:**

- Reads API key from `.env`
- Fetches weather data for a city list
- Saves results to `/usr/local/airflow/include/weather_data.csv`
- **Triggers** `weather_to_snowflake` when complete

***

### 2️ weather_to_snowflake

**File:** `weather_pipeline.py`
**Schedule:** `None` – runs only when triggered by DAG 1
**Purpose:** Load daily extracted data into the **RAW (Bronze)** table in Snowflake

**Steps:**

- Reads CSV file
- Connects to Snowflake via credentials in `.env`
- Performs `MERGE` upsert into `WEATHER_RAW` (avoids costly truncates)
- **Triggers** `weather_etl_pipeline`

***

### 3️ weather_etl_pipeline

**File:** `weather_etl_dag.py`
**Schedule:** `@once` – only runs when triggered by DAG 2
**Purpose:** Transform RAW → STAGE (**Bronze → Silver**)

**Steps:**

- Executes stored procedure:

```sql
CALL STAGE.LOAD_WEATHER_STAGE();
```

which cleans, deduplicates, and standardizes data
- **Triggers** `weather_final_etl_pipeline`

***

### 4️ weather_final_etl_pipeline

**File:** `weather_to_final.py`
**Schedule:** `@once` – runs only when triggered by DAG 3
**Purpose:** Transform STAGE → FINAL (**Silver → Gold**)

**Steps:**

- Executes stored procedure:

```sql
CALL FINAL.LOAD_WEATHER_FINAL();
```

which aggregates and enriches data for analytics

***

## **Secrets \& Environment Variables**

- Secrets are stored in `.env`:

```
WEATHER_API_KEY=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=...
```

- Loaded in Python scripts using:

```python
from dotenv import load_dotenv
load_dotenv("/usr/local/airflow/.env", override=True)
```


***

## **Best Practices Implemented**

- **One DAG scheduled daily**; rest are event-triggered for sequential execution
- **MERGE** used instead of TRUNCATE+INSERT to reduce Snowflake costs
- **Medallion Architecture** ensures data traceability and maintainability
- **Secrets management** using `.env` instead of hardcoding credentials
- Heavy transformations pushed down to Snowflake stored procedures for performance

***

## **How to Run**

1. Set up `.env` file with your API key \& Snowflake credentials
2. Ensure all schemas, tables, and stored procedures (`WEATHER_RAW`, `STAGE.LOAD_WEATHER_STAGE`, `FINAL.LOAD_WEATHER_FINAL`) exist in Snowflake
3. Place DAG files in Airflow `dags/` folder
4. Start Airflow services and ensure scheduler is running
5. The pipeline starts daily from `extract_weather_dag` and triggers the rest of the DAGs automatically

***

## **Technology Stack**

- **Apache Airflow** – Workflow orchestration
- **Python** – Data extraction logic
- **Snowflake** – Cloud Data Warehouse (storage and transformation)
- **OpenWeather API** – Weather data source

***

Do you want me to also prepare a **diagram.png** in draw.io showing this trigger chain, so your README visually illustrates the pipeline? This would make your GitHub repo look much more professional and clear.

<div style="text-align: center">⁂</div>

[^1]: extract_weather.py

[^2]: weather_etl_dag.py

[^3]: weather_pipeline.py

[^4]: weather_to_final.py

