"""
## Simple ETL DAG loading data from the Open-Meteo API to a Postgres database

This DAG extracts weather data from the Open-Meteo API, transforms it, and
loads it into a Postgres database in an ETL pattern.
It passes the data through XComs.
"""

import os
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import dag, task, chain, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from include.col_orders import WEATHER_COL_ORDER

# ------------------- #
# DAG-level variables #
# ------------------- #

DAG_ID = os.path.basename(__file__).replace(".py", "")

_POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
_POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "postgres")
_POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
_POSTGRES_TRANSFORMED_TABLE = os.getenv(
    "POSTGRES_WEATHER_TABLE_TRANSFORMED", f"model_weather_data_{DAG_ID}"
)
_SQL_DIR = Path(os.getenv("AIRFLOW_HOME")) / "include" / f"sql/pattern_dags/{DAG_ID}"


# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2025, 8, 1),  # date after which the DAG can be scheduled
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    max_active_runs=1,  # maximum number of active DAG runs
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after 5 consecutive failed runs, experimental
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "Astro",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": timedelta(seconds=30),  # tasks wait 30s in between retries
    },
    tags=["Patterns", "ETL", "XCom"],  # add tags in the UI
    params={
        "coordinates": Param({"latitude": 46.9481, "longitude": 7.4474}, type="object")
    },  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
    template_searchpath=[str(_SQL_DIR)],  # path to the SQL templates
)
def etl_xcom():

    # ---------------- #
    # Task Definitions #
    # ---------------- #
    # the @task decorator turns any Python function into an Airflow task
    # any @task decorated function that is called inside the @dag decorated
    # function is automatically added to the DAG.
    # if one exists for your use case you can use traditional Airflow operators
    # and mix them with @task decorators. Checkout registry.astronomer.io for available operators
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    _create_table_if_not_exists = SQLExecuteQueryOperator(
        task_id="create_table_if_not_exists",
        conn_id=_POSTGRES_CONN_ID,
        database=_POSTGRES_DATABASE,
        sql="create_table_if_not_exists.sql",
        params={"schema": _POSTGRES_SCHEMA, "table": _POSTGRES_TRANSFORMED_TABLE},
    )

    @task
    def extract(**context):
        """
        Extract data from the Open-Meteo API
        Returns:
            dict: The full API response
        """
        import requests

        url = os.getenv("WEATHER_API_URL")

        coordinates = context["params"]["coordinates"]
        latitude = coordinates["latitude"]
        longitude = coordinates["longitude"]

        url = url.format(latitude=latitude, longitude=longitude)

        response = requests.get(url)

        return response.json()

    @task
    def transform(api_response: dict, **context) -> dict:
        """
        Transform the data
        Args:
            api_response (dict): The full API response
        Returns:
            dict: The transformed data
        """

        time = api_response["hourly"]["time"]
        dag_run_timestamp = context["ts"]

        transformed_data = {
            "temperature_2m": api_response["hourly"]["temperature_2m"],
            "relative_humidity_2m": api_response["hourly"]["relative_humidity_2m"],
            "precipitation_probability": api_response["hourly"][
                "precipitation_probability"
            ],
            "timestamp": time,
            "date": [
                datetime.strptime(x, "%Y-%m-%dT%H:%M").date().strftime("%Y-%m-%d")
                for x in time
            ],
            "day": [datetime.strptime(x, "%Y-%m-%dT%H:%M").day for x in time],
            "month": [datetime.strptime(x, "%Y-%m-%dT%H:%M").month for x in time],
            "year": [datetime.strptime(x, "%Y-%m-%dT%H:%M").year for x in time],
            "last_updated": [dag_run_timestamp for i in range(len(time))],
            "latitude": [api_response["latitude"] for i in range(len(time))],
            "longitude": [api_response["longitude"] for i in range(len(time))],
        }

        return transformed_data

    @task
    def load(transformed_data: dict):
        """
        Load the data to the destination without using a temporary CSV file.
        Args:
            transformed_data (dict): The transformed data
        """
        import csv
        import io
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(WEATHER_COL_ORDER)
        rows = zip(*[transformed_data[col] for col in WEATHER_COL_ORDER])
        writer.writerows(rows)

        csv_buffer.seek(0)

        with open(f"{str(_SQL_DIR)}/copy_insert.sql") as f:
            sql = f.read()
        sql = sql.replace("{schema}", _POSTGRES_SCHEMA)
        sql = sql.replace("{table}", _POSTGRES_TRANSFORMED_TABLE)

        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.copy_expert(sql=sql, file=csv_buffer)
        conn.commit()
        cursor.close()
        conn.close()

    _extract = extract()
    _transform = transform(api_response=_extract)
    _load = load(transformed_data=_transform)
    chain(_transform, _load)
    chain(_create_table_if_not_exists, _load)


etl_xcom()
