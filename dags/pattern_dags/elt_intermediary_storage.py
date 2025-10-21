"""
## Simple ELT DAG loading data from the Open-Meteo API to a Postgres database

This DAG extracts weather data from the Open-Meteo API,
loads it into a Postgres database and transforms it, using an ELT pattern.
It passes the data through XCom between extract and transform.
"""

import os
import json
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task, chain, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ------------------- #
# DAG-level variables #
# ------------------- #

DAG_ID = os.path.basename(__file__).replace(".py", "")

_AWS_CONN_ID = os.getenv("MINIO_CONN_ID", "minio_local")
_S3_BUCKET = os.getenv("S3_BUCKET", "open-meteo-etl")

_POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
_POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "postgres")
_POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
_POSTGRES_IN_TABLE = os.getenv("POSTGRES_WEATHER_TABLE_IN", f"in_weather_data_{DAG_ID}")
_POSTGRES_TRANSFORMED_TABLE = os.getenv(
    "POSTGRES_WEATHER_TABLE_TRANSFORMED", f"model_weather_data_{DAG_ID}"
)
_SQL_DIR = f"{os.getenv('AIRFLOW_HOME')}/include/sql/pattern_dags/{DAG_ID}"

_EXTRACT_TASK_ID = "extract"


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
    tags=["Patterns", "ELT", "XCom"],  # add tags in the UI
    params={
        "coordinates": Param({"latitude": 46.9481, "longitude": 7.4474}, type="object")
    },  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
    template_searchpath=[str(_SQL_DIR)],  # path to the SQL templates
)
def elt_intermediary_storage():

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

    _create_in_table_if_not_exists = SQLExecuteQueryOperator(
        task_id="create_in_table_if_not_exists",
        conn_id=_POSTGRES_CONN_ID,
        database=_POSTGRES_DATABASE,
        sql="create_in_table_if_not_exists.sql",
        params={"schema": _POSTGRES_SCHEMA, "table": _POSTGRES_IN_TABLE},
    )

    _create_model_table_if_not_exists = SQLExecuteQueryOperator(
        task_id="create_model_table_if_not_exists",
        conn_id=_POSTGRES_CONN_ID,
        database=_POSTGRES_DATABASE,
        sql="create_model_table_if_not_exists.sql",
        params={"schema": _POSTGRES_SCHEMA, "table": _POSTGRES_TRANSFORMED_TABLE},
    )

    @task(task_id=_EXTRACT_TASK_ID)
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
        dag_run_timestamp = context["ts"]
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id

        url = url.format(latitude=latitude, longitude=longitude)

        response = requests.get(url).json()

        response_bytes = json.dumps(response).encode("utf-8")

        # Save the data to S3
        hook = S3Hook(aws_conn_id=_AWS_CONN_ID)
        hook.load_bytes(
            bytes_data=response_bytes,
            key=f"{dag_id}/{task_id}/{dag_run_timestamp}.json",
            bucket_name=_S3_BUCKET,
            replace=True,
        )

    _extract = extract()

    @task
    def load(**context):
        """
        Load the data from S3 to Postgres
        """

        dag_run_timestamp = context["ts"]
        dag_id = context["dag"].dag_id
        upstream_task_id = _EXTRACT_TASK_ID

        print(f"{dag_id}/{upstream_task_id}/{dag_run_timestamp}.json")

        s3_hook = S3Hook(aws_conn_id=_AWS_CONN_ID)
        response = s3_hook.read_key(
            key=f"{dag_id}/{upstream_task_id}/{dag_run_timestamp}.json",
            bucket_name=_S3_BUCKET,
        )
        api_response = json.loads(response)

        postgres_hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)

        insert_sql = f"""
        INSERT INTO {_POSTGRES_SCHEMA}.{_POSTGRES_IN_TABLE} (raw_data)
        VALUES (%s::jsonb);
        """

        postgres_hook.run(insert_sql, parameters=(json.dumps(api_response),))

    _transform = SQLExecuteQueryOperator(
        task_id="transform_data",
        conn_id=_POSTGRES_CONN_ID,
        sql="transform.sql",
        params={
            "schema": _POSTGRES_SCHEMA,
            "in_table": _POSTGRES_IN_TABLE,
            "out_table": _POSTGRES_TRANSFORMED_TABLE,
        },
    )

    chain([_create_in_table_if_not_exists, _extract], load(), _transform)
    chain(_create_model_table_if_not_exists, _transform)


elt_intermediary_storage()
