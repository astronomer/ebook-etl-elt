"""
## ELT DAG extracting data from 3 API calls in parallel using Dynamic Tasks

This DAG extracts weather data from the Open-Meteo API, loads it into a 
ostgres database and transforms it, using an ELT pattern.
It extracts data from 3 different locations in parallel using dynamic task mapping.
"""

import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ------------------- #
# DAG-level variables #
# ------------------- #

DAG_ID = os.path.basename(__file__).replace(".py", "")

_POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
_POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "postgres")
_POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
_POSTGRES_IN_TABLE = os.getenv("POSTGRES_WEATHER_TABLE_IN", f"in_weather_data_{DAG_ID}")
_POSTGRES_TRANSFORMED_TABLE = os.getenv(
    "POSTGRES_WEATHER_TABLE_TRANSFORMED", f"model_weather_data_{DAG_ID}"
)
_SQL_DIR = os.path.join(
    os.path.dirname(__file__), f"../../include/sql/pattern_dags/{DAG_ID}"
)


# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 9, 23),  # date after which the DAG can be scheduled
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,  # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_active_runs=1,  # maximum number of active DAG runs
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after 5 consecutive failed runs, experimental
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "Astro",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": timedelta(seconds=30),  # tasks wait 30s in between retries
    },
    tags=["Patterns", "ELT", "Postgres", "XCom"],  # add tags in the UI
    params={
        "set_of_coordinates": Param(
            {
                "Location 1": {"latitude": 46.9480, "longitude": 7.4474},
                "Location 2": {"latitude": 38.4272, "longitude": 14.9524},
                "Location 3": {"latitude": 46.9480, "longitude": 2.9916},
            },
            type="object",
        )
    },  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
    template_searchpath=[_SQL_DIR],  # path to the SQL templates
)
def elt_dtm():

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
        params={"schema": _POSTGRES_SCHEMA, "table": _POSTGRES_IN_TABLE},
    )

    @task
    def get_sets_of_coordinates(**context):
        """
        Get the set of coordinates from the params
        """
        coordinate_dict = context["params"]["set_of_coordinates"]
        list_of_coordinates = []
        for k, v in coordinate_dict.items():
            list_of_coordinates.append(v)

        return list_of_coordinates

    _get_sets_of_coordinates = get_sets_of_coordinates()

    @task
    def extract(coordinates: dict):
        """
        Extract data from the Open-Meteo API
        Returns:
            dict: The full API response
        """
        import requests

        url = os.getenv("WEATHER_API_URL")

        latitude = coordinates["latitude"]
        longitude = coordinates["longitude"]

        url = url.format(latitude=latitude, longitude=longitude)

        response = requests.get(url)

        return response.json()

    _extract = extract.expand(coordinates=_get_sets_of_coordinates)

    @task
    def create_sql_statements(data, schema, table):
        """
        Create SQL statements to load the data
        """
        data_json = json.dumps(data)

        return f"""
            INSERT INTO {schema}.{table} (raw_data)
            VALUES ('{data_json}'::jsonb);
            """

    _create_sql_statements = create_sql_statements.partial(
        schema=_POSTGRES_SCHEMA, table=_POSTGRES_IN_TABLE
    ).expand(data=_extract)

    _load = SQLExecuteQueryOperator.partial(
        task_id="load_raw_data",
        conn_id=_POSTGRES_CONN_ID,
    ).expand(sql=_create_sql_statements)

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

    chain([_create_table_if_not_exists, _extract], _load, _transform)


elt_dtm()
