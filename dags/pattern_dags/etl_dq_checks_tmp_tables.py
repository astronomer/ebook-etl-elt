"""
## ETL with Data Quality Checks and Temporary Tables

This DAG will perform the following steps:
1. Extract weather data from the Open-Meteo API
2. Transform the data
3. Load the data into a temporary table in Postgres
4. Perform stopping data quality checks on the temporary table
5. Swap the temporary table with the permanent table (and creating a backup of the old permanent table)
6. Drop the temporary table
7. Perform stopping data quality checks on the new permanent table
8. Drop the backup table
9. Perform additional warning data quality checks on the new permanent table
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task, task_group, chain, Param
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLTableCheckOperator,
)
from include.col_orders import WEATHER_COL_ORDER

DAG_ID = os.path.basename(__file__).replace(".py", "")

_POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
_POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "postgres")
_POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
_POSTGRES_TMP_TABLE = os.getenv(
    "POSTGRES_WEATHER_TABLE_TMP", f"tmp_weather_data_{DAG_ID}"
)
_POSTGRES_BACKUP_TABLE = os.getenv(
    "POSTGRES_WEATHER_TABLE_BACKUP", f"backup_weather_data_{DAG_ID}"
)
_POSTGRES_TRANSFORMED_TABLE = os.getenv(
    "POSTGRES_WEATHER_TABLE_TRANSFORMED", f"model_weather_data_{DAG_ID}"
)
_SQL_DIR = os.path.join(
    os.path.dirname(__file__), f"../../include/sql/pattern_dags/{DAG_ID}"
)


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
        "postgres_conn_id": _POSTGRES_CONN_ID,
        "conn_id": _POSTGRES_CONN_ID,
    },
    tags=["Patterns", "ETL", "Postgres", "XCom"],  # add tags in the UI
    params={
        "coordinates": Param({"latitude": 46.9481, "longitude": 7.4474}, type="object")
    },  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
    template_searchpath=[_SQL_DIR],  # path to the SQL templates
)
def etl_dq_checks_tmp_tables():

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
    def transform_in_flight(api_response: dict, **context) -> dict:
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

    _extract = extract()
    _transform = transform_in_flight(_extract)

    @task_group
    def load():
        _create_tmp_table_if_not_exists = SQLExecuteQueryOperator(
            task_id="create_tmp_table_if_not_exists",
            database=_POSTGRES_DATABASE,
            sql="create_tmp_table_if_not_exists.sql",
            params={"schema": _POSTGRES_SCHEMA, "table": _POSTGRES_TMP_TABLE},
        )

        @task
        def load_to_tmp(transformed_data: dict):
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

            with open(f"{_SQL_DIR}/copy_insert.sql") as f:
                sql = f.read()
            sql = sql.replace("{schema}", _POSTGRES_SCHEMA)
            sql = sql.replace("{table}", _POSTGRES_TMP_TABLE)

            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.copy_expert(sql=sql, file=csv_buffer)
            conn.commit()
            cursor.close()
            conn.close()

        _load_to_tmp = load_to_tmp(_transform)

        @task_group
        def test_tmp_table():

            SQLTableCheckOperator(
                task_id="test_table",
                retry_on_failure="True",
                table=f"{_POSTGRES_SCHEMA}.{_POSTGRES_TMP_TABLE}",
                checks={
                    "row_count_check": {"check_statement": "COUNT(*) > 10"},
                },
            )

        swap = SQLExecuteQueryOperator(
            task_id="swap",
            sql="swap.sql",
            params={
                "schema": _POSTGRES_SCHEMA,
                "tmp_table": _POSTGRES_TMP_TABLE,
                "perm_table": _POSTGRES_TRANSFORMED_TABLE,
            },
        )

        drop_tmp = SQLExecuteQueryOperator(
            task_id="drop_tmp",
            sql=f"DROP TABLE IF EXISTS {_POSTGRES_SCHEMA}.{_POSTGRES_TMP_TABLE};",
        )

        @task_group
        def test_perm_table():
            SQLColumnCheckOperator(
                task_id="test_cols",
                retry_on_failure="True",
                table=f"{_POSTGRES_SCHEMA}.{_POSTGRES_TRANSFORMED_TABLE}",
                column_mapping={
                    "month": {"min": {"geq_to": 1}, "max": {"leq_to": 12}},
                    "day": {"min": {"geq_to": 1}, "max": {"leq_to": 31}},
                },
                accept_none="True",
            )

            SQLTableCheckOperator(
                task_id="test_table",
                retry_on_failure="True",
                table=f"{_POSTGRES_SCHEMA}.{_POSTGRES_TRANSFORMED_TABLE}",
                checks={
                    "row_count_check": {"check_statement": "COUNT(*) > 10"},
                },
            )

        drop_backup = SQLExecuteQueryOperator(
            task_id="drop_backup",
            sql=f"DROP TABLE IF EXISTS {_POSTGRES_SCHEMA}.{_POSTGRES_BACKUP_TABLE};",
        )

        chain(
            _create_tmp_table_if_not_exists,
            _load_to_tmp,
            test_tmp_table(),
            swap,
            [drop_tmp, test_perm_table()],
            drop_backup,
        )

        @task_group
        def validate():
            test_cols = SQLColumnCheckOperator(
                task_id="test_cols",
                retry_on_failure="True",
                table=f"{_POSTGRES_SCHEMA}.{_POSTGRES_TRANSFORMED_TABLE}",
                column_mapping={
                    "temperature_2m": {"min": {"geq_to": -10}, "max": {"leq_to": 50}},
                },
                accept_none="True",
            )

            @task(trigger_rule="all_done")
            def sql_check_done():
                return "Additional data quality checks are done!"

            test_cols >> sql_check_done()

        swap >> validate()

    load()


etl_dq_checks_tmp_tables()
