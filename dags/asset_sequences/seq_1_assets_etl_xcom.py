"""
## Simple asset sequence creating 4 DAGs to perform an ETL pattern

These 4 asset decorated functions created 4 DAGs each containing one task
following a similar pattern as the regular etl_xcom DAG.

Note that passing the data between assets via XCom necessitates a Cross-DAG xcom pull
and other information like DAG params and timestamps are passed via the metadata
asset events.
"""

import os
from datetime import datetime

from airflow.sdk import Param, asset, Metadata, Asset
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.col_orders import WEATHER_COL_ORDER

# ------------------- #
# DAG-level variables #
# ------------------- #

SEQUENCE_NAME = os.path.basename(__file__).replace(".py", "")

_POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
_POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "postgres")
_POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
_POSTGRES_TRANSFORMED_TABLE = os.getenv(
    "POSTGRES_WEATHER_TABLE_TRANSFORMED", f"model_weather_data_{SEQUENCE_NAME}"
)
_SQL_DIR = f"{os.getenv('AIRFLOW_HOME')}/include/sql/asset_sequences/{SEQUENCE_NAME}"


# First asset definition
@asset(
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    tags=["assets", "seq_1", "XCom"],  # add tags in the UI
    params={
        "coordinates": Param({"latitude": 46.9481, "longitude": 7.4474}, type="object")
    },
)
def seq_1_start(context):
    yield Metadata(Asset("seq_1_start"), {
            "latitude": context["params"]["coordinates"]["latitude"],
            "longitude": context["params"]["coordinates"]["longitude"],
            "ts": context["dag_run"].run_after.isoformat(),
        },
    )


@asset(
    schedule=[seq_1_start],
    tags=["assets", "seq_1", "XCom"],
)
def seq_1_create_table(context):
    from jinja2 import Template

    hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)

    with open(f"{str(_SQL_DIR)}/create_table_if_not_exists.sql", "r") as f:
        sql_template = f.read()

    template = Template(sql_template)
    sql = template.render(
        params={"schema": _POSTGRES_SCHEMA, "table": _POSTGRES_TRANSFORMED_TABLE}
    )

    hook.run(sql)

    # You can attach metadata to an asset event. This is useful for passing small amounts of
    # data specific to a sequence.
    # See: https://www.astronomer.io/docs/learn/airflow-datasets/#attaching-information-to-an-asset-event


@asset(
    schedule=[seq_1_start],
    tags=["assets", "seq_1", "XCom"],
    params={
        "coordinates": Param({"latitude": 46.9481, "longitude": 7.4474}, type="object")
    },  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
)
def seq_1_extract(context):
    """
    Extract data from the Open-Meteo API
    Returns:
        dict: The full API response
    """
    import requests

    url = os.getenv("WEATHER_API_URL")

    # if the DAG is run manually use the time stamp and lat/long of the manual run
    if str(context["dag_run"].run_type) == "DagRunType.MANUAL":
        latitude = context["params"]["coordinates"]["latitude"]
        longitude = context["params"]["coordinates"]["longitude"]
        ts = context["dag_run"].run_after.isoformat()
    # if the DAG is run as part of the asset sequence use the lat/long and timestamp of the upstream asset event
    else:
        # You can retrieve metadata from the triggering asset event.
        # See: https://www.astronomer.io/docs/learn/airflow-datasets/#retrieving-asset-information-in-a-downstream-task
        metadata_upstream = context["triggering_asset_events"][
            Asset("seq_1_start")
        ][0].extra

        latitude = metadata_upstream["latitude"]
        longitude = metadata_upstream["longitude"]
        ts = metadata_upstream["ts"]

    url = url.format(latitude=latitude, longitude=longitude)

    response = requests.get(url)

    # attach the timestamp of the DAG run to the asset event
    yield Metadata(Asset("seq_1_extract"), {"ts": ts})

    # returning a value from an @asset pushes it to XCom
    return response.json()


@asset(
    schedule=[seq_1_extract],
    tags=["assets", "seq_1", "XCom"],
)
def seq_1_transform(context) -> dict:
    """
    Transform the data
    Args:
        api_response (dict): The full API response
    Returns:
        dict: The transformed data
    """

    # if the DAG is run manually use the time stamp of the manual run
    upstream_run_id = None
    if str(context["dag_run"].run_type) == "DagRunType.MANUAL":
        dag_run_timestamp = context["dag_run"].run_after.isoformat()
    # if the DAG is run as part of the asset sequence use the timestamp of the upstream asset event
    else:
        # You can retrieve metadata from the triggering asset event.
        # See: https://www.astronomer.io/docs/learn/airflow-datasets/#retrieving-asset-information-in-a-downstream-task
        upstream_event = context["triggering_asset_events"][Asset("seq_1_extract")][0]

        dag_run_timestamp = upstream_event.extra["ts"]
        # asset-triggered runs have no logical_date, so "latest" xcom lookups are
        # ambiguous across runs. Pin the pull to the exact upstream run instead.
        upstream_run_id = upstream_event.source_run_id

    # To retrieve data pushed by an upstream asset perform a cross-dag xcom pull
    api_response = context["ti"].xcom_pull(
        dag_id="seq_1_extract",
        task_ids=["seq_1_extract"],
        key="return_value",
        run_id=upstream_run_id,
        include_prior_dates=True,
    )[0]

    time = api_response["hourly"]["time"]

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


@asset(
    schedule=[seq_1_create_table, seq_1_transform],
    tags=["assets", "seq_1", "XCom"],
)
def seq_1_load(context):
    """
    Load the data to the destination without using a temporary CSV file.
    Args:
        transformed_data (dict): The transformed data
    """
    import csv
    import os
    import tempfile
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from include.postgres_copy_insert import copy_insert

    # asset-triggered runs have no logical_date, so "latest" xcom lookups are
    # ambiguous across runs. Pin the pull to the exact upstream run instead.
    upstream_events = context["triggering_asset_events"].get(Asset("seq_1_transform"))
    upstream_run_id = upstream_events[0].source_run_id if upstream_events else None

    transformed_data = context["ti"].xcom_pull(
        dag_id="seq_1_transform",
        task_ids=["seq_1_transform"],
        key="return_value",
        run_id=upstream_run_id,
        include_prior_dates=True,
    )[0]

    hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        writer = csv.writer(tmp)
        writer.writerow(WEATHER_COL_ORDER)
        rows = zip(*[transformed_data[col] for col in WEATHER_COL_ORDER])
        writer.writerows(rows)
        tmp_path = tmp.name

    with open(f"{str(_SQL_DIR)}/copy_insert.sql") as f:
        sql = f.read()
    sql = sql.replace("{schema}", _POSTGRES_SCHEMA)
    sql = sql.replace("{table}", _POSTGRES_TRANSFORMED_TABLE)

    copy_insert(hook, sql, tmp_path)
    os.remove(tmp_path)
