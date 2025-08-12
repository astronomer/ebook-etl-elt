"""
## Simple asset sequence creating 4 DAGs to perform an ELT pattern with intermediary storage

These 4 asset decorated functions created 4 DAGs each containing one task
following a similar pattern as the regular elt_intermediary_storage DAG.

Note that passing the data between assets via XCom necessitates a Cross-DAG xcom pull
and other information like DAG params and timestamps are passed via the metadata
asset events.
"""

import os
import json

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Param, asset, Metadata, Asset
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
_SQL_DIR = os.path.join(
    os.path.dirname(__file__), f"../../include/sql/asset_sequences/{DAG_ID}"
)

_INTERMEDIARY_STORAGE_KEY = "seq_2_extract"


@asset(
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    tags=["assets", "seq_2", "XCom"],  # add tags in the UI
    params={
        "coordinates": Param({"latitude": 46.9481, "longitude": 7.4474}, type="object")
    },  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
)
def seq_2_start(context):
    yield Metadata(
        Asset("seq_2_start"),
        {
            "latitude": context["params"]["coordinates"]["latitude"],
            "longitude": context["params"]["coordinates"]["longitude"],
            "ts": context["ts"],
        },
    )


@asset(
    schedule=[seq_2_start],
    tags=["assets", "seq_2", "XCom"],
)
def seq_2_create_in_table(context):
    from jinja2 import Template

    hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)

    with open(f"{_SQL_DIR}/create_in_table_if_not_exists.sql", "r") as f:
        sql_template = f.read()

    template = Template(sql_template)
    sql = template.render(
        params={"schema": _POSTGRES_SCHEMA, "table": _POSTGRES_IN_TABLE}
    )

    hook.run(sql)


@asset(
    schedule=[seq_2_start],
    tags=["assets", "seq_2", "XCom"],
)
def seq_2_create_model_table(context):
    from jinja2 import Template

    hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)

    with open(f"{_SQL_DIR}/create_model_table_if_not_exists.sql", "r") as f:
        sql_template = f.read()

    template = Template(sql_template)
    sql = template.render(
        params={"schema": _POSTGRES_SCHEMA, "table": _POSTGRES_TRANSFORMED_TABLE}
    )

    hook.run(sql)


@asset(
    schedule=[seq_2_start],
    tags=["assets", "seq_2", "XCom"],
    params={
        "coordinates": Param({"latitude": 46.9481, "longitude": 7.4474}, type="object")
    },
)
def seq_2_extract(context):
    """
    Extract data from the Open-Meteo API
    Returns:
        dict: The full API response
    """
    import requests

    # if the DAG is run manually use the time stamp and lat/long of the manual run
    if str(context["dag_run"].run_type) == "DagRunType.MANUAL":
        latitude = context["params"]["coordinates"]["latitude"]
        longitude = context["params"]["coordinates"]["longitude"]
        dag_run_timestamp = context["ts"]
    # if the DAG is run as part of the asset sequence use the lat/long and timestamp of the upstream asset event
    else:
        # You can retrieve metadata from the triggering asset event.
        # See: https://www.astronomer.io/docs/learn/airflow-datasets/#retrieving-asset-information-in-a-downstream-task
        metadata_upstream = context["triggering_asset_events"][Asset("seq_2_start")][
            0
        ].extra

        latitude = metadata_upstream["latitude"]
        longitude = metadata_upstream["longitude"]
        dag_run_timestamp = metadata_upstream["ts"]

    url = os.getenv("WEATHER_API_URL")

    url = url.format(latitude=latitude, longitude=longitude)

    response = requests.get(url).json()

    response_bytes = json.dumps(response).encode("utf-8")

    # Save the data to S3
    hook = S3Hook(aws_conn_id=_AWS_CONN_ID)
    hook.load_bytes(
        bytes_data=response_bytes,
        key=f"{_INTERMEDIARY_STORAGE_KEY}/{dag_run_timestamp}.json",
        bucket_name=_S3_BUCKET,
        replace=True,
    )

    yield Metadata(Asset("seq_2_extract"), {"ts": dag_run_timestamp})


@asset(
    schedule=[seq_2_create_in_table, seq_2_create_model_table, seq_2_extract],
    tags=["assets", "seq_2", "XCom"],
)
def seq_2_load(context):
    """
    Load the data from S3 to Postgres
    """

    # if the DAG is run manually use the time stamp of the manual run
    if str(context["dag_run"].run_type) == "DagRunType.MANUAL":
        dag_run_timestamp = context["ts"]
    # if the DAG is run as part of the asset sequence use the timestamp of the upstream asset event
    else:
        # You can retrieve metadata from the triggering asset event.
        # See: https://www.astronomer.io/docs/learn/airflow-datasets/#retrieving-asset-information-in-a-downstream-task
        metadata_upstream = context["triggering_asset_events"][Asset("seq_2_extract")][
            0
        ].extra

        dag_run_timestamp = metadata_upstream["ts"]

    s3_hook = S3Hook(aws_conn_id=_AWS_CONN_ID)
    response = s3_hook.read_key(
        key=f"{_INTERMEDIARY_STORAGE_KEY}/{dag_run_timestamp}.json",
        bucket_name=_S3_BUCKET,
    )
    api_response = json.loads(response)

    postgres_hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)

    insert_sql = f"""
    INSERT INTO {_POSTGRES_SCHEMA}.{_POSTGRES_IN_TABLE} (raw_data)
    VALUES (%s::jsonb);
    """

    postgres_hook.run(insert_sql, parameters=(json.dumps(api_response),))


@asset(
    schedule=[seq_2_load],
    tags=["assets", "seq_2", "XCom"],
)
def seq_2_transform(context):
    from jinja2 import Template

    hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)

    with open(f"{_SQL_DIR}/transform.sql", "r") as f:
        sql_template = f.read()

    template = Template(sql_template)
    sql = template.render(
        params={
            "schema": _POSTGRES_SCHEMA,
            "in_table": _POSTGRES_IN_TABLE,
            "out_table": _POSTGRES_TRANSFORMED_TABLE,
        }
    )

    hook.run(sql)
