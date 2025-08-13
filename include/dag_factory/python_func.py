import os
from datetime import datetime
import json

from include.col_orders import (
    SUNRISE_COL_ORDER,
    WEATHER_CODE_COL_ORDER,
    WIND_COL_ORDER,
)


def extract(**context):
    """
    Extract data from the Open-Meteo API
    Returns:
        dict: The full API response
    """
    import requests

    url = context["params"]["weather_api_url"]

    coordinates = context["params"]["coordinates"]
    latitude = coordinates["latitude"]
    longitude = coordinates["longitude"]

    url = url.format(latitude=latitude, longitude=longitude)

    response = requests.get(url)

    return response.json()


def transform_weather_code(api_response: dict, **context) -> dict:
    """
    Transform the data
    Args:
        api_response (dict): The full API response
    Returns:
        dict: The transformed data
    """

    api_response = api_response.replace("'", '"')
    api_response = json.loads(api_response)

    time = api_response["daily"]["time"]

    dag_run_timestamp = context["ts"]

    transformed_data = {
        "date": [datetime.strptime(x, "%Y-%m-%d").strftime("%Y-%m-%d") for x in time],
        "day": [datetime.strptime(x, "%Y-%m-%d").day for x in time],
        "month": [datetime.strptime(x, "%Y-%m-%d").month for x in time],
        "year": [datetime.strptime(x, "%Y-%m-%d").year for x in time],
        "weather_code": api_response["daily"]["weather_code"],
        "last_updated": [dag_run_timestamp for i in range(len(time))],
        "latitude": [api_response["latitude"] for i in range(len(time))],
        "longitude": [api_response["longitude"] for i in range(len(time))],
        "elevation": [api_response["elevation"] for i in range(len(time))],
        "utc_offset_seconds": [
            api_response["utc_offset_seconds"] for i in range(len(time))
        ],
        "timezone": [api_response["timezone"] for i in range(len(time))],
        "timezone_abbreviation": [
            api_response["timezone_abbreviation"] for i in range(len(time))
        ],
    }

    return transformed_data


def transform_sunrise(api_response: dict, **context) -> dict:
    """
    Transform the data
    Args:
        api_response (dict): The full API response
    Returns:
        dict: The transformed data
    """

    api_response = api_response.replace("'", '"')
    api_response = json.loads(api_response)

    time = api_response["daily"]["time"]

    dag_run_timestamp = context["ts"]

    transformed_data = {
        "date": [datetime.strptime(x, "%Y-%m-%d").strftime("%Y-%m-%d") for x in time],
        "day": [datetime.strptime(x, "%Y-%m-%d").day for x in time],
        "month": [datetime.strptime(x, "%Y-%m-%d").month for x in time],
        "year": [datetime.strptime(x, "%Y-%m-%d").year for x in time],
        "sunrise": api_response["daily"]["sunrise"],
        "last_updated": [dag_run_timestamp for i in range(len(time))],
        "latitude": [api_response["latitude"] for i in range(len(time))],
        "longitude": [api_response["longitude"] for i in range(len(time))],
        "elevation": [api_response["elevation"] for i in range(len(time))],
        "utc_offset_seconds": [
            api_response["utc_offset_seconds"] for i in range(len(time))
        ],
        "timezone": [api_response["timezone"] for i in range(len(time))],
        "timezone_abbreviation": [
            api_response["timezone_abbreviation"] for i in range(len(time))
        ],
    }

    return transformed_data


def transform_wind(api_response: dict, **context) -> dict:
    """
    Transform the data
    Args:
        api_response (dict): The full API response
    Returns:
        dict: The transformed data
    """

    api_response = api_response.replace("'", '"')
    api_response = json.loads(api_response)

    time = api_response["daily"]["time"]

    dag_run_timestamp = context["ts"]

    transformed_data = {
        "date": [datetime.strptime(x, "%Y-%m-%d").strftime("%Y-%m-%d") for x in time],
        "day": [datetime.strptime(x, "%Y-%m-%d").day for x in time],
        "month": [datetime.strptime(x, "%Y-%m-%d").month for x in time],
        "year": [datetime.strptime(x, "%Y-%m-%d").year for x in time],
        "wind": api_response["daily"]["wind_speed_10m_max"],
        "last_updated": [dag_run_timestamp for i in range(len(time))],
        "latitude": [api_response["latitude"] for i in range(len(time))],
        "longitude": [api_response["longitude"] for i in range(len(time))],
        "elevation": [api_response["elevation"] for i in range(len(time))],
        "utc_offset_seconds": [
            api_response["utc_offset_seconds"] for i in range(len(time))
        ],
        "timezone": [api_response["timezone"] for i in range(len(time))],
        "timezone_abbreviation": [
            api_response["timezone_abbreviation"] for i in range(len(time))
        ],
    }

    return transformed_data


def load(transformed_data: dict, **context):
    """
    Load the data to the destination without using a temporary CSV file.
    Args:
        transformed_data (dict): The transformed data
    """
    import csv
    import io
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    postgres_conn_id = context["params"]["postgres_conn_id"]
    postgres_schema = context["params"]["schema_name"]
    postgres_table = context["params"]["table_name"]
    sql_dir = context["params"]["sql_dir"]

    transformed_data = transformed_data.replace("'", '"')
    transformed_data = json.loads(transformed_data)

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    if context["dag_run"].dag_id == "dag_factory_dag_etl_sunrise":
        col_order = SUNRISE_COL_ORDER
    elif context["dag_run"].dag_id == "dag_factory_dag_etl_weather_code":
        col_order = WEATHER_CODE_COL_ORDER
    elif context["dag_run"].dag_id == "dag_factory_dag_etl_wind":
        col_order = WIND_COL_ORDER

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(col_order)
    rows = zip(*[transformed_data[col] for col in col_order])
    writer.writerows(rows)

    csv_buffer.seek(0)

    with open(f"{sql_dir}/copy_insert.sql") as f:
        sql = f.read()
    sql = sql.replace("{schema}", postgres_schema)
    sql = sql.replace("{table}", postgres_table)

    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.copy_expert(sql=sql, file=csv_buffer)
    conn.commit()
    cursor.close()
    conn.close()
