"""
## Helper DAG to query all tables in the Postgres database

Queries both the number of records as well as the first 5 records of one of 
the tables in the Postgres database.
"""

from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.baseoperator import chain

_POSTGRES_CONN_ID = "postgres_default"
_TABLE_NAME = "in_weather_data_etl_xcom"


@dag(
    dag_display_name="🛠️ Helper: Query Tables",
    start_date=None,
    schedule=None,
    catchup=False,
    tags=["helper", "Postgres"],
    params={"table_name": _TABLE_NAME},
)
def query_tables_postgres():

    get_list_of_tables = SQLExecuteQueryOperator(
        task_id="get_list_of_tables",
        conn_id=_POSTGRES_CONN_ID,
        sql="SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
    )

    get_num_records = SQLExecuteQueryOperator(
        task_id="get_num_records",
        conn_id=_POSTGRES_CONN_ID,
        sql="""
        {% set tables = task_instance.xcom_pull(task_ids='get_list_of_tables') %}
        {% if tables %}
            {% set query = [] %}
            {% for table in tables %}
                {% set table_name = table[0] %}
                {% set row_query = "SELECT '" + table_name + "' as table_name, COUNT(*) as row_count FROM " + table_name %}
                {% do query.append(row_query) %}
            {% endfor %}
            {{ query | join(' UNION ALL ') }};
        {% else %}
            SELECT 'No tables to query' as table_name, 0 as row_count;
        {% endif %}
        """,
        show_return_value_in_logs=True,
    )

    print_table_head = SQLExecuteQueryOperator(
        task_id="print_table_head",
        conn_id=_POSTGRES_CONN_ID,
        sql="""
        SELECT * FROM {{ params.table_name }} ORDER BY last_updated DESC LIMIT 5 ;
        """,
        show_return_value_in_logs=True,
    )

    chain(get_list_of_tables, get_num_records)


query_tables_postgres()
