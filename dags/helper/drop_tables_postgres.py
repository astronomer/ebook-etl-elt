"""
## Helper DAG to drop all tables in the Postgres database

CAVE: This DAG will drop all tables in the database. Use with caution!
"""

from airflow.sdk import dag, task, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

_POSTGRES_CONN_ID = "postgres_default"


@dag(
    dag_display_name="🛠️ Helper: Drop Tables",
    tags=["helper"],
)
def drop_tables_postgres():

    _get_list_of_tables = SQLExecuteQueryOperator(
        task_id="get_list_of_tables",
        conn_id=_POSTGRES_CONN_ID,
        sql="SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
    )

    _drop_tables = SQLExecuteQueryOperator(
        task_id="drop_tables",
        conn_id=_POSTGRES_CONN_ID,
        sql="""
        {% set tables = task_instance.xcom_pull(task_ids='get_list_of_tables') %}
        {% if tables %}
            DROP TABLE IF EXISTS 
            {{ tables | map(attribute=0) | join(', ') }} CASCADE;
        {% else %}
            SELECT 'No tables to drop';
        {% endif %}
        """,
    )

    chain(_get_list_of_tables, _drop_tables)


drop_tables_postgres()
