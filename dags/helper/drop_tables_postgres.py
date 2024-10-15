"""
## Helper DAG to drop all tables in the Postgres database

CAVE: This DAG will drop all tables in the database. Use with caution!
"""

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.baseoperator import chain

_POSTGRES_CONN_ID = "postgres_default"


@dag(
    dag_display_name="🛠️ Helper: Drop Tables",
    start_date=None,
    schedule=None,
    catchup=False,
    tags=["helper", "Postgres"],
)
def drop_tables_postgres():

    get_list_of_tables = SQLExecuteQueryOperator(
        task_id="get_list_of_tables",
        conn_id=_POSTGRES_CONN_ID,
        sql="SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
    )

    drop_tables = SQLExecuteQueryOperator(
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

    chain(get_list_of_tables, drop_tables)


drop_tables_postgres()
