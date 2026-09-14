"""
Helper for running a "CREATE TEMP TABLE; COPY ... FROM STDIN; INSERT ... ;"
sequence against Postgres.

psycopg3 (the default driver used by apache-airflow-providers-postgres>=7.0
when psycopg3 + SQLAlchemy 2.x are installed) does not allow a COPY statement
to be combined with other statements in the same query string, unlike
psycopg2. The three statements must be executed individually on the same
connection so the CREATE TEMP TABLE is visible to the COPY and INSERT that
follow it.
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook


def copy_insert(hook: PostgresHook, sql: str, csv_path: str) -> None:
    """
    Run a create-temp-table / copy-from-stdin / insert-from-temp-table sequence.

    Args:
        hook: PostgresHook to execute against.
        sql: The three ";"-terminated statements (create, copy, insert), in order.
        csv_path: Path to the CSV file to load via COPY FROM STDIN.
    """
    create_sql, copy_sql, insert_sql = (
        f"{statement.strip()};" for statement in sql.split(";") if statement.strip()
    )

    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(create_sql)
    with open(csv_path, "rb") as f:
        if hasattr(cursor, "copy_expert"):
            cursor.copy_expert(sql=copy_sql, file=f)
        else:
            with cursor.copy(copy_sql) as copy:
                while data := f.read(8192):
                    copy.write(data)
    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()
    conn.close()
