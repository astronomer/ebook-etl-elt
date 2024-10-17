# airflow, this is needed to Airflow will parse this file
import dagfactory
from pathlib import Path

config_file = Path.cwd() / "dags/dag-factory_dags/config_file.yml"
dag_factory = dagfactory.DagFactory(config_file)

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())