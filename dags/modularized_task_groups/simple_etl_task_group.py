from airflow.sdk import dag, task
from include.custom_task_group.etl_task_group import MyETLTaskGroup


@dag
def simple_etl_task_group():
    @task
    def get_url():
        return "https://catfact.ninja/fact"

    MyETLTaskGroup(group_id="my_task_group", url=get_url(), key_of_interest="fact")


simple_etl_task_group()
