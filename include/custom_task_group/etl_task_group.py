from airflow.sdk import TaskGroup, task


class MyETLTaskGroup(TaskGroup):
    """A task group containing a simple ETL pattern."""

    def __init__(self, group_id, url: str, key_of_interest: str, **kwargs):
        super().__init__(group_id=group_id, **kwargs)

        self.url = url

        @task(task_group=self)
        def extract(url: str) -> dict:
            """Extracts data from a URL.
            Args:
                url (str): The URL to extract data from."""
            import requests

            response = requests.get(url)
            return response.json()

        @task(task_group=self)
        def transform(api_response: dict, key: str) -> str:
            """Transforms the api_response.
            Args:
                api_response (dict): The number to transform."""

            return api_response[key]

        @task(task_group=self)
        def load(transformed_data: str):
            """Prints the transformed data (simulating loading).
            Args:
                transformed_data (dict): The transformed data."""
            print(transformed_data)

        load(transform(api_response=extract(self.url), key=key_of_interest))
