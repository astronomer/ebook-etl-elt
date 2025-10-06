from airflow.sdk import dag, task, chain


@dag
def xcom_patterns():
    @task
    def task_pushing_to_xcom():
        return {"a": 1, "b": 2}

    @task
    def task_pulling_from_xcom(my_input):
        return my_input

    @task
    def task_1():
        pass

    @task
    def task_2():
        pass

    @task
    def task_pulling_from_xcom_2(**context):
        return context["ti"].xcom_pull(
            task_ids="task_pushing_to_xcom", key="return_value"
        )

    _task_pushing_to_xcom = task_pushing_to_xcom()
    _task_pulling_from_xcom = task_pulling_from_xcom(_task_pushing_to_xcom)
    _task_1 = task_1()
    _task_2 = task_2()
    _task_pulling_from_xcom_2 = task_pulling_from_xcom_2()

    chain(
        _task_pushing_to_xcom,
        _task_pulling_from_xcom,
    )

    chain(
        _task_pushing_to_xcom,
        _task_1,
        _task_2,
        _task_pulling_from_xcom_2,
    )


xcom_patterns()
