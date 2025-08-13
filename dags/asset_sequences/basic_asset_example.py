from airflow.sdk import asset, Metadata, Asset

@asset(schedule=None, tags=["basic_asset_example"])
def my_asset_1():
    print("Add any Python Code!")

@asset(schedule=[my_asset_1], tags=["basic_asset_example"])
def my_asset_2():
    # you can return data from an @asset decorated function
    # to push it to XCom 
    return {"a": 1, "b": 2}

@asset(schedule=[my_asset_1, my_asset_2], tags=["basic_asset_example"])
def my_asset_3(context):
    # you can pull the data from XCom via the context 
    # note that this is a cross-dag xcom pull
    data = context["ti"].xcom_pull(
        dag_id="my_asset_2",
        task_ids=["my_asset_2"],
        key="return_value",
        include_prior_dates=True,
    )[0]

    print(data)

@asset(schedule=[my_asset_1], tags=["basic_asset_example"])
def my_asset_4(context):
    # you can also attache Metadata to the Asset Event
    yield Metadata(Asset("my_asset_4"), {"a": 1, "b": 2})


@asset(schedule=[my_asset_4], tags=["basic_asset_example"])
def my_asset_5(context):
    # and retrieve the Metadata from the Asset Event
    metadata = context["triggering_asset_events"][Asset("my_asset_4")][0].extra
    print(metadata)

