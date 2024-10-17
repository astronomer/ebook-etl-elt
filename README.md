# Best practices for writing ETL and ELT pipelines

This repository contains the code for the webinar demo shown in: Best practices for writing ETL and ELT pipelines.

[Watch the webinar here for free!](https://www.astronomer.io/events/webinars/best-practices-for-writing-etl-and-elt-pipelines-video/)

This repository is configured to spin up 6 Docker containers when you run `astro dev start` (See [Install the Astro CLI]([Astro CLI](https://docs.astronomer.io/astro/cli/install-cli))). 

The containers are:
- Postgres, port 5432: Airflow's Metadata Database
- Webserver, port 8080: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks
- Postgres, port 5433: A Postgres database for the demo data
- MinIO, port 9000: An S3-compatible object storage service for the demo data

To connect Airflow to both the Postgres database and MinIO, create a `.env` file in the root directory of the project with the exact contents of the `.env.example` file. Note that you need to restart the Airflow instance with `astro dev restart` after creating the `.env` file for the changes to take effect.

All the DAGs run without any further setup or tools needed!

## Content

This repository contains:

- [`dag-factory_dags`](/dags/dag-factory_dags/): A folder containing the code necessary to generate 3 DAGs with the `dag-factory` package.
    - `config_file.yml`: Config file creating the 3 DAGs.
    - `generate_dags.py`: The code to generate DAGs from the config file.
- [`helper`](/dags//helper/): This folder contains two DAGs meant to help you explore and develop.
    - `query_tables`: A DAG that queries the tables in the Postgres database to return the number of records for each table.
    - `drop_tables_postgres`: A DAG that drops all the tables in the Postgres database.
- [`modularized_task_groups`](/dags/modularized_task_groups/): This folder contains a DAG with a modularized task group.
- [`pattern_dags`](/dags/pattern_dags/): Contains several DAGs showing different ETL and ELT patterns. They all use the Open Meteo API as a source system and load data to Postgres. 

## How to run the demo

1. Fork and clone this repository.
2. Make sure you have the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed and that [Docker](https://www.docker.com/products/docker-desktop) is running.
3. Copy the `.env.example` file to a new file called `.env`. If you want to use a custom XCom backend with MinIO uncomment the last 4 lines in the `.env` file.
4. Run `astro dev start` to start the Airflow instance. The webserver with the Airflow UI will be available at `localhost:8080`. Log in with the credentials `admin:admin`.
5. Run any DAG. They all are independent from each other.
7. Use the `query_tables` DAG to check the number of records in the tables.

If you'd like to directly interact with the Postgres database, you can use the following commands to connect to the database:

```bash
docker ps
```

This command will list all the running containers. Look for the container with the image `postgres:15.4-alpine`. Copy the container ID (in the format `30cfd7660be9`) and run the following command:

```bash
docker exec -it <container_id> psql -U postgres
```

You are now in a `psql` session connected to the Postgres database. You can list the tables with the command `\dt` and query the tables with `SELECT * FROM <table_name>;`.

## Resources

- [Best practices for writing ETL and ELT pipelines](https://www.astronomer.io/events/webinars/best-practices-for-writing-etl-and-elt-pipelines-video/)