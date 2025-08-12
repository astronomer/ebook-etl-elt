# Best practices for writing ETL and ELT pipelines

This repository contains the code for the webinar demo shown in: Best practices for writing ETL and ELT pipelines.

[Watch the webinar here for free!](https://www.astronomer.io/events/webinars/best-practices-for-writing-etl-and-elt-pipelines-video/)

> [!TIP]
> The DAGs in this webinar have been updated for Airflow 3, the webinar above showed them running in Airflow 2. 

This repository is configured to spin up 7 Docker containers when you run `astro dev start` (See [Install the Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)). if you cannot use the Astro CLI see [Running the demo in GH codespaces](#running-the-demo-in-gh-codespaces).

The containers are:
- Postgres, port 5432: Airflow's Metadata Database
- API server, port 8080: The Airflow component responsible for rendering the Airflow UI and serving several APIs
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- Triggerer: The Airflow component responsible for triggering deferred tasks
- Postgres, port 5433: A Postgres database for the demo data
- MinIO, port 9000: An S3-compatible object storage service for the demo data

To connect Airflow to both the Postgres database and MinIO, create a `.env` file in the root directory of the project with the exact contents of the `.env.example` file. Note that you need to restart the Airflow instance with `astro dev restart` after creating the `.env` file for the changes to take effect.

> [!TIP]
> You need to be on at least version 1.34.0 of the Astro CLI in oder to run this repo. You can check your version with `astro version` and upgrade with `brew upgrade astro`

All the DAGs run without any further setup or tools needed! 

## Content

This repository contains:

- [`dag-factory_dags`](/dags/dag-factory_dags/): A folder containing the code necessary to generate 3 DAGs with the `dag-factory` package.
    - `config_file.yml`: Config file creating the 3 DAGs.
    - `generate_dags.py`: The code to generate DAGs from the config file.
- [`helper`](/dags//helper/): This folder contains two DAGs meant to help you explore and develop.
    - `query_tables`: A DAG that queries the tables in the Postgres database to return the number of records for each table.
    - `drop_tables_postgres`: A DAG that drops all the tables in the Postgres database.
- [`modularized_task_groups`](/dags/modularized_task_groups/): This folder contains a DAG with a modularized task group, stored in [`include/custom_task_group/etl_task_group.py](/include/custom_task_group/etl_task_group.py).
- [`pattern_dags`](/dags/pattern_dags/): Contains several DAGs showing different ETL and ELT patterns. They all use the Open Meteo API as a source system and load data to Postgres. 

All supporting SQL code is stored in the include folder.

- [`include/dag_factory`](/include/dag_factory/): Contains the SQL code for the 3 DAG factory tasks.
- [`include/sql`](/include/sql/): Contains the SQL code for all other tasks.

The SQL code is repetitive for demo purposes, meaning you can manipulate the code for just one DAG to explore the DAGs without affecting other DAGs. In a real-world scenario you would likely modularize the SQL code further and avoid repetition.

## How to run the demo

1. Fork and clone this repository.
2. Make sure you have the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed and are at least on version 1.34.0.
3. Copy the `.env.example` file to a new file called `.env`. If you want to use a custom XCom backend with MinIO uncomment the last 4 lines in the `.env` file.
4. Run `astro dev start` to start the Airflow instance. The webserver with the Airflow UI will be available at `localhost:8080`.
5. Run any DAG. They all are independent from each other.
7. Use the `query_tables` DAG to check the number of records in the tables.

If you'd like to directly interact with the Postgres database, you can use the following commands to connect to the database:

```bash
docker ps
```

This command will list all the running containers. Look for the container with the image `postgres:17.5-alpine`. Copy the container ID (in the format `30cfd7660be9`) and run the following command:

```bash
docker exec -it <container_id> psql -U postgres
```

You are now in a `psql` session connected to the Postgres database. You can list the tables with the command `\dt` and query the tables with `SELECT * FROM <table_name>;`.

## Resources

- Ebook: [Apache Airflow® Best Practices for ETL and ELT Pipelines](https://www.astronomer.io/ebooks/apache-airflow-best-practices-etl-elt-pipelines/)
- Webinar: [Best practices for writing ETL and ELT pipelines](https://www.astronomer.io/events/webinars/best-practices-for-writing-etl-and-elt-pipelines-video/)
- Book on Airflow 3: [Practical Guide to Apache Airflow® 3](https://www.astronomer.io/ebooks/practical-guide-to-apache-airflow-3/)

# Running the demo in GH codespaces

If you can't install the CLI, you can run the project from your forked repo using GitHub Codespaces.

1. Fork this repository
2. Click on the green "Code" button and select the "Codespaces" tab. 
3. Click on the 3 dots and then `+ New with options...` to create a new Codespace with a configuration, make sure to select a Machine type of at least `8-core`.

   ![Start GH Codespaces](img/start_codespaces.png)

4. Copy the `.env.example` file to a new file called `.env`. If you want to use a custom XCom backend with MinIO uncomment the last 4 lines in the `.env` file.

5. Run `astro dev start -n --wait 5m` in the Codespaces terminal to start the Airflow environment using the Astro CLI. This can take a few minutes.

   ![Start the Astro CLI in Codespaces](img/codespaces_start_astro.png)

   Once you see the following printed to your terminal, the Airflow environment is ready to use:

   ```text
   ✔ Project image has been updated
   ✔ Project started
   ➤ Airflow UI: http://localhost:8080
   ➤ Postgres Database: postgresql://localhost:5435/postgres
   ➤ The default Postgres DB credentials are: postgres:postgres
   ```

6. Once the Airflow project has started, access the Airflow UI by clicking on the Ports tab and opening the forward URL for port `8080`.

> [!TIP]
> If when accessing the forward URL you get an error like `{"detail":"Invalid or unsafe next URL"}`, you will need to modify the forwarded URL. Delete everything forward of `next=....` (this should be after `/login?`, ). The URL will update, adn then remove `:8080`, so your URL should endd in `.app.github.dev`

7. Log into the Airflow UI. It is possible that after logging in you see an error, in this case you have to open the URL again from the ports tab.