import subprocess
from pathlib import Path
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


# Répertoire du projet dans le conteneur Docker
PROJECT_DIR = Path("/opt/airflow/project")

# Python installé dans le conteneur Airflow
PYTHON = "/usr/local/bin/python"


def run_python_module(module):
    subprocess.run(
        [PYTHON, "-m", module],
        cwd=str(PROJECT_DIR),
        check=True,
    )


@dag(
    dag_id="ecommerce_pipeline",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(
        2026, 1, 1, tz="Europe/Paris"
    ),
    catchup=False,
    tags=["ecommerce", "docker", "data-engineering"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)
def ecommerce_pipeline():

    @task
    def ingestion_api():
        run_python_module("src.ingestion.api_ingestion")

    @task
    def bronze_to_silver():
        run_python_module("src.transformation.bronze_to_silver")

    @task
    def silver_to_gold():
        run_python_module("src.transformation.silver_to_gold")

    @task
    def gold_to_postgres():
        run_python_module("src.loading.gold_to_postgres")

    @task
    def dbt_build():
        subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                str(PROJECT_DIR / "dbt" / "ecommerce_dbt"),
                "--profiles-dir",
                "/home/airflow/.dbt",
            ],
            cwd=str(PROJECT_DIR),
            check=True,
        )

    (
        ingestion_api()
        >> bronze_to_silver()
        >> silver_to_gold()
        >> gold_to_postgres()
        >> dbt_build()
    )


ecommerce_pipeline()