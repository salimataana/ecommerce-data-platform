import subprocess
from pathlib import Path
from datetime import timedelta
from airflow.sdk import dag, task
import pendulum


PROJECT_DIR = Path(__file__).resolve().parents[2]
PYTHON = PROJECT_DIR / ".venv" / "bin" / "python"


@dag(
    dag_id="ecommerce_pipeline",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["ecommerce", "data-engineering"],
    default_args={
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
},
)
def ecommerce_pipeline():

    @task
    def ingestion_api():
        subprocess.run(
            [str(PYTHON), "-m", "src.ingestion.api_ingestion"],
            cwd=str(PROJECT_DIR),
            check=True,
        )

    @task
    def bronze_to_silver():
        subprocess.run(
            [str(PYTHON), "-m", "src.transformation.bronze_to_silver"],
            cwd=str(PROJECT_DIR),
            check=True,
        )

    @task
    def silver_to_gold():
        subprocess.run(
            [str(PYTHON), "-m", "src.transformation.silver_to_gold"],
            cwd=str(PROJECT_DIR),
            check=True,
            )

    @task
    def gold_to_postgres():
        subprocess.run(
            [str(PYTHON), "-m", "src.loading.gold_to_postgres"],
            cwd=str(PROJECT_DIR),
            check=True,
        )

    @task
    def dbt_build():
        subprocess.run(
            [
                str(PROJECT_DIR / ".venv" / "bin" / "dbt"),
                "build",
                "--project-dir",
                str(PROJECT_DIR / "dbt" / "ecommerce_dbt"),
            ],
            cwd=str(PROJECT_DIR),
            check=True,
        )

    ingestion_api() >> bronze_to_silver() >> silver_to_gold() >> gold_to_postgres() >> dbt_build()


ecommerce_pipeline()