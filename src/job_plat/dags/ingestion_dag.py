from datetime import UTC, datetime, timedelta

from airflow.decorators import dag as airflow_dag, task
from airflow.operators.python import get_current_context

from job_plat.dags.dag_helpers import build_cli_command, run_command


@airflow_dag(
    schedule="@hourly",
    params={"env": "dev"},
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def ingestion_dag():
    @task(execution_timeout=timedelta(minutes=30))
    def ingest_jobs():
        context = get_current_context()
        run_command(build_cli_command("bronze", context))

    ingest_jobs()


dag = ingestion_dag()
