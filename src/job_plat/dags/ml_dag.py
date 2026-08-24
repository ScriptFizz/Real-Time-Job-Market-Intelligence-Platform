from datetime import UTC, datetime, timedelta

from airflow.decorators import dag as airflow_dag, task
from airflow.operators.python import get_current_context
from airflow.sensors.external_task import ExternalTaskSensor

from job_plat.dags.dag_helpers import build_cli_command, run_command


@airflow_dag(
    schedule="@weekly",
    params={"env": "dev"},
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def ml_dag():
    wait_for_gold = ExternalTaskSensor(
        task_id="wait_for_gold",
        external_dag_id="processing_dag",
        external_task_id="run_gold",
        mode="reschedule",
        timeout=600,
    )

    @task(execution_timeout=timedelta(minutes=30))
    def run_features():
        context = get_current_context()
        run_command(build_cli_command("feature", context))

    @task(execution_timeout=timedelta(minutes=30))
    def run_ml():
        context = get_current_context()
        run_command(build_cli_command("ml", context))

    wait_for_gold >> run_features() >> run_ml()


dag = ml_dag()
