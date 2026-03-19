from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from job_plat.dags.dag_helpers import run_command


@dag(schedule="@daily", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
def processing_dag():
    
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="ingestion_dag",
        external_task_id="ingest_jobs",
        mode="reschedule",
        timeout=600,
        execution_delta=timedelta(hours=1)
    )
    
    @task(execution_timeout=timedelta(minutes=30)) 
    def run_silver():
        context = get_current_context()
        env = context["params"]["env"]
        run_command(["-m", "job_plat.cli", "silver", "--env", env])
    
    @task(execution_timeout=timedelta(minutes=30)) 
    def run_gold():
        context = get_current_context()
        env = context["params"]["env"]
        run_command(["-m", "job_plat.cli", "gold", "--env", env])
        
    wait_for_bronze >> run_silver() >> run_gold()

dag = processing_dag()
