from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from job_plat.dags.dag_helpers import spark_app
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(schedule="@hourly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
def ingestion_dag():
    
    ingest_jobs = SparkSubmitOperator(
        task_id="ingest_jobs",
        application=spark_app("data/bronze_runner.py"),
        application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        conn_id="spark_default",
        execution_timeout=timedelta(minutes=30),
        verbose=True,
    )
    
    ingest_jobs

dag = ingestion_dag()



##################################################


# @dag(schedule="@hourly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
# def ingestion_dag():
    
    # ingest_jobs = SparkSubmitOperator(
        # task_id="ingest_jobs",
        # application="/app/src/job_plat/cli.py",
        # application_args=["bronze", "--env", "{{ params.env }}"],
        # conn_id="spark_default",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
    
    # ingest_jobs

# dag = ingestion_dag()


####################################################


# @dag(schedule="@hourly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
# def ingestion_dag():
    
    
    # @task(execution_timeout=timedelta(minutes=30))
    # def ingest_jobs():
        # context = get_current_context()
        # env = context["params"]["env"]
        # run_command(["-m", "job_plat.cli", "bronze", "--env", env])
    
    # ingest_jobs()

# dag = ingestion_dag()
