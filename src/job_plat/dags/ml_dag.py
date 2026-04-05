from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from job_plat.dags.dag_helpers import spark_app
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(schedule="@weekly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
def ml_dag():
    
    wait_for_gold = ExternalTaskSensor(
        task_id="wait_for_gold",
        external_dag_id="processing_dag",
        external_task_id="run_gold",
        mode="reschedule",
        timeout=600,
    )
    
    
    run_features = SparkSubmitOperator(
        task_id="run_features",
        application=spark_app("ml/feature_runner.py"),
        application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        conn_id="spark_default",
        execution_timeout=timedelta(minutes=30),
        verbose=True,
    )
    
    run_ml = SparkSubmitOperator(
        task_id="run_ml",
        application=spark_app("ml/ml_runner.py"),
        #application_args=["ml", "--execution-date", execution_date, "--env", "{{ params.env }}"],
        application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        conn_id="spark_default",
        execution_timeout=timedelta(minutes=30),
        verbose=True,
    )
    
    wait_for_gold >> run_features >> run_ml

dag = ml_dag()




# @dag(schedule="@weekly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
# def ml_dag():
    
    # wait_for_gold = ExternalTaskSensor(
        # task_id="wait_for_gold",
        # external_dag_id="processing_dag",
        # external_task_id="run_gold",
        # mode="reschedule",
        # timeout=600,
    # )
    
    
    # @task(execution_timeout=timedelta(minutes=30))
    # def run_features():
        
        # context = get_current_context()
        # execution_date = context["logical_date"].isoformat()
        # env = context["params"]["env"]
        # run_command(["-m", "job_plat.cli", "feature", "--execution-date", execution_date, "--env", env])
            
    # @task(execution_timeout=timedelta(minutes=30))
    # def run_ml():
        
        # context = get_current_context()
        # execution_date = context["logical_date"].isoformat()
        # env = context["params"]["env"]
        # run_command([ "-m", "job_plat.cli", "ml", "--execution-date", execution_date, "--env", env])
    
    # wait_for_gold >> run_features() >> run_ml()

# dag = ml_dag()
