from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import subprocess


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def processing_dag():
    
    @task 
    def run_silver():
        result = subprocess.run(
            [
                "python", "-m", "job_plat.cli", "silver"
            ],

            check=True
        )
    
    @task 
    def run_gold():
        result = subprocess.run(
            [ 
                "python", "-m", "job_plat.cli", "gold"
            ],
            check=True
        )
    run_silver() >> run_gold()

dag = processing_dag()

