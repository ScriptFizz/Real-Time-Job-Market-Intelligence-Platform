from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime
import subprocess


@dag(schedule="@weekly", start_date=datetime(2024, 1, 1), catchup=False)
def ml_dag():
    
    @task
    def run_features():
        
        context = get_current_context()
        execution_date = context["logical_date"].isoformat()
        subprocess.run(
            [
                "python", "-m", "job_plat.cli", "feature",
                "--execution-date", execution_date,
            ],
            check=True
        )
            
    @task
    def run_ml():
        
        context = get_current_context()
        execution_date = context["logical_date"].isoformat()
        subprocess.run(
            [
                "python", "-m", "job_plat.cli", "ml",
                "--execution-date", execution_date,
            ],

            check=True
        )
    
    run_features() >> run_ml()

dag = ml_dag()
