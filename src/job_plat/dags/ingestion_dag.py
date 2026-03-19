from airflow.decorators import dag, task
from datetime import datetime
import subprocess


@dag(schedule="@hourly", start_date=datetime(2024, 1, 1), catchup=False)
def ingestion_dag():
    
    @task
    def ingest_jobs():
        subprocess.run(
            [
                "python", "-m", "job_plat.cli", "bronze"
            ],
            check=True
        )
    
    ingest_jobs()

dag = ingestion_dag()
