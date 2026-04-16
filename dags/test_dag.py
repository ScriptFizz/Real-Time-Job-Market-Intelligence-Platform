from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["test"])
def test_dag():
    
    @task
    def hello():
        print("This is the first test message: Hello")
    
    @task
    def world():
        print("This is the second test message: World")
    
    hello() >> world()

dag = test_dag()
