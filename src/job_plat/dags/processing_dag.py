from airflow.decorators import dag
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


def should_trigger_weekly(**kwargs):
    return datetime.utcnow().weekday() == 0

@dag(schedule=None, params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=1),})
def processing_dag():
    
    run_silver = SparkSubmitOperator(
        task_id="run_silver",
        application="/opt/jobplat/src/job_plat/runners/data/silver_runner.py",
        application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        conn_id="spark_default",  
        #conf={"spark.submit.deployMode": "client"}, 
        conf={"spark.submit.deployMode": "cluster",
            #"spark.submit.pyFiles": "/opt/jobplat/src",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "file:/tmp/spark-events"}, 
        execution_timeout=timedelta(minutes=30),
        verbose=True,
    )
    
    run_gold = SparkSubmitOperator(
        task_id="run_gold",
        application="/opt/jobplat/src/job_plat/runners/data/gold_runner.py",
        application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        conn_id="spark_default", 
        #conf={"spark.submit.deployMode": "client"},
        conf={"spark.submit.deployMode": "cluster",
            #"spark.submit.pyFiles": "/opt/jobplat/src",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "file:/tmp/spark-events"}, 
        execution_timeout=timedelta(minutes=30),
        verbose=True,
    )
    
    weekly_gate = ShortCircuitOperator(
        task_id="weekly_gate",
        python_callable=should_trigger_weekly,
    )
    
    trigger_ml = TriggerDagRunOperator(
        task_id="trigger_ml",
        trigger_dag_id="ml_dag",
        conf={"env": "{{ params.env }}"},
    )
        
    run_silver >> run_gold >> weekly_gate >> trigger_ml

dag = processing_dag()


#######################

# @dag(schedule="@daily", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
# def processing_dag():
    
    # wait_for_bronze = ExternalTaskSensor(
        # task_id="wait_for_bronze",
        # external_dag_id="ingestion_dag",
        # external_task_id="ingest_jobs",
        # mode="reschedule",
        # timeout=600,
        # execution_delta=timedelta(hours=1)
    # )
    
    
    # run_silver = SparkSubmitOperator(
        # task_id="run_silver",
        # #application="/opt/spark/jobs/job_plat/runners/data/silver_runner.py",
        # application="/opt/jobplat/src/job_plat/runners/data/silver_runner.py",
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        # conn_id="spark_default",  # keep it real
        # conf={
        # #"spark.master": "spark://spark-master:7077",
        # "spark.submit.deployMode": "client"}, 
        # #conf={
        # #"spark.master": "spark://spark-master:7077"#,
        # #"spark.submit.deployMode": "cluster"
        # #},
        # #master=None,
        # #deploy_mode="client", #"cluster",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
    
    # run_gold = SparkSubmitOperator(
        # task_id="run_gold",
        # #application="/opt/spark/jobs/job_plat/runners/data/gold_runner.py",
        # application="/opt/jobplat/src/job_plat/runners/data/gold_runner.py",
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        # conn_id="spark_default",  # keep it real
        # conf={#"spark.master": "spark://spark-master:7077",
        # "spark.submit.deployMode": "client"},
        # #conf={
        # #"spark.master": "spark://spark-master:7077"#,
        # #"spark.submit.deployMode": "cluster"
        # #},
        # #master=None,
        # #deploy_mode="client", #"cluster",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
        
    # wait_for_bronze >> run_silver >> run_gold

# processing_dag()


##########################
# @dag(
    # schedule="@daily",
    # start_date=datetime(2024, 1, 1),
    # catchup=False,
    # params={"env": "dev"},
    # default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
# )
# def processing_dag():
    # wait_for_bronze = ExternalTaskSensor(
        # task_id="wait_for_bronze",
        # external_dag_id="ingestion_dag",
        # external_task_id="ingest_jobs",
        # mode="reschedule",
        # timeout=600,
        # execution_delta=timedelta(hours=1)
    # )

    # run_silver = LivyOperator(
        # task_id="run_silver",
        # file="local:///opt/spark/jobs/job_plat/runners/data/silver_runner.py",
        # livy_conn_id="livy_default",
        # conf={"spark.master": "spark://spark-master:7077", "spark.app.name": "silver-processing"},
        # args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"]
    # )

    # run_gold = LivyOperator(
        # task_id="run_gold",
        # file="local:///opt/spark/jobs/job_plat/runners/data/gold_runner.py",
        # livy_conn_id="livy_default",
        # conf={"spark.master": "spark://spark-master:7077", "spark.app.name": "gold-processing"},
        # args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"]
    # )

    # wait_for_bronze >> run_silver >> run_gold

# processing_dag()


########################
# @dag(schedule="@daily", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
# def processing_dag():
    
    # wait_for_bronze = ExternalTaskSensor(
        # task_id="wait_for_bronze",
        # external_dag_id="ingestion_dag",
        # external_task_id="ingest_jobs",
        # mode="reschedule",
        # timeout=600,
        # execution_delta=timedelta(hours=1)
    # )
    
    
    # run_silver = SparkSubmitOperator(
        # task_id="run_silver",
        # application=spark_app("data/silver_runner.py"),
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        # conn_id="spark_default",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
    
    # run_gold = SparkSubmitOperator(
        # task_id="run_gold",
        # application=spark_app("data/gold_runner.py"),
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        # conn_id="spark_default",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
        
    # wait_for_bronze >> run_silver >> run_gold

# processing_dag()
##########################
# @dag(schedule="@daily", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
# def processing_dag():
    
    # wait_for_bronze = ExternalTaskSensor(
        # task_id="wait_for_bronze",
        # external_dag_id="ingestion_dag",
        # external_task_id="ingest_jobs",
        # mode="reschedule",
        # timeout=600,
        # execution_delta=timedelta(hours=1)
    # )
    
    
    # run_silver = SparkSubmitOperator(
        # task_id="run_silver",
        # application="/app/src/job_plat/cli.py",
        # application_args=["silver", "--env", "{{ params.env }}"],
        # conn_id="spark_default",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
    
    # run_gold = SparkSubmitOperator(
        # task_id="run_gold",
        # application="/app/src/job_plat/cli.py",
        # application_args=["gold", "--env", "{{ params.env }}"],
        # conn_id="spark_default",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
        
    # wait_for_bronze >> run_silver >> run_gold

# dag = processing_dag()


##############


# from airflow.decorators import dag, task
# from airflow.operators.python import get_current_context
# from airflow.sensors.external_task import ExternalTaskSensor
# from datetime import datetime, timedelta
# from job_plat.dags.dag_helpers import run_command


# @dag(schedule="@daily", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
# def processing_dag():
    
    # wait_for_bronze = ExternalTaskSensor(
        # task_id="wait_for_bronze",
        # external_dag_id="ingestion_dag",
        # external_task_id="ingest_jobs",
        # mode="reschedule",
        # timeout=600,
        # execution_delta=timedelta(hours=1)
    # )
    
    # @task(execution_timeout=timedelta(minutes=30)) 
    # def run_silver():
        # context = get_current_context()
        # env = context["params"]["env"]
        # run_command(["-m", "job_plat.cli", "silver", "--env", env])
    
    # @task(execution_timeout=timedelta(minutes=30)) 
    # def run_gold():
        # context = get_current_context()
        # env = context["params"]["env"]
        # run_command(["-m", "job_plat.cli", "gold", "--env", env])
        
    # wait_for_bronze >> run_silver() >> run_gold()

# dag = processing_dag()
