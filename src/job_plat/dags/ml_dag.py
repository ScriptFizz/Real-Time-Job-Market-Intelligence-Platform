from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

@dag(schedule=None, params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=1),})
def ml_dag():
    
    run_features = SparkSubmitOperator(
        task_id="run_features",
        application="/opt/jobplat/src/job_plat/runners/data/feature_runner.py",
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
    
    run_ml = SparkSubmitOperator(
        task_id="run_ml",
        application="/opt/jobplat/src/job_plat/runners/data/ml_runner.py",
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
    
    run_features >> run_ml

dag = ml_dag()


####

# @dag(schedule="@weekly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
# def ml_dag():
    
    # wait_for_gold = ExternalTaskSensor(
        # task_id="wait_for_gold",
        # external_dag_id="processing_dag",
        # external_task_id="run_gold",
        # mode="reschedule",
        # timeout=600,
    # )
    
    
    # run_features = SparkSubmitOperator(
        # task_id="run_features",
        # #application="/opt/spark/jobs/job_plat/runners/ml/feature_runner.py",
        # application="/opt/jobplat/src/job_plat/runners/data/feature_runner.py",
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        # conn_id="spark_default",  # keep it real
        # conf={#"spark.master": "spark://spark-master:7077",
        # "spark.submit.deployMode": "client"}, 
        # #conf={
        # #"spark.master": "spark://spark-master:7077"#,
        # #"spark.submit.deployMode": "cluster"
        # #},
        # #deploy_mode="client", #"cluster",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
    
    # run_ml = SparkSubmitOperator(
        # task_id="run_ml",
        # #application="/opt/spark/jobs/job_plat/runners/ml/ml_runner.py",
        # application="/opt/jobplat/src/job_plat/runners/data/ml_runner.py",
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        # conn_id="spark_default",  # keep it real
        # conf={#"spark.master": "spark://spark-master:7077",
        # "spark.submit.deployMode": "client"}, 
        # #conf={
        # #"spark.master": "spark://spark-master:7077"#,
        # #"spark.submit.deployMode": "cluster"
        # #},
        # #deploy_mode="client", #"cluster",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
    
    # wait_for_gold >> run_features >> run_ml

# ml_dag()

#####
# @dag(
    # schedule="@weekly",
    # start_date=datetime(2024, 1, 1),
    # catchup=False,
    # params={"env": "dev"},
    # default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
# )
# def ml_dag():
    # wait_for_gold = ExternalTaskSensor(
        # task_id="wait_for_gold",
        # external_dag_id="processing_dag",
        # external_task_id="run_gold",
        # mode="reschedule",
        # timeout=600
    # )

    # run_features = LivyOperator(
        # task_id="run_features",
        # file="local:///opt/spark/jobs/job_plat/runners/ml/feature_runner.py",
        # livy_conn_id="livy_default",
        # conf={"spark.master": "spark://spark-master:7077", "spark.app.name": "feature-processing"},
        # args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"]
    # )

    # run_ml = LivyOperator(
        # task_id="run_ml",
        # file="local:///opt/spark/jobs/job_plat/runners/ml/ml_runner.py",
        # livy_conn_id="livy_default",
        # conf={"spark.master": "spark://spark-master:7077", "spark.app.name": "ml-processing"},
        # args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"]
    # )

    # wait_for_gold >> run_features >> run_ml

# ml_dag()


######################################################
# @dag(schedule="@weekly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
# def ml_dag():
    
    # wait_for_gold = ExternalTaskSensor(
        # task_id="wait_for_gold",
        # external_dag_id="processing_dag",
        # external_task_id="run_gold",
        # mode="reschedule",
        # timeout=600,
    # )
    
    
    # run_features = SparkSubmitOperator(
        # task_id="run_features",
        # application=spark_app("ml/feature_runner.py"),
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        # conn_id="spark_default",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
    
    # run_ml = SparkSubmitOperator(
        # task_id="run_ml",
        # application=spark_app("ml/ml_runner.py"),
        # #application_args=["ml", "--execution-date", execution_date, "--env", "{{ params.env }}"],
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        # conn_id="spark_default",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
    
    # wait_for_gold >> run_features >> run_ml

# ml_dag()

###################################################

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
