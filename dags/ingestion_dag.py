from airflow.decorators import dag
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime, timedelta

def should_trigger_daily(**kwargs):
    return datetime.utcnow().hour == 0


@dag(schedule="@hourly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 1, "retry_delay": timedelta(minutes=1),})
def ingestion_dag():
    
    ingest_jobs = SparkKubernetesOperator(
        task_id="spark_ingestion",
        namespace="default",
        application_file="spark_ingestion.yaml",    #"/opt/airflow/dags/spark_ingestion.yaml",
        do_xcom_push=False,
        #env_vars={
        #"ENV": "{{ params.env }}",
        #"EXECUTION_DATE": "{{ ds }}",
        #},
    )
        
    daily_gate = ShortCircuitOperator(
        task_id="daily_gate",
        python_callable=should_trigger_daily,
    )
    
    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_processing",
        trigger_dag_id="processing_dag",
        conf={"env": "{{ params.env }}"},
    )
    
    ingest_jobs >> daily_gate >> trigger_processing

dag = ingestion_dag()

#############14-04-26##############

# from airflow.decorators import dag
# from airflow.operators.python import ShortCircuitOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# #from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# from datetime import datetime, timedelta

# def should_trigger_daily(**kwargs):
    # return datetime.utcnow().hour == 0


# @dag(schedule="@hourly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 1, "retry_delay": timedelta(minutes=1),})
# def ingestion_dag():
    
    # ingest_jobs = KubernetesPodOperator(
        # task_id="ingest_jobs",
        # name="spark-submit",
        # namespace="default",

        # image="jobplat-spark",

        # cmds=["/opt/spark/bin/spark-submit"],
        # arguments=[
            # #"--master", "k8s://https://kubernetes.default.svc",
            # "--master", "k8s://https://host.docker.internal:39909",
            # "--deploy-mode", "cluster",
            # "--name", "arrow-spark",

            # "--conf", "spark.kubernetes.container.image=jobplat-spark",
            # "--conf", "spark.kubernetes.namespace=default",
            # "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=default",
            # #"--conf", "spark.executor.instances=2",
            # "--conf", "spark.executor.instances=1",
            # "--conf", "spark.executor.memory=384m",
            # "--conf", "spark.driver.memory=384m",
            # "--conf spark.executor.cores=1",
            # "--conf spark.driver.cores=1",

            # "--conf", "spark.eventLog.enabled=true",
            # "--conf", "spark.eventLog.dir=file:/tmp/spark-events",

            # "local:///opt/jobplat/src/job_plat/runners/data/bronze_runner.py",
            # "--env", "{{ params.env }}",
            # "--execution-date", "{{ ts }}"
        # ],

        # get_logs=True,
        # is_delete_operator_pod=True,
    # )
        
    # daily_gate = ShortCircuitOperator(
        # task_id="daily_gate",
        # python_callable=should_trigger_daily,
    # )
    
    # trigger_processing = TriggerDagRunOperator(
        # task_id="trigger_processing",
        # trigger_dag_id="processing_dag",
        # conf={"env": "{{ params.env }}"},
    # )
    
    # ingest_jobs >> daily_gate >> trigger_processing

# dag = ingestion_dag()

############12-04-26#############

# ingest_jobs = SparkSubmitOperator(
        # task_id="ingest_jobs",
        # application="/opt/jobplat/src/job_plat/runners/data/bronze_runner.py",
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"], 
        # #conf={"spark.submit.deployMode": "client"}, 
        # conf={"spark.master": "k8s://https://kubernetes.default.svc",
        # "spark.submit.deployMode": "cluster",

        # "spark.kubernetes.container.image": "jobplat-spark",
        # "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
        # "spark.kubernetes.namespace": "default",

        # "spark.kubernetes.authenticate.driver.serviceAccountName": "default",

        # "spark.executor.instances": "2",

        # #"spark.kubernetes.driver.pod.name": "bronze-driver",

        # "spark.eventLog.enabled": "true",
        # "spark.eventLog.dir": "file:/tmp/spark-events",},
        # env_vars={"SPARK_MASTER": "k8s://https://kubernetes.default.svc"}, 
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )


###############################



# @dag(schedule="@hourly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 1, "retry_delay": timedelta(minutes=1),})
# def ingestion_dag():
    
    # ingest_jobs = SparkSubmitOperator(
        # task_id="ingest_jobs",
        # #application="/opt/spark/jobs/job_plat/runners/data/bronze_runner.py",
        # #application="/opt/airflow/src/job_plat/runners/data/bronze_runner.py",
        # application="/opt/jobplat/src/job_plat/runners/data/bronze_runner.py",
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        # conn_id="spark_default",  # keep it real
        # conf={
        # #"spark.master": "spark://spark-master:7077",
        # "spark.submit.deployMode": "client"}, 
        # #conf={
        # #"spark.master": "spark://spark-master:7077"#,
        # #"spark.submit.deployMode": "cluster"
        # #},
        # #deploy_mode="client", #"cluster",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
    
    # ingest_jobs

# dag = ingestion_dag()



# @dag(
    # schedule="@daily",
    # start_date=datetime(2024, 1, 1),
    # catchup=False,
    # params={"env": "dev"},
    # default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
# )
# def ingestion_dag():
    # ingest_jobs = LivyOperator(
        # task_id="ingest_jobs",
        # file="local:///opt/spark/jobs/job_plat/runners/data/bronze_runner.py",
        # livy_conn_id="livy_default",
        # conf={
            # "spark.master": "spark://spark-master:7077",
            # "spark.app.name": "bronze-ingestion"
        # },
        # args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"]
    # )

# ingestion_dag()

##################################

# @dag(schedule="@hourly", params={"env": "dev"}, start_date=datetime(2024, 1, 1), catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),})
# def ingestion_dag():
    
    # ingest_jobs = SparkSubmitOperator(
        # task_id="ingest_jobs",
        # application=spark_app("data/bronze_runner.py"),
        # application_args=["--env", "{{ params.env }}", "--execution-date", "{{ ts }}"],
        # conn_id="spark_default",
        # execution_timeout=timedelta(minutes=30),
        # verbose=True,
    # )
    
    # ingest_jobs

# dag = ingestion_dag()

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
