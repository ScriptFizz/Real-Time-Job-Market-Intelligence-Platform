from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import subprocess
# from job_plat.utils.helpers import build_common
# import logging
# from job_plat.pipeline.stages.data.silver_stage import SilverStage
# from job_plat.pipeline.stages.data.gold_stage import GoldStage
# from job_plat.partitioning.partition_manager import PartitionManager
# from job_plat.ingestion.connectors import build_connectors
# from job_plat.context.contexts import ExecutionParams
# from job_plat.utils.helpers import create_spark, parse_date
# from job_plat.context.context_builders import build_data_pipeline_context



@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def processing_dag():
    
    # wait_for_ingestion = ExternalTaskSensor(
        # task_id="wait_for_ingestion",
        # external_dag_id="ingestion_dag",
        # external_task_id="ingest_jobs",
    # )
    
    @task 
    def run_silver():
        subprocess.run(
            [
                "poetry", "run", "python", "-m", "job_plat.cli", "silver"
            ],
            check=True
        )
    
    @task 
    def run_gold():
        subprocess.run(
            [
                "poetry", "run", "python", "-m", "job_plat.cli", "gold"
            ],
            check=True
        )
    
    wait_for_ingestion >> run_silver() >> run_gold()

dag = processing_dag()



# @dag(shedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
# def processing_dag():
    
    # wait_for_ingestion = ExternalTaskSensor(
        # task_id="wait_for_ingestion",
        # external_dag_id="ingestion_dag",
        # external_task_id="ingest_jobs",
    # )
    
    # @task 
    # def run_silver():
        # env_config, spark, _, datasets, partition_manager, _ = build_common()
        # try:
            # execution = ExecutionParams(None, None, None)
            # pipeline_ctx = build_data_pipeline_context(
                # execution=execution, 
                # config=env_config,
                # spark=spark
                # )
            # stage = SilverStage(
                # silver_ctx=pipeline_ctx.silver, 
                # bronze_ctx=pipeline_ctx.bronze,
                # datasets= datasets, 
                # partition_manager=partition_manager
                # )
            # stage.execute()
        # finally:
            # spark.stop()
    
    # @task 
    # def run_gold():
        # env_config, spark, _, datasets, partition_manager, _ = build_common()
        # try:
            # execution = ExecutionParams(None, None, None)
            # pipeline_ctx = build_data_pipeline_context(
                # execution=execution, 
                # config=env_config,
                # spark=spark
                # )
            # stage = GoldStage(
                # silver_ctx=pipeline_ctx.silver, 
                # gold_ctx=pipeline_ctx.gold,
                # datasets= datasets, 
                # partition_manager=partition_manager
                # )
            # stage.execute()
        # finally:
            # spark.stop()
    
    # wait_for_ingestion >> run_silver() >> run_gold()

# dag = processing_dag()
