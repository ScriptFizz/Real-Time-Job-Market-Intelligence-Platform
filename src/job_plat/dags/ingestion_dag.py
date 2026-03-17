from airflow.decorators import dag, task
from datetime import datetime
import logging
from job_plat.pipeline.stages.data.bronze_stage import BronzeStage
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.ingestion.connectors import build_connectors
from job_plat.context.contexts import ExecutionParams
from job_plat.utils.helpers import create_spark, parse_date
from job_plat.context.context_builders import build_bronze_context
from job_plat.utils.helpers import build_common


@dag(schedule="@hourly", start_date=datetime(2024, 1, 1), catchup=False)
def ingestion_dag():
    
    @task
    def ingest_jobs():
        env_config, spark, storage, _, _, connectors = build_common()
        try:
            execution = ExecutionParams(None, None, None)
            bronze_ctx = build_bronze_context(
                config=env_config, 
                execution=execution
                )
            for connector in connectors:
                stage = BronzeStage(
                    bronze_ctx=bronze_ctx, 
                    storage = storage, 
                    connector = connector
                    )
                stage.execute()
        finally:
            spark.stop()
    
    ingest_jobs()

dag = ingestion_dag()
