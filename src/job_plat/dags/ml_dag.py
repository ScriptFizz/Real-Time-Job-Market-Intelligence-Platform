from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import logging
from job_plat.pipeline.stages.ml.feature_stage import FeatureStage
from job_plat.pipeline.stages.ml.ml_stage import MLStage
from job_plat.context.context_builders import build_ml_pipeline_context
from job_plat.utils.helpers import build_common

@dag(schedule="@weekly", start_date=datetime(2024, 1, 1), catchup=False)
def ml_dag():
    
    @task
    def run_features():
        env_config, spark, _, datasets, partition_manager, _ = build_common()
        try:
            pipeline_ctx = build_ml_pipeline_context(
                config=env_config,
                spark=spark
                )
            stage = FeatureStage(
                ctx=pipeline_ctx.feature, 
                datasets= datasets, 
                partition_manager=partition_manager
                )
            stage.execute()
        finally:
            spark.stop()
            
    @task
    def run_ml():
        env_config, spark, _, datasets, partition_manager, _ = build_common()
        try:
            pipeline_ctx = build_ml_pipeline_context(
                config=env_config,
                spark=spark
                )
            stage = MLStage(
                ctx=pipeline_ctx.ml, 
                datasets= datasets, 
                partition_manager=partition_manager
                )
            stage.execute()
        finally:
            spark.stop()
    
   run_features() >> run_ml()

dag = ml_dag()
