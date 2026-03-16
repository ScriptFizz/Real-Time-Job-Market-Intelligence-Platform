from datetime import date
from pathlib import Path
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    count, 
    sum
)
from job_plat.context.contexts import BronzeContext, SilverContext, DataPipelineContext
from job_plat.pipeline.core.base_stage import BaseStage
from job_plat.transformations.silver.enrichment.build_job_skills import run_job_skills
from job_plat.utils.helpers import union_all
from job_plat.transformations.silver.cleaning.clean_jobs import clean_jobs, deduplicate_jobs
from job_plat.transformations.silver.validation.quality_checks import run_quality_checks
from typing import List
from job_plat.storage.storages import Storage
from job_plat.ingestion.metadata import StageExecutionContext
from job_plat.schemas.output_schemas import SilverOutputs
from job_plat.pipeline.datasets.dataset_definitions import BronzeJobs
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.partitioning.partition_manager import PartitionManager


class SilverStage(BaseStage):
    
    STAGE_NAME = "silver"
    INPUT_MAP = {"job_bronze_df": BronzeJobs}
    OUTPUT_TYPE = SilverOutputs
    
    def __init__(
        self, 
        silver_ctx: SilverContext, 
        bronze_ctx: BronzeContext,
        datasets: DatasetRegistry,
        partition_manager: PartitionManager,
        ):
        
        super().__init__(spark=silver_ctx.spark, datasets=datasets, partition_manager=partition_manager)
        self.silver_ctx = silver_ctx
        self.bronze_ctx = bronze_ctx
        self._metrics = {}
        
    
    def create_context(self) -> StageExecutionContext:
        run_context = StageExecutionContext(
            stage=self.STAGE_NAME,
            pipeline_version="1.0.0"
        )
        return run_context
    
    def transform(
        self, 
        job_bronze_df: DataFrame
        ) -> SilverOutputs:
        
        df_normalized = job_bronze_df.select(
            col("run_id"),
            col("ingestion_date"),
            col("payload.*"), 
            col("ingestion_metadata.started_at").alias("ingested_at")
        )
        
        quality_metrics = (
            df_normalized
            .agg(
                count("*").alias("total"),
                sum(when(col("job_title_raw").isNull(), 1).otherwise(0)).alias("null_titles"),
                sum(when(col("description_raw").isNull(), 1).otherwise(0)).alias("null_descriptions"),
            )
            .first()
        )
        
        self._metrics = {
                "total": quality_metrics["total"], 
                "null_titles": quality_metrics["null_titles"], 
                "null_descriptions": quality_metrics["null_descriptions"]
                }
        
        self.logger.info("building_df_clean")
        df_clean = clean_jobs(df=df_normalized)
        
        self.logger.info("building_jobs_silver")
        jobs_silver_df = deduplicate_jobs(df_clean)
        
        self.logger.info("building_job_skills_silver")
        job_skills_silver_df = run_job_skills(jobs_silver_df = jobs_silver_df)
        
        
        return SilverOutputs(
            silver_jobs=jobs_silver_df,
            silver_job_skills=job_skills_silver_df,
        )
    
    def compute_metrics(self, outputs: SilverOutputs) -> dict:
        return self._metrics
    
    def evaluate_metrics(self, metrics: dict):
        if metrics["null_titles"] > 0:
            self.logger.warning(
                "data_quality_issue",
                extra={"issue": "null_titles_detected"}
            )
        
        if metrics["null_descriptions"] > 0:
            self.logger.warning(
                "data_quality_issue",
                extra={"issue": "null_descriptions_detected"}
            )
