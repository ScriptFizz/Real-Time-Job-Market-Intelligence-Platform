from job_plat.pipeline.core.base_stage import BaseStage
from pyspark.sql import DataFrame
from job_plat.context.contexts import SilverContext, GoldContext
from job_plat.transformations.gold.v1_analytics.build_dimensions import build_dim_jobs, build_dim_skills
from job_plat.transformations.gold.v1_analytics.fact_job_skills import build_fact_job_skills
from job_plat.storage.storages import Storage
from pathlib import Path
from job_plat.ingestion.metadata import StageExecutionContext
from job_plat.schemas.output_schemas import GoldOutputs
from job_plat.pipeline.datasets.dataset_definitions import SilverJobSkills, SilverJobs
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.partitioning.partition_manager import PartitionManager

class GoldStage(BaseStage):
    
    STAGE_NAME = "gold"
    INPUT_MAP = {"job_silver_df": SilverJobs, "job_skills_silver_df": SilverJobSkills}
    OUTPUT_TYPE = GoldOutputs
    
    def __init__(
        self, 
        gold_ctx: GoldContext, 
        datasets: DatasetRegistry,
        partition_manager: PartitionManager,):
        super().__init__(spark=gold_ctx.spark, datasets=datasets, partition_manager=partition_manager, ctx = gold_ctx)
        
        # self.gold_ctx = gold_ctx
        # self.silver_ctx = silver_ctx
    
    def create_context(self) -> StageExecutionContext:
        run_context = StageExecutionContext(
            stage=self.STAGE_NAME,
            pipeline_version="1.0.0"
        )
        return run_context
    
    def transform(
        self, 
        job_silver_df: DataFrame,
        job_skills_silver_df: DataFrame
        ) -> GoldOutputs:
        
        # Skip transform if no new partitions
        if job_silver_df is None or job_skills_silver_df is None:
            raise StageSkip("no new partitions")
        
        self.logger.info("building_dim_jobs")
        dim_jobs_df = build_dim_jobs(job_silver_df=job_silver_df)
        
        self.logger.info("building_dim_skills")
        dim_skills_df = build_dim_skills(job_skills_silver_df=job_skills_silver_df)
        
        self.logger.info("building_fact_jobs")
        fact_df = build_fact_job_skills(
            job_skills_silver_df=job_skills_silver_df,
            dim_skills_df=dim_skills_df
        )

        return GoldOutputs(
            dim_jobs=dim_jobs_df,
            dim_skills=dim_skills_df,
            fact_job_skills=fact_df
        )
        
    def compute_metrics(self, outputs: GoldOutputs) -> dict:
        
        dim_jobs_df = outputs.dim_jobs
        dim_skills_df = outputs.dim_skills
        fact_df = outputs.fact_job_skills
        
        if dim_jobs_df is None or dim_skills_df is None or fact_df is None:
            return {}
        
        dim_jobs_df.cache()
        dim_skills_df.cache()
        fact_df.cache()
        
        jobs = dim_jobs_df.count()
        skills =  dim_skills_df.count()
        fact_rows = fact_df.count()
        orphan_facts_detected = (
                fact_df
                .join(dim_jobs_df.select("job_id"), "job_id", "left_anti")
                .limit(1)
                .count()
            ) > 0
        
        dim_jobs_df.unpersist()
        dim_skills_df.unpersist()
        fact_df.unpersist()
        
        return {
            "jobs": jobs,
            "skills": skills,
            "fact_rows": fact_rows,
            "fact_per_job_ratio": round(
                fact_rows / jobs, 2
            ) if jobs else 0,
            "orphan_facts_detected": orphan_facts_detected
        }
    
    def evaluate_metrics(self, metrics: dict) -> None:
        
        if not metrics:
            return
        
        if metrics.get("orphan_facts_detected"):
            self.logger.warning(
                "data_quality_issue",
                extra={"issue": "orphan_facts_detected"}
            )
        
        if metrics.get("fact_per_job_ratio") > self.ctx.fact_per_job_ratio_threshold:
            self.logger.warning(
                "data_anomaly_detected",
                extra={
                    "issue": "fact_per_job_ratio_high",
                    "value": metrics["fact_per_job_ratio"],
                    "threshold": self.ctx.fact_per_job_ratio_threshold,
                }
            )
        
        if metrics.get("jobs") == 0:
            self.logger.error(
                "data_quality_issue",
                extra={"issue": "no_jobs_generated"}
            )
