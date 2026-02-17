from datetime import date
from pathlib import Path
from pyspark.sql import DataFrame
from job_plat.config.context import BronzeContext, SilverContext, PipelineContext
from job_plat.pipelines.stages import BaseStage
from job_plat.silver.cleaning.run_clean import run_clean
from job_plat.utils.helpers import union_all
from job_plat.processing.clean_jobs import clean_jobs, deduplicate_jobs
from job_plat.silver.validation.quality_checks import run_quality_checks
from typing import List


class SilverStage(BaseStage):
    
    def __init__(self, silver_ctx: SilverContext, bronze_ctx: BronzeContext):
        super().__init__(silver_ctx.spark)
        self.silver_ctx = silver_ctx
        self.bronze_ctx = bronze_ctx
        
    def validate_inputs(self) -> None:
        
        missing = []
        
        for path in self.bronze_ctx.jobs_path_list:
            if not self._path_exists(path):
                missing.append(str(path))
        
        if missing:
            raise FileNotFoundError(
                f"Missing input datasets: {', '.join(missing)}"
            )
    
    def read(self) -> dict:
        return {
            "job_bronze_dfs":
                [self.spark.read.parquet(path) for path in self.bronze_ctx.jobs_path_list]
        }
    
    def transform(
        self, 
        job_bronze_dfs: List[DataFrame],
        ) -> dict:
        
        dfs = []
        for df in job_bronze_dfs:
            df_clean = clean_jobs(df=df)
            dfs.append(df_clean)
        
        silver_df = union_all(dfs)
        silver_df = deduplicate_jobs(silver_df)
        
        data_date = self.silver_ctx.data_date
        silver_df = silver_df.withColumn("data_date", lit(data_date))
        
        return {
            "dim_jobs": dim_jobs_df,
            "dim_skills": dim_skills_df,
            "fact_job_skills": fact_df
        }
    
    def write(self, outputs: dict) -> None:
        
        self.spark.conf.set(
            "spark.sql.sources.partitionOverWriteMode",
            "dynamic"
        )
        
        for name, mode, path in [
            ("dim_jobs", "overwrite", self.gold_v1_ctx.dim_jobs_path),
            ("dim_skills", "overwrite", self.gold_v1_ctx.dim_skills_path),
            ("fact_job_skills", "append", self.gold_v1_ctx.fact_job_skill_path),
        ]:
            
            outputs[name] \
            .write \
            .mode(mode) \
            .partitionBy("data_date") \
            .parquet(path)
