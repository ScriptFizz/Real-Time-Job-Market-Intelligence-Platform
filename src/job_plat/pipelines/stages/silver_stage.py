from datetime import date
from pathlib import Path
from pyspark.sql import DataFrame
from job_plat.config.context import BronzeContext, SilverContext, PipelineContext
from job_plat.pipelines.stages import BaseStage
from job_plat.silver.cleaning.run_clean import run_clean
from job_plat.silver.enrichment.build_job_skills import run_job_skills
from job_plat.utils.helpers import union_all
from job_plat.processing.clean_jobs import clean_jobs, deduplicate_jobs
from job_plat.silver.validation.quality_checks import run_quality_checks
from typing import List
from job_plat.utils.storage import Storage


class SilverStage(BaseStage):
    
    def __init__(
        self, 
        silver_ctx: SilverContext, 
        bronze_ctx: BronzeContext
        storage: Storage):
        super().__init__(spark=silver_ctx.spark, storage=storage)
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
        
        jobs_silver_df = union_all(dfs)
        jobs_silver_df = deduplicate_jobs(jobs_silver_df)
        
        job_skills_silver_df = run_job_skills(jobs_silver_df = jobs_silver_df)
        
        data_date = self.silver_ctx.data_date
        jobs_silver_df = jobs_silver_df.withColumn("data_date", lit(data_date))
        job_skills_silver_df = job_skills_silver_df.withColumn("data_date", lit(data_date))
        
        return {
            "jobs_silver": job_silver_df,
            "job_skills_silver": job_skills_silver_df
        }
    
    def write(self, outputs: dict) -> None:
        
        self.spark.conf.set(
            "spark.sql.sources.partitionOverWriteMode",
            "dynamic"
        )
        
        for name, mode, path in [
            ("jobs_silver", "overwrite", self.silver_ctx.jobs_path),
            ("job_skills_", "overwrite", self.silver_ctx.job_skills_path)
        ]:
            
            self.storage.write_dataframe(
                df=outputs[name],
                path=path,
                mode=mode,
                partition_cols=["data_date"]
                )
            
            # outputs[name] \
            # .write \
            # .mode(mode) \
            # .partitionBy("data_date") \
            # .parquet(path)
