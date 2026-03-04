from datetime import date
from pathlib import Path
import logging
from pyspark.sql import DataFrame
from job_plat.pipelines.context.contexts import BronzeContext, SilverContext, PipelineContext
from job_plat.pipelines.stages.base_stage import BaseStage
#from job_plat.silver.cleaning.clean_jobs import run_clean
from job_plat.silver.enrichment.build_job_skills import run_job_skills
from job_plat.utils.helpers import union_all
from job_plat.silver.cleaning.clean_jobs import normalize_jobs, clean_jobs, deduplicate_jobs
from job_plat.silver.validation.quality_checks import run_quality_checks
from typing import List
from job_plat.utils.storage import Storage
from job_plat.bronze.ingestion.metadata import StageExecutionContext


class SilverStage(BaseStage):
    
    def __init__(
        self, 
        silver_ctx: SilverContext, 
        bronze_ctx: BronzeContext,
        storage: Storage):
        super().__init__(spark=silver_ctx.spark, storage=storage)
        self.silver_ctx = silver_ctx
        self.bronze_ctx = bronze_ctx
        
    def validate_inputs(self) -> None:
        
        bronze_root = self.bronze_ctx.bronze_root
        if not bronze_root.exists():
            raise FileNotFoundError(
                f"Missing input dataset directory: {bronze_root}"
            )
    
    def read(self) -> dict:
        bronze_root = str(self.bronze_ctx.bronze_root)
        
        df = self.spark.read.json(bronze_root)
        
        date_range = self.silver_ctx.date_range
        
        # Read all data
        if date_range.is_full_load():
            return {"job_bronze_df": df}
        
        # Read a date range
        if date_range.start_date and date_range.end_date:
            df = df.where(
                col("ingestion_date").between(
                    str(date_range.start_date),
                    str(date_range.end_date)
                )
            )
        elif date_rage.start_date:
            df = df.where(
                col("ingestion_date") == str(date_range.start_date)
            )
        else:
            raise ValueError("Invalid DateRage configuration.")
        
        return {"job_bronze_df": df}
    
    def create_context(self) -> StageExecutionContext:
        run_context = StageExecutionContext(
            stage="silver",
            pipeline_version="1.0.0"
        )
        return run_context
    
    def transform(
        self, 
        inputs: dict
        ) -> dict:
        
        job_bronze_df = inputs["job_bronze_df"]
        df_normalized = normalize_jobs(df=job_bronze_df)
        
        self.logger.info("building_df_clean")
        df_clean = clean_jobs(df=df_normalized)
        
        self.logger.info("building_jobs_silver")
        jobs_silver_df = deduplicate_jobs(df_clean)
        
        self.logger.info("building_job_skills_silver")
        job_skills_silver_df = run_job_skills(jobs_silver_df = jobs_silver_df)
        
        data_date = self.silver_ctx.data_date
        jobs_silver_df = jobs_silver_df.withColumn("data_date", lit(data_date))
        job_skills_silver_df = job_skills_silver_df.withColumn("data_date", lit(data_date))
        
        return {
            "jobs_normalized": df_normalized,
            "jobs_silver": jobs_silver_df,
            "job_skills_silver": job_skills_silver_df
        }
    
    def compute_metrics(self, outputs: dict) -> dict:
        
        df_normalized = outputs["jobs_normalized"]
        quality_metrics = (
            df_normalized
            .agg(
                count("*").alias("total"),
                sum(when(col("job_title_raw").isNull(), 1).otherwise(0)).alias("null_titles"),
                sum(when(col("description_raw").isNull(), 1).otherwise(0)).alias("null_descriptions"),
            )
            .first()
        )
        return {
                "total": quality_metrics["total"], 
                "null_titles": quality_metrics["null_titles"], 
                "null_descriptions": quality_metrics["null_descriptions"]
                }
    
    def write(self, outputs: dict) -> None:
        
        self.spark.conf.set(
            "spark.sql.sources.partitionOverWriteMode",
            "dynamic"
        )
        
        for name, mode, path in [
            ("jobs_silver", "overwrite", self.silver_ctx.jobs_path),
            ("job_skills_silver", "overwrite", self.silver_ctx.job_skills_path)
        ]:
            
            self.storage.write_dataframe(
                df=outputs[name],
                path=path,
                mode=mode,
                partition_cols=["data_date"]
                )

#################

# def transform(
        # self, 
        # job_bronze_df: DataFrame,
        # logger: logging.Logger
        # ) -> dict:
        
        
        # df_normalized = normalize_jobs(df=job_bronze_df)
        
        # ########### TO LOG!!!!!!!!!!! #########
        # quality_metrics = (
            # df_normalized
            # .select(
                # count("*").alias("total"),
                # sum(when(col("job_title_raw").isNull(), 1).otherwise(0)).alias("null_titles"),
                # sum(when(col("description_raw").isNull(), 1).otherwise(0)).alias("null_descriptions"),
            # )
            # .first()
        # )
        
        # df_clean = clean_jobs(df=df_normalized)
    
        # jobs_silver_df = deduplicate_jobs(df_clean)
        
        # job_skills_silver_df = run_job_skills(jobs_silver_df = jobs_silver_df)
        
        # data_date = self.silver_ctx.data_date
        # jobs_silver_df = jobs_silver_df.withColumn("data_date", lit(data_date))
        # job_skills_silver_df = job_skills_silver_df.withColumn("data_date", lit(data_date))
        
        # return {
            # "jobs_silver": jobs_silver_df,
            # "job_skills_silver": job_skills_silver_df
        # }
###################


# class SilverStage(BaseStage):
    
    # def __init__(
        # self, 
        # silver_ctx: SilverContext, 
        # bronze_ctx: BronzeContext
        # storage: Storage):
        # super().__init__(spark=silver_ctx.spark, storage=storage)
        # self.silver_ctx = silver_ctx
        # self.bronze_ctx = bronze_ctx
        
    # def validate_inputs(self) -> None:
        
        # missing = []
        
        # for path in self.bronze_ctx.jobs_path_list:
            # if not self._path_exists(path):
                # missing.append(str(path))
        
        # if missing:
            # raise FileNotFoundError(
                # f"Missing input datasets: {', '.join(missing)}"
            # )
    
    # def read(self) -> dict:
        # return {
            # "job_bronze_dfs":
                # [self.spark.read.parquet(path) for path in self.bronze_ctx.jobs_path_list]
        # }
    
    # def transform(
        # self, 
        # job_bronze_dfs: List[DataFrame],
        # ) -> dict:
        
        # dfs = []
        # for df in job_bronze_dfs:
            # df_clean = clean_jobs(df=df)
            # dfs.append(df_clean)
        
        # jobs_silver_df = union_all(dfs)
        # jobs_silver_df = deduplicate_jobs(jobs_silver_df)
        
        # job_skills_silver_df = run_job_skills(jobs_silver_df = jobs_silver_df)
        
        # data_date = self.silver_ctx.data_date
        # jobs_silver_df = jobs_silver_df.withColumn("data_date", lit(data_date))
        # job_skills_silver_df = job_skills_silver_df.withColumn("data_date", lit(data_date))
        
        # return {
            # "jobs_silver": job_silver_df,
            # "job_skills_silver": job_skills_silver_df
        # }
    
    # def write(self, outputs: dict) -> None:
        
        # self.spark.conf.set(
            # "spark.sql.sources.partitionOverWriteMode",
            # "dynamic"
        # )
        
        # for name, mode, path in [
            # ("jobs_silver", "overwrite", self.silver_ctx.jobs_path),
            # ("job_skills_", "overwrite", self.silver_ctx.job_skills_path)
        # ]:
            
            # self.storage.write_dataframe(
                # df=outputs[name],
                # path=path,
                # mode=mode,
                # partition_cols=["data_date"]
                # )
