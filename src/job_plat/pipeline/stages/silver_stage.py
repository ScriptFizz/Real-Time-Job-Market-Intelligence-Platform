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
    
    STAGE_NAME = "silver_jobs"
    INPUT_DATASETS = ["bronze_jobs"]
    OUTPUT_DATASETS = ["silver_jobs", "silver_job_skills"]
    
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
        df_normalized = job_bronze_df.select(
            col("run_id"),
            col("ingestion_date"),
            col("payload.*"), 
            col("ingestion_metadata.started_at").alias("ingested_at")
        )
        
        self.logger.info("building_df_clean")
        df_clean = clean_jobs(df=df_normalized)
        
        self.logger.info("building_jobs_silver")
        jobs_silver_df = deduplicate_jobs(df_clean)
        
        self.logger.info("building_job_skills_silver")
        job_skills_silver_df = run_job_skills(jobs_silver_df = jobs_silver_df)
        
        # data_date = self.silver_ctx.data_date
        # jobs_silver_df = jobs_silver_df.withColumn("data_date", lit(data_date))
        # job_skills_silver_df = job_skills_silver_df.withColumn("data_date", lit(data_date))
        
        return {
            "jobs_normalized": df_normalized,
            "jobs_silver": jobs_silver_df,
            "job_skills_silver": job_skills_silver_df,
            "partitions": inputs["partitions"]
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
    

###########################

# class SilverStage(BaseStage):
    
    # def __init__(
        # self, 
        # silver_ctx: SilverContext, 
        # bronze_ctx: BronzeContext,
        # datasets: DatasetRegistry,
        # partition_manager: PartitionManager,
        # ):
        # self.STAGE_NAME = "silver_jobs"
        # super().__init__(spark=silver_ctx.spark, datasets=datasets, partition_manager=partition_manager)
        # self.silver_ctx = silver_ctx
        # self.bronze_ctx = bronze_ctx
        
    # def validate_inputs(self) -> None:
        
        # bronze_path = self.datasets.bronze_jobs.path
        # if not bronze_path.exists():
            # raise FileNotFoundError(
                # f"Missing input dataset directory: {bronze_path}"
            # )
    
    # def read(self) -> dict:
        
        # dataset = self.datasets.get("bronze_jobs")
        
        # # available = dataset.list_partitions()
        # # processed = self.partition_manager.get_processed(stage_name=self.STAGE_NAME)
        # # partitions = sorted(set(available) - set(processed))
        # partitions = dataset.get_available_partitions(partition_manager=self.partition_manager, stage_name=self.STAGE_NAME)
        
        # #df = dataset.read_partitions(spark=self.spark, dataset_path=dataset.path, partitions=partitions)
        # df = dataset.read_partitions(spark=self.spark, partitions=partitions)
        
        # return {"job_bronze_df": df, "partitions": partitions}
    
    # def create_context(self) -> StageExecutionContext:
        # run_context = StageExecutionContext(
            # stage="silver",
            # pipeline_version="1.0.0"
        # )
        # return run_context
    
    # def transform(
        # self, 
        # inputs: dict
        # ) -> dict:
        
        # job_bronze_df = inputs["job_bronze_df"]
        # df_normalized = job_bronze_df.select(
            # col("run_id"),
            # col("ingestion_date"),
            # col("payload.*"), 
            # col("ingestion_metadata.started_at").alias("ingested_at")
        # )
        
        # self.logger.info("building_df_clean")
        # df_clean = clean_jobs(df=df_normalized)
        
        # self.logger.info("building_jobs_silver")
        # jobs_silver_df = deduplicate_jobs(df_clean)
        
        # self.logger.info("building_job_skills_silver")
        # job_skills_silver_df = run_job_skills(jobs_silver_df = jobs_silver_df)
        
        # # data_date = self.silver_ctx.data_date
        # # jobs_silver_df = jobs_silver_df.withColumn("data_date", lit(data_date))
        # # job_skills_silver_df = job_skills_silver_df.withColumn("data_date", lit(data_date))
        
        # return {
            # "jobs_normalized": df_normalized,
            # "jobs_silver": jobs_silver_df,
            # "job_skills_silver": job_skills_silver_df,
            # "partitions": inputs["partitions"]
        # }
    
    # def compute_metrics(self, outputs: dict) -> dict:
        
        # df_normalized = outputs["jobs_normalized"]
        # quality_metrics = (
            # df_normalized
            # .agg(
                # count("*").alias("total"),
                # sum(when(col("job_title_raw").isNull(), 1).otherwise(0)).alias("null_titles"),
                # sum(when(col("description_raw").isNull(), 1).otherwise(0)).alias("null_descriptions"),
            # )
            # .first()
        # )
        # return {
                # "total": quality_metrics["total"], 
                # "null_titles": quality_metrics["null_titles"], 
                # "null_descriptions": quality_metrics["null_descriptions"]
                # }
    
    # def write(self, inputs: dict, outputs: dict) -> None:
        # self.spark.conf.set(
            # "spark.sql.sources.partitionOverWriteMode",
            # "dynamic"
        # )
        
        # for name, mode, dataset in [
            # ("jobs_silver", "overwrite", self.datasets.silver_jobs),
            # ("job_skills_silver", "overwrite", self.datasets.silver_job_skills)
        # ]:
            
            # dataset.write(df = outputs[name], mode=mode)
            
            # partitions = inputs["partitions"]
            
            # self.partition_manager.mark_processed(
                # stage_name=self.STAGE_NAME,
                # partitions=partitions
            # )

#####################

# class SilverStage(BaseStage):
    
    # def __init__(
        # self, 
        # silver_ctx: SilverContext, 
        # bronze_ctx: BronzeContext,
        # storage: Storage):
        # super().__init__(spark=silver_ctx.spark, storage=storage)
        # self.silver_ctx = silver_ctx
        # self.bronze_ctx = bronze_ctx
        
    # def validate_inputs(self) -> None:
        
        # bronze_root = self.bronze_ctx.bronze_root
        # if not bronze_root.exists():
            # raise FileNotFoundError(
                # f"Missing input dataset directory: {bronze_root}"
            # )
    
    # def read(self) -> dict:
        # bronze_root = str(self.bronze_ctx.bronze_root)
        
        # df = self.spark.read.json(bronze_root)
        
        # date_range = self.silver_ctx.date_range
        
        # # Read all data
        # if date_range.is_full_load():
            # return {"job_bronze_df": df}
        
        # # Read a date range
        # if date_range.start_date and date_range.end_date:
            # df = df.where(
                # col("ingestion_date").between(
                    # str(date_range.start_date),
                    # str(date_range.end_date)
                # )
            # )
        # elif date_range.start_date:
            # df = df.where(
                # col("ingestion_date") == str(date_range.start_date)
            # )
        # else:
            # raise ValueError("Invalid DateRange configuration.")
        
        # return {"job_bronze_df": df}
    
    # def create_context(self) -> StageExecutionContext:
        # run_context = StageExecutionContext(
            # stage="silver",
            # pipeline_version="1.0.0"
        # )
        # return run_context
    
    # def transform(
        # self, 
        # inputs: dict
        # ) -> dict:
        
        # job_bronze_df = inputs["job_bronze_df"]
        # df_normalized = job_bronze_df.select(
            # col("run_id"),
            # col("ingestion_date"),
            # col("payload.*"), 
            # col("ingestion_metadata.started_at").alias("ingested_at")
        # )
        
        # self.logger.info("building_df_clean")
        # df_clean = clean_jobs(df=df_normalized)
        
        # self.logger.info("building_jobs_silver")
        # jobs_silver_df = deduplicate_jobs(df_clean)
        
        # self.logger.info("building_job_skills_silver")
        # job_skills_silver_df = run_job_skills(jobs_silver_df = jobs_silver_df)
        
        # # data_date = self.silver_ctx.data_date
        # # jobs_silver_df = jobs_silver_df.withColumn("data_date", lit(data_date))
        # # job_skills_silver_df = job_skills_silver_df.withColumn("data_date", lit(data_date))
        
        # return {
            # "jobs_normalized": df_normalized,
            # "jobs_silver": jobs_silver_df,
            # "job_skills_silver": job_skills_silver_df
        # }
    
    # def compute_metrics(self, outputs: dict) -> dict:
        
        # df_normalized = outputs["jobs_normalized"]
        # quality_metrics = (
            # df_normalized
            # .agg(
                # count("*").alias("total"),
                # sum(when(col("job_title_raw").isNull(), 1).otherwise(0)).alias("null_titles"),
                # sum(when(col("description_raw").isNull(), 1).otherwise(0)).alias("null_descriptions"),
            # )
            # .first()
        # )
        # return {
                # "total": quality_metrics["total"], 
                # "null_titles": quality_metrics["null_titles"], 
                # "null_descriptions": quality_metrics["null_descriptions"]
                # }
    
    # def write(self, outputs: dict) -> None:
        
        # self.spark.conf.set(
            # "spark.sql.sources.partitionOverWriteMode",
            # "dynamic"
        # )
        
        # for name, mode, path in [
            # ("jobs_silver", "overwrite", self.silver_ctx.jobs_path),
            # ("job_skills_silver", "overwrite", self.silver_ctx.job_skills_path)
        # ]:
            
            # self.storage.write_dataframe(
                # df=outputs[name],
                # path=path,
                # mode=mode,
                # partition_cols=["ingestion_date"]
                # )




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
