# from job_plat.utils.io import load_params
# from job_plat.utils.helpers import create_spark
from job_plat.context.contexts import (
    ExecutionParams,
    BronzeContext, 
    SilverContext, 
    GoldV1Context, 
    GoldV2Context,
    PipelineContext
    )
from datetime import date
from pyspark.sql import SparkSession
from typing import Dict
from pathlib import Path
from job_plat.config.env_config import EnvironmentConfig


def build_bronze_context(
    config: EnvironmentConfig,
    execution: ExecutionParams,
) -> BronzeContext:
    
    final_query = execution.query or config.bronze.query
    final_location = execution.location or config.bronze.location
    
    missing = []
    if not final_query:
        missing.append("Query must not be empty")
    if not final_location:
        missing.append("Location must not be empty")
    if missing:
        raise ValueError(", ".join(missing))
    
    return BronzeContext(
        query = final_query,
        location = final_location
    )


def build_pipeline_context(
    execution: ExecutionParams,
    config: EnvironmentConfig,
    spark: SparkSession,
) -> PipelineContext:
    
    final_query = execution.query or config.bronze.query
    final_location = execution.location or config.bronze.location

    
    bronze_ctx = BronzeContext(
        query = final_query,
        location = final_location
    )
    
    silver_ctx = SilverContext(
        base_path = config.paths.silver,
        spark = spark
    )
    
    gold_v1_ctx = GoldV1Context(
        fact_per_job_ratio_threshold = config.gold_v1.fact_per_job_ratio_threshold,
        spark = spark
    )
    
    gold_v2_ctx = GoldV2Context(
        min_clusters = config.gold_v2.min_clusters,
        min_silhouette = config.gold_v2.min_silhouette,
        spark = spark
    )
    
    return PipelineContext(
        env = config.env,
        spark = spark,
        bronze = bronze_ctx,
        silver = silver_ctx,
        gold_v1 = gold_v1_ctx,
        gold_v2 = gold_v2_ctx
    )
    

################################### 06-03


# def build_bronze_context(
    # config: EnvironmentConfig,
    # execution: ExecutionParams,
# ) -> BronzeContext:
    
    # final_query = execution.query or config.bronze.query
    # final_location = execution.location or config.bronze.location
    
    # missing = []
    # if not final_query:
        # missing.append("Query must not be empty")
    # if not final_location:
        # missing.append("Location must not be empty")
    # if missing:
        # raise ValueError(", ".join(missing))
    
    # return BronzeContext(
        # base_path = Path(config.paths.bronze),
        # query = final_query,
        # location = final_location
    # )


# def build_pipeline_context(
    # execution: ExecutionParams,
    # config: EnvironmentConfig,
    # spark: SparkSession,
# ) -> PipelineContext:
    
    # final_query = execution.query or config.bronze.query
    # final_location = execution.location or config.bronze.location

    
    # bronze_ctx = BronzeContext(
        # base_path = Path(config.paths.bronze),
        # query = final_query,
        # location = final_location
    # )
    
    # silver_ctx = SilverContext(
        # date_range = execution.date_range,
        # base_path = config.paths.silver,
        # spark = spark
    # )
    
    # gold_v1_ctx = GoldV1Context(
        # date_range = execution.date_range,
        # base_path = config.paths.gold_v1,
        # fact_per_job_ratio_threshold = config.gold_v1.fact_per_job_ratio_threshold,
        # spark = spark
    # )
    
    # gold_v2_ctx = GoldV2Context(
        # date_range = execution.date_range,
        # base_path = config.paths.gold_v2,
        # min_clusters = config.gold_v2.min_clusters,
        # min_silhouette = config.gold_v2.min_silhouette,
        # spark = spark
    # )
    
    # return PipelineContext(
        # #date_range = execution.date_range,
        # env = config.env,
        # spark = spark,
        # bronze = bronze_ctx,
        # silver = silver_ctx,
        # gold_v1 = gold_v1_ctx,
        # gold_v2 = gold_v2_ctx
        
    # )

    
##################################à
# def build_bronze_context(
    # pipeline_ctx: PipelineContext,
    # query: str | None,
    # location: str | None,
# ) -> BronzeContext:
    
    # final_query = query or pipeline_ctx.bronze.query
    # final_location = location or pipeline_ctx.bronze.location
    
    # missing = []
    # if not final_query:
        # missing.append("Query must not be empty")
    # if not final_location:
        # missing.append("Location must not be empty")
    # if missing:
        # raise ValueError(", ".join(missing))
    
    # return BronzeContext(
        # data_date = pipeline_ctx.data_date,
        # base_path = pipeline_ctx.bronze.base_path,
        # query = final_query,
        # location = final_location
    # )


# def build_pipeline_context(
    # data_range: DateRange,
    # config: EnvironmentConfig,
    # spark: SparkSession
# ) -> PipelineContext:

    
    # bronze_ctx = BronzeContext(
        # #data_date = data_date,
        # base_path = Path(config.paths.bronze),
        # query = config.bronze.query,
        # location = config.bronze.location
    # )
    
    # silver_ctx = SilverContext(
        # date_range = date_range,
        # base_path = config.paths.silver,
        # spark = spark
    # )
    
    # gold_v1_ctx = GoldV1Context(
        # date_range = date_range,
        # base_path = config.paths.gold_v1,
        # fact_per_job_ratio_threshold = config.gold_v1.fact_per_job_ratio_threshold,
        # spark = spark
    # )
    
    # gold_v2_ctx = GoldV2Context(
        # date_range = date_range,
        # base_path = config.paths.gold_v2,
        # min_clusters = config.gold_v2.min_clusters,
        # min_silhouette = config.gold_v2.min_silhouette,
        # spark = spark
    # )
    
    # return PipelineContext(
        # date_range = date_range,
        # env = config.env,
        # spark = spark,
        # bronze = bronze_ctx,
        # silver = silver_ctx,
        # gold_v1 = gold_v1_ctx,
        # gold_v2 = gold_v2_ctx
        
    # )
