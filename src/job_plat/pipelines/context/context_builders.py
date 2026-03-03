from job_plat.utils.io import load_params
from job_plat.utils.helpers import create_spark
from job_plat.pipelines.context.contexts import (
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

def build_pipeline_context(
    data_date: date,
    config: EnvironmentConfig,
    spark: SparkSession
) -> PipelineContext:

    
    bronze_ctx = BronzeContext(
        data_date = data_date,
        base_path = Path(config.paths.bronze),
        query = config.bronze.query,
        location = config.bronze.location
    )
    
    silver_ctx = SilverContext(
        data_date = data_date,
        base_path = config.paths.silver,
        spark = spark
    )
    
    gold_v1_ctx = GoldV1Context(
        data_date = data_date,
        base_path = config.paths.gold_v1,
        fact_per_job_ratio_threshold = config.gold_v1.fact_per_job_ratio_threshold,
        spark = spark
    )
    
    gold_v2_ctx = GoldV2Context(
        data_date = data_date,
        base_path = config.paths.gold_v2,
        min_clusters = config.gold_v2.min_clusters,
        min_silhouette = config.gold_v2.min_silhouette,
        spark = spark
    )
    
    return PipelineContext(
        data_date = data_date,
        env = config.env,
        spark = spark,
        bronze = bronze_ctx,
        silver = silver_ctx,
        gold_v1 = gold_v1_ctx,
        gold_v2 = gold_v2_ctx
        
    )
    
    
def build_bronze_context(
    pipeline_ctx: PipelineContext,
    query: str | None,
    location: str | None,
) -> BronzeContext:
    
    final_query = query or pipeline_ctx.bronze.query
    final_location = location or pipeline_ctx.bronze.location
    
    missing = []
    if not final_query:
        missing.append("Query must not be empty")
    if not final_location:
        missing.append("Location must not be empty")
    if missing:
        raise ValueError(", ".join(missing))
    
    return BronzeContext(
        data_date = pipeline_ctx.data_date,
        base_path = pipeline_ctx.bronze.base_path,
        query = final_query,
        location = final_location
    )
    
    
