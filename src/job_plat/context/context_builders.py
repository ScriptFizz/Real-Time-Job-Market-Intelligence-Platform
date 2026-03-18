from job_plat.context.contexts import (
    ExecutionParams,
    BronzeContext, 
    SilverContext, 
    GoldContext, 
    DataPipelineContext,
    FeatureContext,
    MLContext,
    MLPipelineContext
    )
from datetime import date, datetime
from pyspark.sql import SparkSession
from typing import Dict
from pathlib import Path
from job_plat.config.env_config import EnvironmentConfig


def build_bronze_context(
    config: EnvironmentConfig,
    execution: ExecutionParams,
    execution_date: datetime
) -> BronzeContext:
    
    final_query = execution.query or config.bronze.query
    final_country = execution.country or config.bronze.country
    final_location = execution.location or config.bronze.location
    root_path = config.paths.root
    
    missing = []
    if not final_query:
        missing.append("Query must not be empty")
    if not final_country:
        missing.append("Country must not be empty")
    if not final_location:
        missing.append("Location must not be empty")
    if missing:
        raise ValueError(", ".join(missing))
    
    return BronzeContext(
        root_path = root_path,
        query = final_query,
        country=final_country,
        location = final_location,
        execution_date=execution_date
    )


def build_data_pipeline_context(
    execution: ExecutionParams,
    config: EnvironmentConfig,
    spark: SparkSession,
    execution_date: datetime
) -> DataPipelineContext:
    
    final_query = execution.query or config.bronze.query
    final_country = execution.country or config.bronze.country
    final_location = execution.location or config.bronze.location
    root_path = config.paths.root
    
    bronze_ctx = BronzeContext(
        root_path = root_path,
        query = final_query,
        country=final_country,
        location = final_location,
        execution_date=execution_date
    )
    
    silver_ctx = SilverContext(
        spark = spark,
        execution_date=execution_date
    )
    
    gold_ctx = GoldContext(
        fact_per_job_ratio_threshold = config.gold.fact_per_job_ratio_threshold,
        spark = spark,
        execution_date=execution_date
    )
    
    
    return DataPipelineContext(
        env = config.env,
        spark = spark,
        bronze = bronze_ctx,
        silver = silver_ctx,
        gold = gold_ctx,
        execution_date=execution_date
    )


def build_ml_pipeline_context(
    config: EnvironmentConfig,
    spark: SparkSession,
    execution_date: datetime
) -> MLPipelineContext:
    

    
    feature_ctx = FeatureContext(
        spark = spark,
        window_days = config.ml.window_days,
        execution_date=execution_date
    )
    
    ml_ctx = MLContext(
        min_clusters = config.ml.min_clusters,
        min_silhouette = config.ml.min_silhouette,
        spark = spark,
        execution_date=execution_date
    )
    
    return MLPipelineContext(
        env = config.env,
        spark = spark,
        feature = feature_ctx,
        ml = ml_ctx,
        execution_date=execution_date
    )


