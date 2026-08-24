from datetime import datetime

from pyspark.sql import SparkSession

from job_plat.config.env_config import EnvironmentConfig
from job_plat.context.contexts import (
    BronzeContext,
    DataPipelineContext,
    ExecutionParams,
    FeatureContext,
    GoldContext,
    MLContext,
    MLPipelineContext,
    SilverContext,
)
from job_plat.storage.paths import join_storage_path


def build_bronze_context(
    config: EnvironmentConfig, execution: ExecutionParams, execution_date: datetime
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
        root_path=root_path,
        query=final_query,
        country=final_country,
        location=final_location,
        execution_date=execution_date,
    )


def build_data_pipeline_context(
    execution: ExecutionParams,
    config: EnvironmentConfig,
    spark: SparkSession,
    execution_date: datetime,
) -> DataPipelineContext:
    final_query = execution.query or config.bronze.query
    final_country = execution.country or config.bronze.country
    final_location = execution.location or config.bronze.location
    root_path = config.paths.root

    bronze_ctx = BronzeContext(
        root_path=root_path,
        query=final_query,
        country=final_country,
        location=final_location,
        execution_date=execution_date,
    )

    silver_ctx = SilverContext(spark=spark, execution_date=execution_date)

    gold_ctx = GoldContext(
        fact_per_job_ratio_threshold=config.gold.fact_per_job_ratio_threshold,
        spark=spark,
        execution_date=execution_date,
    )

    return DataPipelineContext(
        env=config.env,
        spark=spark,
        bronze=bronze_ctx,
        silver=silver_ctx,
        gold=gold_ctx,
        execution_date=execution_date,
    )


def build_ml_pipeline_context(
    config: EnvironmentConfig, spark: SparkSession, execution_date: datetime
) -> MLPipelineContext:
    feature_ctx = FeatureContext(
        spark=spark,
        window_days=config.ml.window_days,
        execution_date=execution_date,
        embedding_model_name=config.ml.embedding_model_name,
        embedding_model_version=config.ml.embedding_model_version,
        embedding_model_provider=config.ml.embedding_model_provider,
        embedding_batch_size=config.ml.embedding_batch_size,
        max_driver_skills=config.ml.max_driver_skills,
        job_embedding_aggregation=config.ml.job_embedding_aggregation,
    )

    artifact_root = config.paths.artifacts or join_storage_path(
        config.paths.root,
        "artifacts",
    )

    ml_ctx = MLContext(
        min_clusters=config.ml.min_clusters,
        min_silhouette=config.ml.min_silhouette,
        spark=spark,
        execution_date=execution_date,
        artifact_root=join_storage_path(artifact_root, "models", "job_clustering"),
        model_version=config.ml.clustering_model_version,
        k_values=tuple(config.ml.clustering_k_values),
        seed=config.ml.clustering_seed,
        promote_model=config.ml.promote_model,
    )

    return MLPipelineContext(
        env=config.env,
        spark=spark,
        feature=feature_ctx,
        ml=ml_ctx,
        execution_date=execution_date,
    )
