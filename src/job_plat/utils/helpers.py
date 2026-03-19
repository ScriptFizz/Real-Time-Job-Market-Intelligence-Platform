from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from urllib.parse import urlencode
from job_plat.config.env_config import SparkConfig
from datetime import datetime

from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.pipeline.datasets.dataset_definitions import DATASET_DEFS
from job_plat.config.config_loader import ConfigLoader
from job_plat.ingestion.connectors import build_connectors
from job_plat.context.contexts import ExecutionParams
from job_plat.config.logconfig import setup_logging
from job_plat.storage.storages import get_storage
#from job_plat.utils.helpers import create_spark, parse_date
from job_plat.context.context_builders import build_bronze_context, build_data_pipeline_context, build_ml_pipeline_context


class StageSkip(Exception):
    pass

def parse_date(d: str | None):
    return datetime.strptime(d, "%Y-%m-%d").date() if d else None


def create_spark(
    spark_config: SparkConfig
) -> SparkSession:
    """
    Create a SparkSession with a specific application name and master URL.
    
    Args:
        
        
    Returns:
        (SparkSession): Entry point to programming Spark.
    """
    
    builder = (
        SparkSession.builder
        .appName(spark_config.app_name)
        .master(spark_config.master)
    )
    
    for key, value in spark_config.config.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()


def union_all(dfs: list[DataFrame]) -> DataFrame:
    """
    Union a list of Spark DataFrames by column name.
    Assumes schemas are aligned.
    
    Args:
        dfs (list[DataFrame]): List of schema-aligned Spark DataFrame to join.
    
    Returns:
        DataFrame: Spark DataFrame of the combined dataframes list.
    """
    
    if not dfs:
        raise ValuerError("No DataFrames to union")
    
    return reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
        dfs,
    )

def path_exists(spark: SparkSession, path: str | Path) -> bool:
    
    try:
        spark.read.parquet(path).limit(1).collect()
        return True
    except Exception:
        return False



def assert_df_equality(df1, df2):
    assert sorted(df1.collect()) == sorted(df2.collect())



# def build_common(env: str = "dev", config_path: str = "settings.yaml"):
        
    # config_loader = ConfigLoader(config_path=config_path, env=env)
    # env_config = config_loader.load_env()
    
    # log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    # setup_logging(log_level=log_level)
    
    # spark = create_spark(env_config.spark)
    # storage = get_storage(env_config.storage.type)
    
    # datasets = DatasetRegistry(
        # root = env_config.paths.root,
        # storage = storage,
        # dataset_defs = DATASET_DEFS
    # )
    
    # partition_manager = PartitionManager(
        # metadata_path = env_config.paths.metadata
    # )
    
    # connectors = build_connectors(env_config)
    
    # return env_config, spark, storage, datasets, partition_manager, connectors
