from datetime import datetime
from typing import Tuple
import logging

from pyspark.sql import SparkSession

from job_plat.config.config_loader import ConfigLoader
from job_plat.config.env_config import EnvironmentConfig
from job_plat.context.contexts import ExecutionParams
from job_plat.utils.helpers import create_spark
from job_plat.storage.storages import get_storage, Storage
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.config.logconfig import setup_logging


def build_runtime(
    config_path: str,
    env: str,
    execution_date: str | None,
    query: str | None = None,
    country: str | None = None,
    location: str | None = None,
) -> Tuple[
    EnvironmentConfig,
    ExecutionParams,
    datetime,
    SparkSession,
    DatasetRegistry,
    PartitionManager,
]:
    
    # -------- Load config -----------
    config_loader = ConfigLoader(config_path=config_path, env=env)
    env_config = config_loader.load_env()
    
    # -------- Logging --------------
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    # ---------- Execution params ---------
    execution = ExecutionParams(
        query=query,
        country=country,
        location=location,
    )
    
    execution_dt = (
        datetime.fromisoformat(execution_date)
        if execution_date
        else datetime.utcnow()
    )

    # -------- Spark ----------
    spark = create_spark(env_config.spark)
    
    # -------- Shared infra ----------
    storage = get_storage(env_config.storage.type)
    
    datasets = DatasetRegistry(
        root=env_config.paths.root,
        storage=storage,
        dataset_defs=DATASET_DEFS,
    )
    
    partition_manager = PartitionManager(
        metadata_path=env_config.paths.metadata
    )
    
    return (
        env_config,
        execution,
        execution_dt,
        spark,
        datasets,
        partition_manager,
    )
