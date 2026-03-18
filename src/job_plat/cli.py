import typer
import logging
from typing import Tuple
from dotenv import load_dotenv
from datetime import datetime
from pyspark.sql import SparkSession
from job_plat.config.config_loader import ConfigLoader
from job_plat.ingestion.connectors import build_connectors
from job_plat.config.env_config import EnvironmentConfig
from job_plat.context.context_builders import build_bronze_context, build_data_pipeline_context, build_ml_pipeline_context
from job_plat.orchestration.data_pipeline import run_bronze_pipeline, run_silver_pipeline, run_gold_pipeline, run_data_pipeline
from job_plat.orchestration.ml_pipeline import run_feature_pipeline, run_ml_pipeline, run_full_ml_pipeline
from job_plat.context.contexts import ExecutionParams
from job_plat.config.logconfig import setup_logging
from job_plat.storage.storages import get_storage, Storage
from job_plat.utils.helpers import create_spark, parse_date
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.pipeline.datasets.dataset_definitions import DATASET_DEFS

load_dotenv()

app = typer.Typer(help="Job postings data pipeline CLI")


def setup_run(config: str, env: str, execution_date: str | None, query: str | None = None, country: str | None = None, location: str | None = None) -> Tuple[EnvironmentConfig, ExecutionParams, datetime, SparkSession]:
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    execution = ExecutionParams(
        query=query,
        country=country,
        location=location
    )
    
    execution_date = (
        datetime.fromisoformat(execution_date)
        if execution_date
        else datetime.utcnow()
    )
        
    spark = create_spark(env_config.spark)
    return env_config, execution, execution_date, spark

def build_common(env_config: EnvironmentConfig, execution: ExecutionParams, spark: SparkSession, execution_date: datetime) -> Tuple[Storage, DatasetRegistry, PartitionManager]:
            
    storage = get_storage(env_config.storage.type)
    
    datasets = DatasetRegistry(
        root=env_config.paths.root,
        storage=storage,
        dataset_defs=DATASET_DEFS
    )
    
    partition_manager = PartitionManager(metadata_path=env_config.paths.metadata)
    
    return storage, datasets, partition_manager

@app.command()
def bronze(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    query: str = typer.Option(None, help="Override search query"),
    country: str = typer.Option(None, help="Override country"),
    location: str = typer.Option(None, help="Override location"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
    execution_date: str = typer.Option(None, help="Execution date ISO format"),
):
    """
    Run bronze ingestion stage.
    """
    
    env_config, execution, execution_date, spark = setup_run(config=config, env=env, execution_date=execution_date, query=query, country=country, location=location)
    
    try:
        
        bronze_ctx = build_bronze_context(
            config = env_config,
            execution = execution,
            execution_date=execution_date
        )
        
        storage = get_storage(env_config.storage.type)
        
        connectors = build_connectors(env_config)
        
        partition_manager = PartitionManager(metadata_path=env_config.paths.metadata)
        
        for connector in connectors:
            run_bronze_pipeline(
                ctx=bronze_ctx,
                storage=storage,
                connector=connector
            )
    
    finally:
        spark.stop()


@app.command()
def silver(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
    execution_date: str = typer.Option(None, help="Execution date ISO format"),
):
    """
    Run silver stage.
    """
    
    env_config, execution, execution_date, spark = setup_run(config=config, env=env, execution_date=execution_date)
    
    try:
    
        pipeline_ctx = build_data_pipeline_context(
            execution=execution, 
            config=env_config,
            spark=spark,
            execution_date=execution_date,
            )
        
        storage, datasets, partition_manager = build_common(env_config=env_config, execution=execution, spark=spark, execution_date=execution_date)
        
        run_silver_pipeline(
            ctx=pipeline_ctx,
            datasets=datasets,
            partition_manager=partition_manager
        )
    
    finally:
        spark.stop()



@app.command()
def gold(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
    execution_date: str = typer.Option(None, help="Execution date ISO format"),
):
    """
    Run gold stage.
    """
    
    env_config, execution, execution_date, spark = setup_run(config=config, env=env, execution_date=execution_date)
    
    try:
    
        pipeline_ctx = build_data_pipeline_context(
            execution=execution, 
            config=env_config,
            spark=spark,
            execution_date=execution_date
            )
        
        storage, datasets, partition_manager = build_common(env_config=env_config, execution=execution, spark=spark, execution_date=execution_date)

        run_gold_pipeline(
            ctx=pipeline_ctx,
            datasets=datasets,
            partition_manager=partition_manager
        )
    
    finally:
        spark.stop()



@app.command()
def data_pipeline(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    query: str = typer.Option(None, help="Override search query"),
    country: str = typer.Option(None, help="Override country"),
    location: str = typer.Option(None, help="Override location"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
    execution_date: str = typer.Option(None, help="Execution date ISO format"),
):
    
    """
    Run the full pipeline (bronze → silver → gold).
    """
    
    env_config, execution, execution_date, spark = setup_run(config=config, env=env, execution_date=execution_date, query=query, country=country, location=location)

    try: 
        pipeline_ctx = build_data_pipeline_context(
            execution=execution, 
            config=env_config,
            spark=spark,
            execution_date=execution_date
            )
        
        connectors = build_connectors(env_config)
        
        storage, datasets, partition_manager = build_common(env_config=env_config, execution=execution, spark=spark, execution_date=execution_date)
        
        run_data_pipeline(
            ctx=pipeline_ctx,
            storage=storage,
            datasets=datasets,
            partition_manager=partition_manager,
            connectors=connectors,
        )
    
    finally:
        spark.stop()


###################
#   ML PIPELINES
###################

# FEATURE

@app.command()
def feature(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
    execution_date: str = typer.Option(None, help="Execution date ISO format"),
):
    """
    Run feature stage.
    """
    
    env_config, execution, execution_date, spark = setup_run(config=config, env=env, execution_date=execution_date)
    
    try:
    
        pipeline_ctx = build_ml_pipeline_context(
            config=env_config,
            spark=spark,
            execution_date=execution_date
        )
            
        
        storage, datasets, partition_manager = build_common(env_config=env_config, execution=execution, spark=spark, execution_date=execution_date)
        
        run_feature_pipeline(
            ctx=pipeline_ctx,
            datasets=datasets,
            partition_manager=partition_manager
        ) 
    
    finally:
        spark.stop()


# ML

@app.command()
def ml(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
    execution_date: str = typer.Option(None, help="Execution date ISO format"),
):
    """
    Run ml stage.
    """
    
    env_config, execution, execution_date, spark = setup_run(config=config, env=env, execution_date=execution_date)
    
    try:
    
        pipeline_ctx = build_ml_pipeline_context(
            config=env_config,
            spark=spark,
            execution_date=execution_date
        )
        
        storage, datasets, partition_manager = build_common(env_config=env_config, execution=execution, spark=spark, execution_date=execution_date)
        
        run_ml_pipeline(
            ctx=pipeline_ctx,
            datasets=datasets,
            partition_manager=partition_manager
        ) 
    
    finally:
        spark.stop()


# FULL ML

@app.command()
def ml_pipeline(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
    execution_date: str = typer.Option(None, help="Execution date ISO format"),
):
    """
    Run full ml pipeline.
    """
    
    env_config, execution, execution_date, spark = setup_run(config=config, env=env, execution_date=execution_date)
    
    try:
    
        pipeline_ctx = build_ml_pipeline_context(
            config=env_config,
            spark=spark,
            execution_date=execution_date
        )
        
        storage, datasets, partition_manager = build_common(env_config=env_config, execution=execution, spark=spark, execution_date=execution_date)
        
        run_full_ml_pipeline(
            ctx=pipeline_ctx,
            datasets=datasets,
            partition_manager=partition_manager
        ) 
    
    finally:
        spark.stop()




if __name__ == "__main__":
    app()
    
