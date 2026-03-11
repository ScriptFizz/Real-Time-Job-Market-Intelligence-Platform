import typer
import logging
from dotenv import load_dotenv
from datetime import datetime
from job_plat.config.config_loader import ConfigLoader
from job_plat.ingestion.connectors import build_connectors
from job_plat.context.context_builders import build_bronze_context, build_data_pipeline_context, build_ml_pipeline_context
from job_plat.orchestration.data_pipeline import run_bronze_pipeline, run_silver_pipeline, run_gold_pipeline, run_data_pipeline
from job_plat.orchestration.ml_pipeline import run_feature_pipeline, run_ml_pipeline, run_full_ml_pipeline
from job_plat.context.contexts import ExecutionParams
from job_plat.config.logconfig import setup_logging
from job_plat.storage.storages import get_storage
from job_plat.utils.helpers import create_spark, parse_date
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.pipeline.datasets.dataset_definitions import DATASET_DEFS

load_dotenv()

app = typer.Typer(help="Job postings data pipeline CLI")

@app.command()
def bronze(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    query: str = typer.Option(None, help="Override search query"),
    country: str = typer.Option(None, help="Override country"),
    location: str = typer.Option(None, help="Override location"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
):
    """
    Run bronze ingestion stage.
    """
    
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    execution = ExecutionParams(
        query=query,
        country=country,
        location=location
    )
    
    spark = create_spark(env_config.spark)
    
    try:
        
        bronze_ctx = build_bronze_context(
            config = env_config,
            execution = execution
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
):
    """
    Run silver stage.
    """
    
    execution = ExecutionParams(query=None, location=None)
    
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    spark = create_spark(env_config.spark)
    
    try:
    
        pipeline_ctx = build_data_pipeline_context(
            execution=execution, 
            config=env_config,
            spark=spark
            )
            
        storage = get_storage(env_config.storage.type)
        
        datasets = DatasetRegistry(
            root=env_config.paths.root,
            storage=storage,
            dataset_defs=DATASET_DEFS
        )
        
        partition_manager = PartitionManager(metadata_path=env_config.paths.metadata)
        
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
):
    """
    Run gold stage.
    """
    
    execution = ExecutionParams(query=None, location=None)
    
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    spark = create_spark(env_config.spark)
    
    try:
    
        pipeline_ctx = build_data_pipeline_context(
            execution=execution, 
            config=env_config,
            spark=spark
            )
            
        storage = get_storage(env_config.storage.type)
        
        datasets = DatasetRegistry(
            root=env_config.paths.root,
            storage=storage,
            dataset_defs=DATASET_DEFS
        )
        
        partition_manager = PartitionManager(metadata_path=env_config.paths.metadata)
        
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
):
    
    """
    Run the full pipeline (bronze → silver → gold).
    """
    
    execution = ExecutionParams(query=None, location=None)
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    execution = ExecutionParams(
        query=query,
        country=country,
        location=location
    )
    
    spark = create_spark(env_config.spark)

    try: 
        pipeline_ctx = build_data_pipeline_context(
            execution=execution, 
            config=env_config,
            spark=spark
            )
        storage = get_storage(env_config.storage.type)
        
        connectors = build_connectors(env_config)
        
        datasets = DatasetRegistry(
            root=env_config.paths.root,
            storage=storage,
            dataset_defs=DATASET_DEFS
        )
        
        partition_manager = PartitionManager(metadata_path=env_config.paths.metadata)
        
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
):
    """
    Run feature stage.
    """
    
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    spark = create_spark(env_config.spark)
    
    try:
    
        pipeline_ctx = build_ml_pipeline_context(
            config=env_config,
            spark=spark,
        )
            
        
        storage = get_storage(env_config.storage.type)
        datasets = DatasetRegistry(
            root=env_config.paths.root,
            storage=storage,
            dataset_defs=DATASET_DEFS
        )
        
        partition_manager = PartitionManager(metadata_path=env_config.paths.metadata)
        
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
):
    """
    Run ml stage.
    """
    
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    spark = create_spark(env_config.spark)
    
    try:
    
        pipeline_ctx = build_ml_pipeline_context(
            config=env_config,
            spark=spark,
        )
        
        storage = get_storage(env_config.storage.type)
        datasets = DatasetRegistry(
            root=env_config.paths.root,
            storage=storage,
            dataset_defs=DATASET_DEFS
        )
        
        partition_manager = PartitionManager(metadata_path=env_config.paths.metadata)
        
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
):
    """
    Run full ml pipeline.
    """
    
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    spark = create_spark(env_config.spark)
    
    try:
    
        pipeline_ctx = build_ml_pipeline_context(
            config=env_config,
            spark=spark,
        )
        
        storage = get_storage(env_config.storage.type)
        datasets = DatasetRegistry(
            root=env_config.paths.root,
            storage=storage,
            dataset_defs=DATASET_DEFS
        )
        
        partition_manager = PartitionManager(metadata_path=env_config.paths.metadata)
        
        run_full_ml_pipeline(
            ctx=pipeline_ctx,
            datasets=datasets,
            partition_manager=partition_manager
        ) 
    
    finally:
        spark.stop()




if __name__ == "__main__":
    app()
    
