import typer
import logging
from dotenv import load_dotenv
from datetime import datetime
from job_plat.config.config_loader import ConfigLoader
from job_plat.bronze.ingestion.connectors import build_connectors
from job_plat.pipelines.context.context_builders import build_pipeline_context, build_bronze_context
from job_plat.pipelines.pipeline_stages import run_bronze_pipeline, run_silver_pipeline, run_full_pipeline
from job_plat.pipelines.context.contexts import DateRange, ExecutionParams
from job_plat.config.logconfig import setup_logging
from job_plat.utils.storage import get_storage
from job_plat.utils.helpers import create_spark, parse_date

load_dotenv()

app = typer.Typer(help="Job postings data pipeline CLI")

@app.command()
def bronze(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    query: str = typer.Option(None, help="Override search query"),
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
        location=location
    )
    
    spark = create_spark(env_config.spark)
        
    bronze_ctx = build_bronze_context(
        config = env_config,
        execution = execution
    )
    
    storage = get_storage(env_config.storage.type)
    
    connectors = build_connectors(env_config)
    
    for connector in connectors:
        run_bronze_pipeline(
            ctx=bronze_ctx,
            storage=storage,
            connector=connector
        )


@app.command()
def silver(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    start_date: str = typer.Option(None, help="Start date YYYY-MM-DD"),
    end_date: str = typer.Option(None, help="End date YYYY-MM-DD"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
):
    """
    Run silver stage.
    """
    
    start = parse_date(d=start_date)
    end = parse_date(d=end_date)
    
    date_range = DateRange(start_date = start, end_date = end)
    
    execution = ExecutionParams(date_range=date_range)
    
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    spark = create_spark(env_config.spark)
    
    pipeline_ctx = build_pipeline_context(
        date_range = date_range, 
        config=env_config,
        spark=spark
        )
        
    storage = get_storage(env_config.storage.type)
    
    run_silver_pipeline(
        ctx=pipeline_ctx,
        storage=storage,
    )
    

@app.command()
def gold_v1(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    date: str = typer.Option(None, help="Data date (YYYY-MM-DD)"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
):
    """
    Run gold_v1 stage.
    """
    
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    data_date = (
        datetime.strptime(date, "%Y-%m-%d").date()
        if date
        else datetime.utcnow().date()
    )
    
    spark = create_spark(env_config.spark)
    
    pipeline_ctx = build_pipeline_context(
        data_date=data_date, 
        config=env_config,
        spark=spark
        )
        
    
    storage = get_storage(env_config.storage.type)
    
    run_gold_v1_pipeline(
        ctx=pipeline_ctx,
        storage=storage,
    )


@app.command()
def gold_v2(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    date: str = typer.Option(None, help="Data date (YYYY-MM-DD)"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
):
    """
    Run gold_v2 stage.
    """
    
    config_loader = ConfigLoader(config_path=config, env=env)
    env_config = config_loader.load_env()
    
    log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    setup_logging(log_level=log_level)
    
    data_date = (
        datetime.strptime(date, "%Y-%m-%d").date()
        if date
        else datetime.utcnow().date()
    )
    
    spark = create_spark(env_config.spark)
    
    pipeline_ctx = build_pipeline_context(
        data_date=data_date, 
        config=env_config,
        spark=spark
        )
        
    
    storage = get_storage(env_config.storage.type)
    
    run_gold_v2_pipeline(
        ctx=pipeline_ctx,
        storage=storage,
    )


@app.command()
def full(
    date: str = typer.Option(None, help="Data date (YYYY-MM-DD)"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
):
    
    """
    Run the full pipeline (bronze → silver → gold).
    """
    
    setup_logging()
    
    config_dict = load_params(config)
    
    data_date = (
        datetime.strptime(date, "%Y-%m-%d").date()
        if date
        else datetime.utcnow().date()
    )
    
    ctx = build_pipeline_context(data_date=data_date, config=config_dict)
    
    storage = get_storage(config_dict["storage"]["type"])
    
    run_full_pipeline(ctx=ctx, storage=storage)

if __name__ == "__main__":
    app()
    



# @app.command()
# def bronze(
    # env: str = typer.Option("dev", help="Environment (dev or prod)"),
    # query: str = typer.Option(None, help="Override search query"),
    # location: str = typer.Option(None, help="Override location"),
    # date: str = typer.Option(None, help="Data date (YYYY-MM-DD)"),
    # config: str = typer.Option("settings.yaml", help="Config file path"),
# ):
    # """
    # Run bronze ingestion stage.
    # """
    
    # config_loader = ConfigLoader(config_path=config, env=env)
    # env_config = config_loader.load_env()
    
    # log_level = getattr(logging, env_config.logging_level.upper(), logging.INFO)
    # setup_logging(log_level=log_level)
    
    # data_date = (
        # datetime.strptime(date, "%Y-%m-%d").date()
        # if date
        # else datetime.utcnow().date()
    # )
    
    # spark = create_spark(env_config.spark)
    
    # pipeline_ctx = build_pipeline_context(
        # data_date=data_date, 
        # config=env_config,
        # spark=spark
        # )
        
    # bronze_ctx = build_bronze_context(
        # pipeline_ctx = pipeline_ctx,
        # query = query,
        # location = location
    # )
    
    
    # storage = get_storage(env_config.storage.type)
    
    # connectors = build_connectors(env_config)
    
    # for connector in connectors:
        # run_bronze_pipeline(
            # ctx=bronze_ctx,
            # storage=storage,
            # connector=connector
        # )
