import typer
import logging
from dotenv import load_dotenv
from datetime import datetime
from job_plat.config.config_loader import ConfigLoader
from job_plat.bronze.ingestion.connectors import build_connectors
from job_plat.pipelines.context.context_builders import build_pipeline_context, build_bronze_context
from job_plat.pipelines.pipeline_stages import run_bronze_pipeline, run_full_pipeline
from job_plat.config.logconfig import setup_logging
from job_plat.utils.storage import get_storage
from job_plat.utils.helpers import create_spark

load_dotenv()

app = typer.Typer(help="Job postings data pipeline CLI")

@app.command()
def bronze(
    env: str = typer.Option("dev", help="Environment (dev or prod)"),
    query: str = typer.Option(None, help="Override search query"),
    location: str = typer.Option(None, help="Override location"),
    date: str = typer.Option(None, help="Data date (YYYY-MM-DD)"),
    config: str = typer.Option("settings.yaml", help="Config file path"),
):
    """
    Run bronze ingestion stage.
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
        
    bronze_ctx = build_bronze_context(
        pipeline_ctx = pipeline_ctx,
        query = query,
        location = location
    )
    
    print("cli_QUERY: ", bronze_ctx.query) 
    print("cli_LOCATION: ", bronze_ctx.location) 
    
    storage = get_storage(env_config.storage.type)
    
    connectors = build_connectors(env_config)
    
    for connector in connectors:
        run_bronze_pipeline(
            ctx=bronze_ctx,
            storage=storage,
            connector=connector
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
    
