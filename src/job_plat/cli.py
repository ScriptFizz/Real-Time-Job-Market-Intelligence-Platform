import typer
from datetime import datetime
from ... import load_config, resolve_bronze_params
from ... import build_pipeline_context
from ... import run_bronze, run_full_pipeline
from ... import setup_logging
from ... import get_storage

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
    
    full_config = load_params(config)
    env_config = full_config["environments"][env]
    
    setup_logging(getattr(logging, env_config["logging_level"]))
    
    data_date = (
        datetime.strptime(date, "%Y-%m-%d").date()
        if date
        else datetime.utcnow().date()
    )
    
    ctx = build_pipeline_context(data_date=data_date, config=env_config)
    
    storage = get_storage(env_config["storage"]["type"])
    
    bronze_params = resolve_bronze_params(
        config=config_dict,
        query=query,
        location=location,
    )
    
    run_bronze(
        ctx=ctx,
        storage=storage,
        query=bronze_params["query"],
        location=bronze_params["location"]
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
    
# import typer
# from datetime import date
# from job_plat.ingestion.run_scrape import run_indeed_scrape
# from job_plat.processing.run_clean import run_clean
# from job_plat.nlp.run_extract_skills import run_extract_skills
# from job_plat.embeddings.run_embed_normalize_skills import embed_normalize_skills

# app = typer.Typer()

# @app.command()
# def scrape_indeed_cli(
    # run_date: date = typer.Argument(..., help="Execution date of the scrape"),
    # query: str = typer.Option("data engineer", help="Job title or keyword to search for"),
    # location: str = typer.Option("Milan", help="Job location to search for")
# ) -> None:
    # """
    # Scrape Indeed job postings and store them in the Bronze layer as JSONL.

    # Args:
        # run_date (date): Execution date of the scrape.
        # query (str): Job title or keyword to search for.
        # location (str): Job location to search for.
    # """
    # count = run_indeed_scrape(run_date = run_date, query = query, location = location)
    # typer.echo(f"Scrapped {count} jobs from Indeed")


# @app.command()
# def run_clean_cli(
    # data_date: date = typer.Argument(..., help="Logical data date (scrape date / snapshot version)"),
    # bronze_path: str | None = typer.Option(None, help="filepath of the Bronze layer."),
    # silver_path: str | None = typer.Option(None, help="filepath of the Silver layer.")
# ) -> None:
    # """
    # Clean and deduplicate jobs data from the Bronze layer, store it in the Silver layer.

    # Args:
        # run_date (date): Logical data date (scrape date / snapshot version).
        # bronze_path (str | Path): Filepath of the Bronze layer.
        # silver_path: (str | Path): Filepath of the Silver layer.
    # """
    
    # params = load_params("settings.yaml")
    
    # base_bronze_path = bronze_path or params["path"]["bronze"]
    # base_silver_path = silver_path or params["path"]["silver"]
    
    # full_silver_path = Path(base_silver_path) / f"{data_date.isoformat()}.parquet"
    
    # run_clean(base_bronze_path = base_bronze_path, silver_path = full_silver_path)
    # typer.echo(f"Data cleaned and stored in {full_silver_path}")

# @app.command()
# def extract_skills_cli(
    # data_date: date = typer.Argument(..., help="Logical data date (scrape date / snapshot version)"),
    # silver_path: str | None = typer.Option(None, help="filepath of the Silver layer."),
    # gold_path: str | None = typer.Option(None, help="filepath of the Gold layer.")
# ) -> None:
    # """
    # Extract from each job its skills, store the result in the Gold layer.
    
    # Args:
        # run_date (date): Logical data date (scrape date / snapshot version).
        # bronze_path (str | Path): Filepath of the Bronze layer.
        # silver_path: (str | Path): Filepath of the Silver layer.
        # silver_path: (str | Path): Filepath of the Silver layer.
    # """
    
    # params = load_params("settings.yaml")
    
    # base_silver_path = silver_path or params["path"]["silver"]
    # base_gold_path = gold_path or params["path"]["gold"]
    
    # full_silver_path = Path(base_silver_path) / f"{data_date.isoformat()}.parquet"
    # full_gold_path = Path(base_gold_path) / f"job_skills_{data_date.isoformat()}.parquet"
    
    # run_extract_skills(
        # silver_path=silver_path,
        # gold_path=gold_path
    # )
    # typer.echo(f"Job skills extracted in {full_gold_path}")
    
    
# @app.command()
# def embed_normalize_skills_cli(
    # gold_path: str | None = None,
    # lookup_output_path: str | None = None
# ) -> None:
    # """
    # Perform skills embeddings and store them in the Gold layer.
    # """
    
    # params = load_params("settings.yaml")
    
    # gold_path = gold_path or params["path"]["gold"]
    # lookup_output_path = lookup_output_path or params["path"]["lookout_output_path"]
    
    # run_normalize(
        # gold_path = gold_path,
        # lookup_output_path = lookup_output_path
    # )
    # typer.echo(f"Skills embeddings stored in {lookup_output_path}")

# if __name__ == "__main__":
    # app()

#########################
###########################

# @app.command()
# def run_normalize_cli(
    # gold_path: str | None = None,
    # lookup_output_path: str | None = None
# ) -> None:
    # """
    # Perform skills embeddings and store them in the Gold layer.
    # """
    
    # params = load_params("settings.yaml")
    
    # gold_path = gold_path or params["path"]["gold"]
    # lookup_output_path = lookup_output_path or params["path"]["lookout_output_path"]
    
    # run_normalize(
        # gold_path = gold_path,
        # lookup_output_path = lookup_output_path
    # )
    # typer.echo(f"Skills embeddings stored in {lookup_output_path}")
