import typer
from datetime import date
from job_plat.ingestion.run_scrape import run_indeed_scrape
from job_plat.processing.run_clean import run_clean
from job_plat.nlp.run_extract_skills import run_extract_skills
from job_plat.embeddings.run_embed_normalize_skills import embed_normalize_skills

app = typer.Typer()

@app.command()
def scrape_indeed_cli(
    run_date: date = typer.Argument(..., help="Execution date of the scrape"),
    query: str = typer.Option("data engineer", help="Job title or keyword to search for"),
    location: str = typer.Option("Milan", help="Job location to search for")
) -> None:
    """
    Scrape Indeed job postings and store them in the Bronze layer as JSONL.

    Args:
        run_date (date): Execution date of the scrape.
        query (str): Job title or keyword to search for.
        location (str): Job location to search for.
    """
    count = run_indeed_scrape(run_date = run_date, query = query, location = location)
    typer.echo(f"Scrapped {count} jobs from Indeed")


@app.command()
def run_clean_cli(
    data_date: date = typer.Argument(..., help="Logical data date (scrape date / snapshot version)"),
    bronze_path: str | None = typer.Option(None, help="filepath of the Bronze layer."),
    silver_path: str | None = typer.Option(None, help="filepath of the Silver layer.")
) -> None:
    """
    Clean and deduplicate jobs data from the Bronze layer, store it in the Silver layer.

    Args:
        run_date (date): Logical data date (scrape date / snapshot version).
        bronze_path (str | Path): Filepath of the Bronze layer.
        silver_path: (str | Path): Filepath of the Silver layer.
    """
    
    params = load_params("settings.yaml")
    
    base_bronze_path = bronze_path or params["path"]["bronze"]
    base_silver_path = silver_path or params["path"]["silver"]
    
    full_silver_path = Path(base_silver_path) / f"{data_date.isoformat()}.parquet"
    
    run_clean(base_bronze_path = base_bronze_path, silver_path = full_silver_path)
    typer.echo(f"Data cleaned and stored in {full_silver_path}")

@app.command()
def extract_skills_cli(
    data_date: date = typer.Argument(..., help="Logical data date (scrape date / snapshot version)"),
    silver_path: str | None = typer.Option(None, help="filepath of the Silver layer."),
    gold_path: str | None = typer.Option(None, help="filepath of the Gold layer.")
) -> None:
    """
    Extract from each job its skills, store the result in the Gold layer.
    
    Args:
        run_date (date): Logical data date (scrape date / snapshot version).
        bronze_path (str | Path): Filepath of the Bronze layer.
        silver_path: (str | Path): Filepath of the Silver layer.
        silver_path: (str | Path): Filepath of the Silver layer.
    """
    
    params = load_params("settings.yaml")
    
    base_silver_path = silver_path or params["path"]["silver"]
    base_gold_path = gold_path or params["path"]["gold"]
    
    full_silver_path = Path(base_silver_path) / f"{data_date.isoformat()}.parquet"
    full_gold_path = Path(base_gold_path) / f"job_skills_{data_date.isoformat()}.parquet"
    
    run_extract_skills(
        silver_path=silver_path,
        gold_path=gold_path
    )
    typer.echo(f"Job skills extracted in {full_gold_path}")
    
    
@app.command()
def embed_normalize_skills_cli(
    gold_path: str | None = None,
    lookup_output_path: str | None = None
) -> None:
    """
    Perform skills embeddings and store them in the Gold layer.
    """
    
    params = load_params("settings.yaml")
    
    gold_path = gold_path or params["path"]["gold"]
    lookup_output_path = lookup_output_path or params["path"]["lookout_output_path"]
    
    run_normalize(
        gold_path = gold_path,
        lookup_output_path = lookup_output_path
    )
    typer.echo(f"Skills embeddings stored in {lookup_output_path}")

if __name__ == "__main__":
    app()



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
