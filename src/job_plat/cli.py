import typer
from datetime import date
from job_plat.ingestion.run_scrape import run_indeed_scrape
from job_plat.processing.run_clean import run_clean
from job_plat.nlp.run_extract_skills import run_extract_skills

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
        run_date (date): Execution date of the scrape.
        bronze_path (str | Path): filepath of the Bronze layer.
        silver_path: (str | Path): filepath of the Silver layer.
    """
    
    params = load_params("settings.yaml")
    
    base_bronze_path = bronze_path or params["path"]["bronze"]
    
    base_silver_path = silver_path or params["path"]["silver"]
    
    full_silver_path = Path(base_silver_path) / f"{data_date.isoformat()}.parquet"
    
    run_clean(base_bronze_path = base_bronze_path, silver_path = full_silver_path)
    typer.echo(f"Data cleaned and stored in {full_silver_path}")

@app.command()
def extract_skills_cli(
    silver_path: str | None = None,
    gold_path: str | None = None
) -> None:

    run_extract_skills(
        silver_path=silver_path,
        gold_path=gold_path
    )

if __name__ == "__main__":
    app()
