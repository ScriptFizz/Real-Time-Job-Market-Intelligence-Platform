import typer
from datetime import date
from job_plat.ingestion.run_scrape import run_indeed_scrape
from job_plat.nlp.run_extract_skills import run_extract_skills

app = typer.Typer()

@app.command()
def scrape_indeed_cli(
    run_date: date = typer.Option(),
    query: str = "data engineer",
    location: str = "Milan"
) -> None:
    count = run_indeed_scrape(run_date = run_date, query = query, location = location)
    typer.echo(f"Scrapped {count} jobs from Indeed")


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
