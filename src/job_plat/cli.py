import typer
from datetime import date
from job_plat.ingestion.run_scrape import run_indeed_scrape

app = typer.Typer()

@app.command()
def scrape_indeed(
    run_date: date = typer.Option(),
    query: str = "data engineer",
    location: str = "Milan"
):
    count = run_indeed_scrape(run_date = run_date, query = query, location = location)
    typer.echo(f"Scrapped {count} jobs from Indeed")

if __name__ == "__main__":
    app()
