from datetime import date
from job_plat.ingestion.scrape_indeed import scrape_indeed
from job_plat.utils.io import write_jsonl

def run_indeed_scrape(run_date: date, query: str, location: str) -> int:
    url = build_indeed_url(query, location)
    jobs = scrape_indeed(url)
    
    output_path = f"bronze/indeed/{run_date.isoformat()}.jsonl"
    write_jsonl(jobs, output_path)
    
    return len(jobs)
