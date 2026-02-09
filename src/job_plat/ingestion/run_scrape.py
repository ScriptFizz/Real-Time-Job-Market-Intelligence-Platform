from datetime import date
from job_plat.ingestion.scrape_indeed import scrape_indeed
from job_plat.utils.io import write_jsonl
from pathlib import Path

def run_indeed_scrape(run_date: date, query: str, location: str) -> int:
    """
    Scrape Indeed job postings and store them in the Bronze layer as JSONL.
    
    Args:
      run_date (date): Execution date of the scrape.
      query (str): job name to use for the search.
      location (str): location to use for the search.
    
    Returns:
      int: count of job cards scraped.
    """
    url = build_indeed_url(query, location)
    output_path = Path(f"bronze/indeed/{run_date.isoformat()}.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        for job in scrape_indeed(url):
            f.write(json.dumps(job, ensure_ascii=False) + "\n")
            count += 1
    
    return count


# def run_indeed_scrape(run_date: date, query: str, location: str) -> int:
    # url = build_indeed_url(query, location)
    # jobs = scrape_indeed(url)
    
    # output_path = f"bronze/indeed/{run_date.isoformat()}.jsonl"
    # write_jsonl(jobs, output_path)
    
    # return len(jobs)
