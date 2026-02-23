from job_plat.bronze.ingestion.http_client import HttpClient
from job_plat.bronze.ingestion.url_builders import build_indeed_url, build_linkedin_url
from pathlib import Path
#from datetime import datetime
import argparse

def update_fixtures_f(
    base_path: str = "tests/fixtures",
    query: str = "data engineer",
    location: str = "Berlin"
) -> None:
    client = HttpClient(headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/117.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://www.google.com/",
            })

    linkedin_url = build_linkedin_url(
            query=query,
            location=location
        )
    linkedin_html = client.get_text(linkedin_url)
    
    client.close()
    
    indeed_url = build_indeed_url(
            query=query,
            location=location
        )
    indeed_html = client.get_text(indeed_url)
    
    #timestamp = datetime.utcnow().strftime("%Y_%m_%d")
    
    linkedin_path = Path(base_path) / "linkedin" / "current.html" #f"v1_{timestamp}"
    indeed_path = Path(base_path) / "indeed" / "current.html" #f"v1_{timestamp}"
    
    linkedin_path.parent.mkdir(parents=True, exist_ok=True)
    indeed_path.parent.mkdir(parents=True, exist_ok=True)
    
    Path(linkedin_path).write_text(
        linkedin_html,
        encoding="utf-8"
    )
    
    Path(indeed_path).write_text(
        indeed_html,
        encoding="utf-8"
    )
    
    print("Fixtures updated.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-path", default="tests/fixtures")
    parser.add_argument("--query", default="data engineer")
    parser.add_argument("--location", default="Berlin")

    args = parser.parse_args()
    update_fixtures_f(
        base_path=args.base_path,
        query=args.query,
        location=args.location
    )
