from job_plat.bronze.ingestion.http_client import HttpClient
from job_plat.bronze.ingestion.url_builders import build_indeed_url, build_linkedin_url
from pathlib import Path
#from job_plat.config.browser import DEFAULT_BROWSER_HEADERS
import argparse

PROJ_ROOT = Path(__file__).resolve().parents[3]

def update_fixtures_f(
    base_path: str = "tests/fixtures",
    query: str = "data engineer",
    location: str = "Berlin"
) -> None:
    
    linkedin_url = build_linkedin_url(
            query=query,
            location=location
        )
    
    indeed_url = build_indeed_url(
            query=query,
            location=location
        )
    
    with HttpClient() as client:
        linkedin_html = client.get_text(linkedin_url)
        
        try:
            indeed_html = client.get_text(indeed_url)
        except Exception as e:
            print("Indeed fetch failed: ", e)
            indeed_html = None
    
    
    linkedin_path = PROJ_ROOT / base_path / "linkedin" / "current.html" 
    indeed_path = PROJ_ROOT / base_path / "indeed" / "current.html" 
    
    linkedin_path.parent.mkdir(parents=True, exist_ok=True)
    indeed_path.parent.mkdir(parents=True, exist_ok=True)
    
    linkedin_path.write_text(
        linkedin_html,
        encoding="utf-8"
    )
    print("Linkedin fixture updated")
    
    if indeed_html is not None:
        indeed_path.write_text(
            indeed_html,
            encoding="utf-8"
        )
        print("Indeed fixture updated")
    else:
        print("Indeed fixture NOT updated")
    
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
