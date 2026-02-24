from job_plat.bronze.ingestion.http_client import HttpClient
#from job_plat.bronze.ingestion.url_builders import build_indeed_url, build_linkedin_url
from job_plat.bronze.ingestion.sources_builders import build_indeed_source, build_linkedin_source
from pathlib import Path
#from job_plat.config.browser import DEFAULT_BROWSER_HEADERS
import argparse

PROJ_ROOT = Path(__file__).resolve().parents[3]

def update_fixtures_f(
    base_path: str = "tests/fixtures",
    query: str = "data engineer",
    location: str = "Berlin",
    domain: str = "de"
) -> None:
    
    linkedin_source = build_linkedin_source(
            query=query,
            location=location
        )
    
    indeed_source = build_indeed_source(
            query=query,
            location=location,
            domain=domain
        )
    
    with HttpClient() as client:
        linkedin_html = client.get_text(linkedin_source.search_url, linkedin_source.ready_selector)
        
        try:
            indeed_html = client.get_text(indeed_source.search_url, indeed_source.ready_selector)
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
    parser.add_argument("--domain", default="de")

    args = parser.parse_args()
    update_fixtures_f(
        base_path=args.base_path,
        query=args.query,
        location=args.location
    )
