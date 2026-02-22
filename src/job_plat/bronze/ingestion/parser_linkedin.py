from typing import Iterator, Dict, Any
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
from job_plat.bronze.ingestion.url_builders import build_linkedin_url

def extract_linkedin_job_id(url: str) -> str | None:
    """
    Extract job ID from LinkedIn job URL.
    Example:
    https://www.linkedin.com/jobs/view/1234567890/
    """
    if not url:
        return None
    
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")
    
    if "view" in parts:
        try:
            idx = parts.index("view")
            return parts[idx + 1]
        except (ValueError, IndexError):
            return None
            
    return None
    
def parse_linkedin_job_cards(soup: BeautifulSoup) -> Iterator[Dict[str, Any]]:
    
    job_cards = soup.find_all("div", class="base-card")
    
    if not job_cards:
        raise ValueError("No LinkedIn job cards found - selector likely broken")
    
    for card in job_cards:
        job = {
            "source": "linkedin",
            "job_id": None,
            "job_title_raw": None,
            "company_raw": None,
            "location_raw": None,
            "description_raw": None,
            "url": None,
            "scraped_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Title + UEL
        link_tag = card.select_one("a.base-card__full-link")
        if link_tag:
            job["url"] = link_tag.get("href")
            job["job_id"] = extract_linkedin_job_id(job["url"])
            
        # Title
        title_tag = card.select_one("h3")
        if title_tag:
            job["job_title_raw"] = title_tag.get_text(strip=True)
        
        # Company
        company_tag = card.select_one("h4")
        if company_tag:
            job["company_raw"] = company_tag.get_text(strip=True)
        
        # Location
        location_tag = card.select_one(".job-search-card__location")
        if location_tag:
            job["location_raw"] = location_tag.get_text(strip=True)
        
        yield job

def scrape_linkedin(
    url: str,
    client: HttpClient) -> Iterator[Dict[str, Any]]:
        html = client.get_text(url)
        soup = BeautifulSoup(html, "html.parser")
        yield from parse_linkedin_job_cards(soup)



if __name__ == "__main__":
    TEST_URL = build_linkedin_url(
        query="data engineer",
        location="Milan"
    )
    client = HttpClient(headers={
        "User-Agent": "Mozilla/5.0 (compatible; JobPlatBot/1.0)"
        })
    jobs = scrape_linkedin(
        url=TEST_URL,
        client=client)
    print(jobs[:2])
