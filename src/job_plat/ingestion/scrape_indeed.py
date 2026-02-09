import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict, Iterator, Any

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; JobPlatBot/1.0)"
} 

@retry(
    stop = stop_after_attempt(5),
    wait = wait_exponential(multipliers=1, min=2, max=30)
)
def fetch_response_text(url: str) -> str:
    """
    Fetch raw text data from a URL.

    Args:
        url (str): URL to fetch data from.

    Returns:
        str: Raw response text (e.g. HTML or JSON as string).
    """
    response = requests.get(url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    return response.text
    
def parse_job_cards(soup: BeautifulSoup) -> Iterator[Dict[str, Any]]:
    """
    Parse job cards from an Indeed search results page.
    
    Args;
        soup (BeautfulSoup): Parsed HTML containing job cards.
    
    Yields: 
        Dict[str, Any]: Parsed raw job data (Bronze layer).
    """
    jobs = []
    
    job_cards = soup.find_all("div", attrs={"data-testid": "jobsearch-SerpJobCard"})
    
    for card in job_cards:
        job = {
            "source": "indeed",
            "job_id": card.get("data-jk"),
            "job_title_raw": None,
            "company_raw": None,
            "location_raw": None,
            "description_raw": None,
            "url": None,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
        
        title_tag = card.find("h2", class_="title")
        if title_tag and title_tag.a:
            job["job_title_raw"] = title_tag.a.get_text(strip=True)
            job["url"] = "https://www.indeed.com" + title_tag.a["href"]
        
        company_tag = card.find("span", class_="company")
        if company_tag:
            job["company_raw"] =  company_tag.get_text(strip=True)
        
        location_tag = card.find("div", class_="recJobLoc")
        if location_tag:
            job["location_raw"] = location_tag.get("data-rc-loc")
            
        summary_tag = card.find("div", class_="summary")
        if summary_tag:
            job["description_raw"] = summary_tag.get_text(" ", strip=True)
        
        yield job


def scrape_indeed(url: str) -> Iterator[Dict[str, Any]]:
    """
    Fetch Indeed result page and yield parsed job cards
    
    Args:
      url (str): URL to fetch data from.
    
    Yields:
      Dict[str, Any]: Parsed raw job data (Bronze layer).
    """
    html = fetch_response_text(url)
    soup = BeautifulSoup(html, "html.parser")
    yield from parse_job_cards(soup)

if __name__ == "__main__":
    TEST_URL = "https://www.indeed.com/jobs?q=data+engineer&l=Berlin"
    jobs = scrape_indeed(TEST_URL)
    print(jobs[:2])
