from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Callable, Iterator, Dict, Any
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urlencode

@dataclass
class JobSource(ABC):
    name: str
    base_url: str
    search_url: str
    ready_selector: str
    #job_card_selector: str
    
    @abstractmethod
    def parse(self, soup: BeautifulSoup) -> Iterator[Dict[str, Any]]:
        """
        Parse the soup for job postings.
        """
        pass


@dataclass
class IndeedJobSource(JobSource):
    job_card_selector: str = '[data-testid="job-card"]'

    def parse(soup: BeautifulSoup) -> Iterator[Dict[str, Any]]:
        """
        Parse job cards from an Indeed search results page.
        
        Args:
            soup (BeautfulSoup): Parsed HTML containing job cards.
        
        Yields: 
            Dict[str, Any]: Parsed raw job data (Bronze layer).
        """
        jobs = []
        
        job_cards = soup.select(self.job_card_selector) #soup.find_all("div", job_card_selector)
        
        if not job_cards:
            raise ValueError("No job cards found - selector likely broken")
        
        for card in job_cards:
            job = {
                "source": self.name,
                "job_id": card.get("data-jk"),
                "job_title_raw": None,
                "company_raw": None,
                "location_raw": None,
                "description_raw": None,
                "url": None,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
            
            title_tag = card.select_one("h2.title a")
            if title_tag and title_tag.a:
                job["job_title_raw"] = title_tag.a.get_text(strip=True)
                job["url"] = self.base_url + title_tag.a["href"]
            
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


@dataclass
class LinkedInJobSource(JobSource):

    def parse(self, soup: BeautifulSoup) -> Iterator[Dict[str, any]]:
        job_cards = soup.find_all("div", class_="base-card")
        if not job_cards:
            raise ValueError("No LinkedIn job cards found - selector likely broken")
        for card in job_cards:
            job = {
                "source": self.name,
                "job_id": None,
                "job_title_raw": None,
                "company_raw": None,
                "location_raw": None,
                "description_raw": None,
                "url": None,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
            link_tag = card.select_one("a.base-card__full-link")
            if link_tag:
                job["url"] = link_tag.get("href")
                job["job_id"] = extract_linkedin_job_id(job["url"])
            title_tag = card.select_one("h3")
            if title_tag:
                job["job_title_raw"] = title_tag.get_text(strip=True)
            company_tag = card.select_one("h4")
            if company_tag:
                job["company_raw"] = company_tag.get_text(strip=True)
            location_tag = card.select_one(".job-search-card__location")
            if location_tag:
                job["location_raw"] = location_tag.get_text(strip=True)
            yield job

        
# def indeed_source
    # query: str,
    # location: str,
    # country: str = "us") -> JobSource:
        
    # domains = {
    # "us": "https://www.indeed.com",
    # "de": "https://de.indeed.com",
    # "uk": "https://uk.indeed.com",
    # "fr": "https://fr.indeed.com",
    # "it": "https://it.indeed.com",
    # }   
    
    # errors = []
    # if not query:
        # errors.append("Missing job role")
    # if not location:
        # errors.append("Missing location")
    
    # base_url = domains.get(country.lower())
    # if not base_url:
        # errors.append(f"Unsupported country: {country}")
    
    # if errors:
        # raise ValueError(f"Error in building the url: {', '.join(errors)}")
    
    # params = {
        # "q": query,
        # "l": location
    # }
    # full_url = f"{base}/jobs?{urlencode(params)}"
    
    # return JobSource(
        # name="indeed",
        # base_url=base_url,
        # search_url=full_url,
        # ready_selector='[data-testid="job-card"], :has-text("No jobs found")',
        # job_card_selector='[data-testid="job-card"]',
        # parser=parse_indeed_job_cards
    # )
