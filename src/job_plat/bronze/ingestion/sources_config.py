from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Callable, Iterator, Dict, Any
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urlencode
import requests
import time
import logging

logger = logging.getLogger(__name__)


@dataclass
class JobConnector(ABC):
    name: str
    
    @abstractmethod
    def fecth(self, **kwargs) -> Iterator[dict]:
        """
        Stream raw jobs from the source (handles pagination internally).
        """
        pass
    
    @abstractmethod
    def normalize(self, raw_job: dict) -> dict:
        """
        Convert source-specific job schema into unified schema.
        """
        pass


class USAJobConnector(JobConnector):
    
    def __init__(self, api_key: str):
        self.name = "usajobs"
        self.base_url = "https://data.usajobs.gov/api/search"
        self.headers = {
            "Host": "data.usajobs.gov",
            "User-Agent": "your_email@example.com",
            "Authorization-Key": api_key,
        }
    
    def _api_call(self, keyword: str, page: int) -> dict:
        
        start = time.time()
        
        try:
            response = requests.get(
                self.base_url,
                params={"Keyword": keyword, "Page": page},
                headers=self.headers,
                timeout=30,
            )
            
            duration = round(time.time() - start, 3)
            
            logger.info(
                "usajobs_api_call",
                extra={
                    "source": self.name,
                    "page": page,
                    "status_code": response.status_code,
                    "duration_sec": duration,
                },
            )
            
            response.raise_for_status()
            
            return response.json()
        
        except requests.RequestException:
            logger.error(
                "usajobs_api_call_failed",
                extra={
                    "source": self.name,
                    "page": page,
                },
                exc_info=True
            )
            raise
    
    def fetch(self, keyword: str) -> Iterator[dict]:
        
        logger.info(
            "connector_fetch_started",
            extra={"source": self.name, "keyword": keyword}
        )
        
        page = 1
        total_records = 0
        
        while True:
            data = self._call_api(keyword, page)
            results = data["SearchResult"]["SearchResultItems"]
            
            if not results:
                break
            
            logger.info(
                "connector_page_fetched",
                extra={
                    "source": self.name,
                    "page": page,
                    "records_in_page": len(results)
                }
            )
            
            for item in results:
                total_records += 1
                yield item
            
            logger.info(
                "connector_fetch_completed",
                extra={
                    "source": self.name,
                    "total_records": total_records,
                    "pages_fetched": page - 1,
                },
            )
            
            page += 1
    
    def normalize(self, raw_job: dict) -> dict:
        desc = raw_job["MatchedObjectDescriptor"]
        
        return {
            "source": self.name,
            "source_job_id": desc["PositionID"],
            "job_title_raw": desc["PositionTitle"],
            "company_raw": desc["OrganizationName"],
            "location_raw": desc["PositionLocationDisplay"],
            "description_raw": desc.get("UserArea", {}).get("Details", {}).get("JobSummary"),
            "employment_type_raw": desc.get("PositionSchedule", [{}])[0].get("Name"),
            "salary_min_raw": desc.get("PositionRemuneration", [{}])[0].get("MinimumRange"),
            "salary_max_raw": desc.get("PositionRemuneration", [{}])[0].get("MaximumRange"),
            "currency_raw": "USD",
            "posted_at": desc.get("PublicationStartDate")
        }


# @dataclass
# class JobSource(ABC):
    # name: str
    # base_url: str
    # search_url: str
    # ready_selector: str
    # #job_card_selector: str
    
    # @abstractmethod
    # def parse(self, soup: BeautifulSoup) -> Iterator[Dict[str, Any]]:
        # """
        # Parse the soup for job postings.
        # """
        # pass


# @dataclass
# class IndeedJobSource(JobSource):
    # job_card_selector: str = '[data-testid="job-card"]'

    # def parse(soup: BeautifulSoup) -> Iterator[Dict[str, Any]]:
        # """
        # Parse job cards from an Indeed search results page.
        
        # Args:
            # soup (BeautfulSoup): Parsed HTML containing job cards.
        
        # Yields: 
            # Dict[str, Any]: Parsed raw job data (Bronze layer).
        # """
        # jobs = []
        
        # job_cards = soup.select(self.job_card_selector) #soup.find_all("div", job_card_selector)
        
        # if not job_cards:
            # raise ValueError("No job cards found - selector likely broken")
        
        # for card in job_cards:
            # job = {
                # "source": self.name,
                # "job_id": card.get("data-jk"),
                # "job_title_raw": None,
                # "company_raw": None,
                # "location_raw": None,
                # "description_raw": None,
                # "url": None,
                # "scraped_at": datetime.now(timezone.utc).isoformat(),
            # }
            
            # title_tag = card.select_one("h2.title a")
            # if title_tag and title_tag.a:
                # job["job_title_raw"] = title_tag.a.get_text(strip=True)
                # job["url"] = self.base_url + title_tag.a["href"]
            
            # company_tag = card.find("span", class_="company")
            # if company_tag:
                # job["company_raw"] =  company_tag.get_text(strip=True)
            
            # location_tag = card.find("div", class_="recJobLoc")
            # if location_tag:
                # job["location_raw"] = location_tag.get("data-rc-loc")
                
            # summary_tag = card.find("div", class_="summary")
            # if summary_tag:
                # job["description_raw"] = summary_tag.get_text(" ", strip=True)
            
            # yield job


# @dataclass
# class LinkedInJobSource(JobSource):

    # def parse(self, soup: BeautifulSoup) -> Iterator[Dict[str, any]]:
        # job_cards = soup.find_all("div", class_="base-card")
        # if not job_cards:
            # raise ValueError("No LinkedIn job cards found - selector likely broken")
        # for card in job_cards:
            # job = {
                # "source": self.name,
                # "job_id": None,
                # "job_title_raw": None,
                # "company_raw": None,
                # "location_raw": None,
                # "description_raw": None,
                # "url": None,
                # "scraped_at": datetime.now(timezone.utc).isoformat(),
            # }
            # link_tag = card.select_one("a.base-card__full-link")
            # if link_tag:
                # job["url"] = link_tag.get("href")
                # job["job_id"] = extract_linkedin_job_id(job["url"])
            # title_tag = card.select_one("h3")
            # if title_tag:
                # job["job_title_raw"] = title_tag.get_text(strip=True)
            # company_tag = card.select_one("h4")
            # if company_tag:
                # job["company_raw"] = company_tag.get_text(strip=True)
            # location_tag = card.select_one(".job-search-card__location")
            # if location_tag:
                # job["location_raw"] = location_tag.get_text(strip=True)
            # yield job
