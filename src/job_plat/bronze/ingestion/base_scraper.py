from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any
from bs4 import BeautifulSoup
from job_plat.bronze.ingestion.http_client import HttpClient
from ... import build_indeed_url, build_linkedin_url

class BaseJobScraper(ABC):
    
    source_name: str
    
    def __init__(self, client: HttpClient):
        self.client = client
    
    def scrape(self, url: str) -> Iterator[Dict[str, Any]]:
        html = self.client.get_text(url)
        soup = BeautifulSoup(html, "html.parser")
        yield from self.parse(soup)
    
    @abstractmethod
    def build_url(self, query: str, location: str) -> str:
        pass
    
    @abstractmethod
    def parse(self, soup: BeautifulSoup) -> Iterator[Dict[str, Any]]:
        pass


class IndeedScraper(BaseJobScraper):
    source_name = "linkedin"
    
    def build_url(self, query: str, location: str):
        return build_indeed_url(query=query, location=location)
        
        
    def parse(self, soup: BeautifulSoup):
        yield from parse_indeed_job_cards(soup)

class LinkedInScraper(BaseJobScraper):
    source_name = "linkedin"
    
    def build_url(self, query: str, location: str):
        return build_linkedin_url(query=query, location=location)
    
    def parse(self, soup: BeautifulSoup):
        yield from parse_linkedin_job_cards(soup)
