from typing import Iterator, Dict, Any
from bs4 import BeautifulSoup
from job_plat.bronze.ingestion.http_client import HttpClient
from job_plat.bronze.ingestion.sources_config import JobSource, IndeedJobSource, LinkedInJobSource

class JobScraper:

    def __init__(self, client: HttpClient, source: JobSource):
        self.client = client
        self.source = source
        
    def scrape(self) -> Iterator[Dict[str, Any]]:
        html = self.client.get_text(self.source.search_url, self.source.ready_selector)
        soup = BeautifulSoup(html, "html.parser")
        yield from self.source.parse(soup)


        

# from abc import ABC, abstractmethod
# from typing import Iterator, Dict, Any
# from bs4 import BeautifulSoup
# from job_plat.bronze.ingestion.http_client import HttpClient
# from job_plat.bronze.ingestion.url_builders import build_indeed_url, build_linkedin_url
# from job_plat.bronze.ingestion.parser_indeed import parse_indeed_job_cards
# from job_plat.bronze.ingestion.parser_linkedin import parse_linkedin_job_cards

# class BaseJobScraper(ABC):
    
    # source_name: str
    
    # def __init__(self, client: HttpClient):
        # self.client = client
    
    # def scrape(self, url: str) -> Iterator[Dict[str, Any]]:
        # html = self.client.get_text(url)
        # soup = BeautifulSoup(html, "html.parser")
        # yield from self.parse(soup)
    
    # @abstractmethod
    # def build_url(self, query: str, location: str) -> str:
        # pass
    
    # @abstractmethod
    # def parse(self, soup: BeautifulSoup) -> Iterator[Dict[str, Any]]:
        # pass


# class IndeedScraper(BaseJobScraper):
    # source_name = "linkedin"
    
    # def build_url(self, query: str, location: str):
        # return build_indeed_url(query=query, location=location)
        
        
    # def parse(self, soup: BeautifulSoup):
        # yield from parse_indeed_job_cards(soup)

# class LinkedInScraper(BaseJobScraper):
    # source_name = "linkedin"
    
    # def build_url(self, query: str, location: str):
        # return build_linkedin_url(query=query, location=location)
    
    # def parse(self, soup: BeautifulSoup):
        # yield from parse_linkedin_job_cards(soup)

