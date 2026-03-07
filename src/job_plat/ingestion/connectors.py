import os
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Callable, Iterator, Dict, Any
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urlencode
import requests
import time
import logging
from job_plat.ingestion.job_schema import CanonicalJobV1
from job_plat.ingestion.search_criteria import JobSearchCriteria
from job_plat.config.env_config import EnvironmentConfig

logger = logging.getLogger(__name__)


class JobConnector(ABC):
    name: str
    
    @abstractmethod
    def fetch(self, **kwargs) -> Iterator[dict]:
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


class PaginatedAPIConnector(JobConnector):
    
    base_url: str
    
    def __init__(
        self,
        max_pages: int | None = None,
        min_interval_seconds: float | None = None,):
            
        self.max_pages = max_pages
        self.min_interval_seconds = min_interval_seconds
        self._last_request_ts: float | None = None
    
    def _throttle(self) -> None:
        if not self.min_interval_seconds:
            return
        now = time.time()
        
        if self._last_request_ts is not None:
            elapsed = now - self._last_request_ts
            remaining = self.min_interval_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)
        
        self._last_request_ts = time.time()
    
    def  _api_get_response(
        self, 
        url: str,
        params: dict, 
        headers: dict | None = None, 
        timeout: int = 30,
        meta: dict| None = None) -> dict:
        
        start = time.time()
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
            )
            
            duration = round(time.time() - start, 3)
            
            logger.info(
                f"{self.name}_api_call",
                extra={
                    "source": self.name,
                    "page": meta.get("page") if meta else None,
                    "status_code": response.status_code,
                    "duration_sec": duration,
                },
            )
            
            response.raise_for_status()
            
            return response.json()
        
        except requests.RequestException:
            logger.error(
                f"{self.name}_api_call_failed",
                extra={
                    "source": self.name,
                    "page": meta.get("page") if meta else None,
                },
                exc_info=True
            )
            raise
        
    @abstractmethod
    def _api_call(self, criteria: JobSearchCriteria, page: int) -> dict:
            pass
    
    @abstractmethod
    def _extract_results(self, data: dict) -> list[dict]:
        pass
    
    def fetch(self, criteria: JobSearchCriteria) -> Iterator[dict]:
        
        logger.info(
            "connector_fetch_started",
            extra={"source": self.name, "query": criteria.query, "location": criteria.location}
        )
        
        page = 1
        total_records = 0
        
        while True:
            
            # Page cap
            if self.max_pages and page > self.max_pages:
                logger.info(
                    "connector_page_limit_reached",
                    extra={
                        "source": self.name,
                        "max_pages": self.max_pages,
                    },
                )
                break
            
            # Throttle before request
            self._throttle()
            
            data = self._api_call(criteria=criteria, page=page)
            results = self._extract_results(data)
            
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
                
            page += 1
        
        logger.info(
                "connector_fetch_completed",
                extra={
                    "source": self.name,
                    "total_records": total_records,
                    "pages_fetched": page - 1,
                },
            )


class USAJobConnector(PaginatedAPIConnector):
    
    def __init__(
        self, 
        api_key: str,
        max_pages: int | None = None,
        min_interval_seconds: float | None = None,
        ):
            
        super().__init__(
            max_pages=max_pages,
            min_interval_seconds=min_interval_seconds
        )
        
        self.name = "usajobs"
        self.base_url = "https://data.usajobs.gov/api/search"
        self.headers = {
            "Host": "data.usajobs.gov",
            "User-Agent": "your_email@example.com",
            "Authorization-Key": api_key,
        }
    
    def _api_call(self, criteria: JobSearchCriteria, page: int) -> dict:
        
        params={"Keyword": criteria.query,  "Page": page}
        
        if criteria.location:
            params["LocationName"] = criteria.location
        
        meta = {"page": page}
        return self._api_get_response(
            url=self.base_url, 
            params=params, 
            headers=self.headers,
            meta=meta)
    
    def _extract_results(self, data: dict) -> list[dict]:
        return data["SearchResult"]["SearchResultItems"]
        
    def normalize(self, raw_job: dict) -> CanonicalJobV1:
        desc = raw_job["MatchedObjectDescriptor"]
        
        return CanonicalJobV1(
            source=self.name,
            source_job_id=desc["PositionID"],
            job_title_raw=desc["PositionTitle"],
            company_raw=desc["OrganizationName"],
            location_raw=desc["PositionLocationDisplay"],
            description_raw=desc.get("UserArea", {}).get("Details", {}).get("JobSummary"),
            
            employment_type_raw=desc.get("PositionSchedule", [{}])[0].get("Name"),
            salary_min_raw=desc.get("PositionRemuneration", [{}])[0].get("MinimumRange"),
            salary_max_raw=desc.get("PositionRemuneration", [{}])[0].get("MaximumRange"),
            currency_raw="USD",
            posted_at_raw=desc.get("PublicationStartDate")
        )



class ADZunaConnector(PaginatedAPIConnector):
    
    def __init__(
        self, 
        api_key: str, 
        app_id: str,
        max_pages: int | None = None,
        min_interval_seconds: float | None = None,
        ):
            
        super().__init__(
            max_pages=max_pages,
            min_interval_seconds=min_interval_seconds
        )
            
            
        self.name = "adzuna"
        self.base_url = "https://api.adzuna.com/v1/api/jobs/us/search"
        self.app_id = app_id
        self.api_key = api_key
            
    def _api_call(self, criteria: JobSearchCriteria, page: int) -> dict:
        url = f"{self.base_url}/{page}"
        params = {
            "app_id": self.app_id,
            "app_key": self.api_key,
            "what": criteria.query,
        }
        
        if criteria.location:
            params["where"] = criteria.location
        
        meta = {"page": page}
        return self._api_get_response(
        url=url, 
        params=params,
        meta=meta)
    
    def _extract_results(self, data: dict) -> list[dict]:
        return data.get("results", [])
    
    def normalize(self, raw_job: dict) -> CanonicalJobV1:
        
        return CanonicalJobV1(
            source= self.name,
            source_job_id=raw_job.get("id"),
            job_title_raw=raw_job.get("title"),
            company_raw=raw_job.get("company", {}).get("display_name"),
            url=raw_job.get("redirect_url"),
            location_raw=raw_job.get("location", {}).get("display_name"),
            description_raw=raw_job.get("description"),
            employment_type_raw=raw_job.get("contract_time"),
            contract_type_raw=raw_job.get("contract_type"),
            salary_min_raw=raw_job.get("salary_min"),
            salary_max_raw=raw_job.get("salary_max"),
            currency_raw=None,
            posted_at_raw=raw_job.get("created")
        )



def build_connectors(config: EnvironmentConfig) -> list[JobConnector]:
    return [
        # USAJobConnector(
            # api_key=os.getenv("USAJOBS_API_KEY"),
            # max_pages=config.bronze.max_pages,
            # min_interval_seconds = config.bronze.min_interval_seconds
        # ),
        ADZunaConnector(
            api_key=os.getenv("ADZUNA_API_KEY"),
            app_id=os.getenv("ADZUNA_APP_ID"),
            max_pages=config.bronze.max_pages,
            min_interval_seconds = config.bronze.min_interval_seconds
        )
    ]
