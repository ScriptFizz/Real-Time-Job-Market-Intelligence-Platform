import logging
import math
import os
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any

import requests

from job_plat.config.env_config import EnvironmentConfig
from job_plat.ingestion.job_schema import CanonicalJobV1, JobSource
from job_plat.ingestion.search_criteria import JobSearchCriteria

logger = logging.getLogger(__name__)


def first_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    return {}


def require_nonempty_string(
    mapping: dict[str, Any],
    key: str,
) -> str:
    value = mapping.get(key)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Expected non-empty string field: {key}")

    return value


def require_environment_variable(name: str) -> str:
    value = os.getenv(name)

    if value is None or not value.strip():
        raise RuntimeError(f"Required environment variable is not set: {name}")

    return value


SUPPORTED_COUNTRIES = {"us", "gb", "de", "fr", "it", "nl", "ca", "au"}


class JobConnector(ABC):
    name: JobSource

    @abstractmethod
    def fetch(self, criteria: JobSearchCriteria) -> Iterator[dict[str, Any]]:
        """
        Stream raw jobs from the source (handles pagination internally).
        """
        raise NotImplementedError

    @abstractmethod
    def normalize(self, raw_job: dict[str, Any]) -> CanonicalJobV1:
        """
        Convert source-specific job schema into unified schema.
        """
        raise NotImplementedError


class PaginatedAPIConnector(JobConnector):
    base_url: str

    def __init__(
        self,
        max_pages: int | None = None,
        min_interval_seconds: float | None = None,
    ):
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

    def _api_get_response(
        self,
        url: str,
        params: dict,
        headers: dict | None = None,
        timeout: int = 30,
        meta: dict | None = None,
    ) -> dict:
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
                exc_info=True,
            )
            raise

    @abstractmethod
    def _api_call(self, criteria: JobSearchCriteria, page: int) -> dict:
        pass

    @abstractmethod
    def _extract_results(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        pass

    def fetch(self, criteria: JobSearchCriteria) -> Iterator[dict]:
        logger.info(
            "connector_fetch_started",
            extra={
                "source": self.name,
                "query": criteria.query,
                "location": criteria.location,
            },
        )

        page = 1
        total_records = 0
        api_max_pages = None

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

            if page == 1:
                total = data.get("count")

                if total and results:
                    api_max_pages = math.ceil(total / len(results))

                    logger.info(
                        "connnector_total_results_detected",
                        extra={
                            "source": self.name,
                            "total_results": total,
                            "extimated_pages": api_max_pages,
                        },
                    )

            if not results:
                logger.info(
                    "connector_empty_page",
                    extra={"source": self.name, "page": page},
                )
                break

            logger.info(
                "connector_page_fetched",
                extra={
                    "source": self.name,
                    "page": page,
                    "records_in_page": len(results),
                },
            )

            for item in results:
                total_records += 1
                yield item

            page += 1

            if api_max_pages and page > api_max_pages:
                break

        logger.info(
            "connector_fetch_completed",
            extra={
                "source": self.name,
                "total_records": total_records,
                "pages_fetched": page - 1,
            },
        )


class USAJobConnector(PaginatedAPIConnector):
    name: JobSource = "usajobs"

    def __init__(
        self,
        api_key: str,
        max_pages: int | None = None,
        min_interval_seconds: float | None = None,
    ):
        super().__init__(max_pages=max_pages, min_interval_seconds=min_interval_seconds)

        self.base_url = "https://data.usajobs.gov/api/search"
        self.headers = {
            "Host": "data.usajobs.gov",
            "User-Agent": "your_email@example.com",
            "Authorization-Key": api_key,
        }

    def _api_call(self, criteria: JobSearchCriteria, page: int) -> dict:
        params = {"Keyword": criteria.query, "Page": page}

        if criteria.location:
            params["LocationName"] = criteria.location

        meta = {"page": page}
        return self._api_get_response(
            url=self.base_url, params=params, headers=self.headers, meta=meta
        )

    def _extract_results(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        return data["SearchResult"]["SearchResultItems"]

    def normalize(self, raw_job: dict[str, Any]) -> CanonicalJobV1:
        desc = raw_job["MatchedObjectDescriptor"]

        schedule = first_mapping(desc.get("PositionSchedule"))
        offering = first_mapping(desc.get("PositionOfferingType"))
        remuneration = first_mapping(desc.get("PositionRemuneration"))

        return CanonicalJobV1(
            source=self.name,
            source_job_id=require_nonempty_string(desc, "PositionID"),
            url=desc.get("PositionURI"),
            job_title_raw=desc["PositionTitle"],
            company_raw=desc["OrganizationName"],
            location_raw=desc["PositionLocationDisplay"],
            description_raw=desc.get("UserArea", {})
            .get("Details", {})
            .get("JobSummary"),
            employment_type_raw=schedule.get("Name"),
            contract_type_raw=offering.get("Name"),
            salary_min_raw=remuneration.get("MinimumRange"),
            salary_max_raw=remuneration.get("MaximumRange"),
            currency_raw="USD",
            posted_at_raw=desc.get("PublicationStartDate"),
        )


class ADZunaConnector(PaginatedAPIConnector):
    name: JobSource = "adzuna"

    def __init__(
        self,
        api_key: str,
        app_id: str,
        max_pages: int | None = None,
        min_interval_seconds: float | None = None,
    ):
        super().__init__(max_pages=max_pages, min_interval_seconds=min_interval_seconds)

        self.base_url = "https://api.adzuna.com/v1/api/jobs"
        self.app_id = app_id
        self.api_key = api_key

    def _api_call(self, criteria: JobSearchCriteria, page: int) -> dict:
        country = criteria.country.strip().lower()
        if country not in SUPPORTED_COUNTRIES:
            raise ValueError(f"Unsupported ADZuna country: {country}")

        url = f"{self.base_url}/{country}/search/{page}"
        params = {
            "app_id": self.app_id,
            "app_key": self.api_key,
            "what": criteria.query,
            "results_per_page": 50,
            "sort_by": "date",
        }

        if criteria.location:
            params["where"] = criteria.location

        meta = {"page": page}
        return self._api_get_response(url=url, params=params, meta=meta)

    def _extract_results(self, data: dict) -> list[dict]:
        return data.get("results", [])

    def normalize(self, raw_job: dict) -> CanonicalJobV1:
        return CanonicalJobV1(
            source=self.name,
            source_job_id=require_nonempty_string(raw_job, "id"),
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
            posted_at_raw=raw_job.get("created"),
        )


def build_connectors(config: EnvironmentConfig) -> list[JobConnector]:
    return [
        # USAJobConnector(
        # api_key=os.getenv("USAJOBS_API_KEY"),
        # max_pages=config.bronze.max_pages,
        # min_interval_seconds = config.bronze.min_interval_seconds
        # ),
        ADZunaConnector(
            api_key=require_environment_variable("ADZUNA_API_KEY"),
            app_id=require_environment_variable("ADZUNA_APP_ID"),
            max_pages=config.bronze.max_pages,
            min_interval_seconds=config.bronze.min_interval_seconds,
        )
    ]
