import logging
import math
import os
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)


class ConnectorRequestError(RuntimeError):
    """Credential-safe error raised when an HTTP request cannot complete."""


class ConnectorHTTPError(ConnectorRequestError):
    def __init__(self, source: JobSource, status_code: int):
        self.source = source
        self.status_code = status_code
        super().__init__(f"{source} API returned HTTP {status_code}")


class ConnectorResponseError(RuntimeError):
    """Raised when a successful response does not contain the expected JSON."""


class JobConnector(ABC):
    name: JobSource
    schema_error_count: int

    @abstractmethod
    def close(self) -> None:
        """Release connector-owned resources, if any."""
        raise NotImplementedError

    def normalize_with_accounting(
        self,
        raw_job: dict[str, Any],
    ) -> CanonicalJobV1 | None:
        try:
            return self.normalize(raw_job)
        except (KeyError, TypeError, ValueError) as error:
            self.schema_error_count += 1
            logger.warning(
                "connector_schema_error",
                extra={
                    "source": self.name,
                    "error_type": type(error).__name__,
                    "schema_error_count": self.schema_error_count,
                },
            )
            return None

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
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: float = 30.0,
        retry_total: int = 3,
        retry_backoff_factor: float = 0.5,
        retry_backoff_jitter: float = 0.1,
        session: requests.Session | None = None,
    ):
        if connect_timeout_seconds <= 0 or read_timeout_seconds <= 0:
            raise ValueError("HTTP timeouts must be greater than zero")
        if retry_total < 0:
            raise ValueError("retry_total must not be negative")
        if retry_backoff_factor < 0 or retry_backoff_jitter < 0:
            raise ValueError("retry backoff values must not be negative")

        self.max_pages = max_pages
        self.min_interval_seconds = min_interval_seconds
        self.timeout = (connect_timeout_seconds, read_timeout_seconds)
        self.schema_error_count = 0
        self._last_request_ts: float | None = None
        self.session = session or self._build_session(
            retry_total=retry_total,
            backoff_factor=retry_backoff_factor,
            backoff_jitter=retry_backoff_jitter,
        )

    @staticmethod
    def _build_session(
        *,
        retry_total: int,
        backoff_factor: float,
        backoff_jitter: float,
    ) -> requests.Session:
        retry_policy = Retry(
            total=retry_total,
            connect=retry_total,
            read=retry_total,
            status=retry_total,
            allowed_methods=frozenset({"GET"}),
            status_forcelist=RETRYABLE_STATUS_CODES,
            backoff_factor=backoff_factor,
            backoff_jitter=backoff_jitter,
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_policy)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def close(self) -> None:
        self.session.close()

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
        params: dict[str, Any],
        headers: dict[str, str] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        start = time.time()
        try:
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )
        except requests.RequestException as error:
            logger.error(
                f"{self.name}_api_call_failed",
                extra={
                    "source": self.name,
                    "page": meta.get("page") if meta else None,
                    "error_type": type(error).__name__,
                },
            )
            raise ConnectorRequestError(
                f"{self.name} API request failed: {type(error).__name__}"
            ) from None

        duration = round(time.time() - start, 3)
        retries = getattr(getattr(response, "raw", None), "retries", None)
        retry_history = getattr(retries, "history", ())
        retry_count = len(retry_history) if retry_history is not None else 0

        logger.info(
            f"{self.name}_api_call",
            extra={
                "source": self.name,
                "page": meta.get("page") if meta else None,
                "status_code": response.status_code,
                "duration_sec": duration,
                "retry_count": retry_count,
            },
        )

        if response.status_code >= 400:
            raise ConnectorHTTPError(self.name, response.status_code)

        try:
            payload = response.json()
        except ValueError as error:
            logger.error(
                f"{self.name}_api_invalid_json",
                extra={
                    "source": self.name,
                    "page": meta.get("page") if meta else None,
                    "error_type": type(error).__name__,
                },
            )
            raise ConnectorResponseError(
                f"{self.name} API returned invalid JSON"
            ) from None

        if not isinstance(payload, dict):
            raise ConnectorResponseError(
                f"{self.name} API returned a non-object JSON payload"
            )

        return payload

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
                        "connector_total_results_detected",
                        extra={
                            "source": self.name,
                            "total_results": total,
                            "estimated_pages": api_max_pages,
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
        user_agent_email: str = "your_email@example.com",
        max_pages: int | None = None,
        min_interval_seconds: float | None = None,
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: float = 30.0,
        retry_total: int = 3,
        retry_backoff_factor: float = 0.5,
        retry_backoff_jitter: float = 0.1,
        session: requests.Session | None = None,
    ):
        super().__init__(
            max_pages=max_pages,
            min_interval_seconds=min_interval_seconds,
            connect_timeout_seconds=connect_timeout_seconds,
            read_timeout_seconds=read_timeout_seconds,
            retry_total=retry_total,
            retry_backoff_factor=retry_backoff_factor,
            retry_backoff_jitter=retry_backoff_jitter,
            session=session,
        )

        self.base_url = "https://data.usajobs.gov/api/search"
        self.headers = {
            "Host": "data.usajobs.gov",
            "User-Agent": user_agent_email,
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
        search_result = data.get("SearchResult")
        if not isinstance(search_result, dict):
            raise ConnectorResponseError("USAJobs response is missing SearchResult")

        results = search_result.get("SearchResultItems")
        if not isinstance(results, list) or not all(
            isinstance(item, dict) for item in results
        ):
            raise ConnectorResponseError(
                "USAJobs response contains invalid SearchResultItems"
            )

        return results

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
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: float = 30.0,
        retry_total: int = 3,
        retry_backoff_factor: float = 0.5,
        retry_backoff_jitter: float = 0.1,
        session: requests.Session | None = None,
    ):
        super().__init__(
            max_pages=max_pages,
            min_interval_seconds=min_interval_seconds,
            connect_timeout_seconds=connect_timeout_seconds,
            read_timeout_seconds=read_timeout_seconds,
            retry_total=retry_total,
            retry_backoff_factor=retry_backoff_factor,
            retry_backoff_jitter=retry_backoff_jitter,
            session=session,
        )

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

    def _extract_results(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        results = data.get("results", [])
        if not isinstance(results, list) or not all(
            isinstance(item, dict) for item in results
        ):
            raise ConnectorResponseError("Adzuna response contains invalid results")
        return results

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
    connectors: list[JobConnector] = []

    for connector_name in config.bronze.connectors:
        if connector_name == "adzuna":
            connectors.append(
                ADZunaConnector(
                    api_key=require_environment_variable("ADZUNA_API_KEY"),
                    app_id=require_environment_variable("ADZUNA_APP_ID"),
                    max_pages=config.bronze.max_pages,
                    min_interval_seconds=config.bronze.min_interval_seconds,
                    connect_timeout_seconds=config.bronze.connect_timeout_seconds,
                    read_timeout_seconds=config.bronze.read_timeout_seconds,
                    retry_total=config.bronze.retry_total,
                    retry_backoff_factor=config.bronze.retry_backoff_factor,
                    retry_backoff_jitter=config.bronze.retry_backoff_jitter,
                )
            )
        elif connector_name == "usajobs":
            connectors.append(
                USAJobConnector(
                    api_key=require_environment_variable("USAJOBS_API_KEY"),
                    user_agent_email=require_environment_variable("USAJOBS_EMAIL"),
                    max_pages=config.bronze.max_pages,
                    min_interval_seconds=config.bronze.min_interval_seconds,
                    connect_timeout_seconds=config.bronze.connect_timeout_seconds,
                    read_timeout_seconds=config.bronze.read_timeout_seconds,
                    retry_total=config.bronze.retry_total,
                    retry_backoff_factor=config.bronze.retry_backoff_factor,
                    retry_backoff_jitter=config.bronze.retry_backoff_jitter,
                )
            )

    return connectors
