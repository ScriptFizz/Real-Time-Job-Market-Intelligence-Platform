from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from job_plat.ingestion.connectors import (
    ADZunaConnector,
    USAJobConnector,
    build_connectors,
    require_environment_variable,
)
from job_plat.ingestion.job_schema import CanonicalJobV1


def test_canonical_job_allows_omitted_nullable_fields():
    job = CanonicalJobV1(
        source="adzuna",
        source_job_id="job-123",
    )

    assert job.job_title_raw is None
    assert job.contract_type_raw is None
    assert job.salary_min_raw is None


def test_canonical_job_requires_nonempty_source_job_id():
    with pytest.raises(ValidationError):
        CanonicalJobV1(
            source="adzuna",
            source_job_id="",
        )


def test_adzuna_normalization():
    connector = ADZunaConnector(
        api_key="test-key",
        app_id="test-app",
    )

    raw_job = {
        "id": "adzuna-123",
        "title": "Data Engineer",
        "company": {"display_name": "Example Ltd"},
        "location": {"display_name": "Rome"},
        "description": "Build data pipelines",
        "redirect_url": "https://example.com/jobs/123",
        "contract_time": "full_time",
        "contract_type": "permanent",
        "salary_min": 40_000,
        "salary_max": 55_000,
        "created": "2026-08-19T10:00:00Z",
    }

    result = connector.normalize(raw_job)

    assert result.source == "adzuna"
    assert result.source_job_id == "adzuna-123"
    assert result.job_title_raw == "Data Engineer"
    assert result.salary_min_raw == 40_000
    assert result.contract_type_raw == "permanent"


def test_usajobs_normalization():
    connector = USAJobConnector(api_key="test-key")

    raw_job = {
        "MatchedObjectDescriptor": {
            "PositionID": "USA-123",
            "PositionTitle": "Data Engineer",
            "OrganizationName": "Example Agency",
            "PositionLocationDisplay": "Washington, DC",
            "PositionURI": "https://www.usajobs.gov/job/123",
            "PublicationStartDate": "2026-08-19T00:00:00Z",
            "PositionSchedule": [{"Name": "Full-time"}],
            "PositionOfferingType": [{"Name": "Permanent"}],
            "PositionRemuneration": [
                {
                    "MinimumRange": "80000",
                    "MaximumRange": "110000",
                }
            ],
            "UserArea": {
                "Details": {
                    "JobSummary": "Build federal data systems",
                }
            },
        }
    }

    result = connector.normalize(raw_job)

    assert result.source == "usajobs"
    assert result.source_job_id == "USA-123"
    assert result.url == "https://www.usajobs.gov/job/123"
    assert result.employment_type_raw == "Full-time"
    assert result.contract_type_raw == "Permanent"
    assert result.salary_min_raw == 80_000


def test_usajobs_normalization_handles_empty_optional_arrays():
    connector = USAJobConnector(api_key="test-key")

    raw_job = {
        "MatchedObjectDescriptor": {
            "PositionID": "USA-456",
            "PositionTitle": "Analyst",
            "OrganizationName": "Example Agency",
            "PositionLocationDisplay": "Remote",
            "PositionSchedule": [],
            "PositionOfferingType": [],
            "PositionRemuneration": [],
        }
    }

    result = connector.normalize(raw_job)

    assert result.employment_type_raw is None
    assert result.contract_type_raw is None
    assert result.salary_min_raw is None
    assert result.salary_max_raw is None


def test_adzuna_normalization_reject_missing_source_job_id():
    connector = ADZunaConnector(
        api_key="test-key",
        app_id="test-app",
    )

    with pytest.raises(ValueError, match="id"):
        connector.normalize(
            {
                "title": "Data Engineer",
            }
        )


def test_adzuna_normalization_rejects_blank_source_job_id():
    connector = ADZunaConnector(
        api_key="test-key",
        app_id="test-app",
    )

    with pytest.raises(ValueError, match="id"):
        connector.normalize({"id": "   "})


def test_required_environment_variable_rejects_missing_value(monkeypatch):
    monkeypatch.delenv("ADZUNA_API_KEY", raising=False)

    with pytest.raises(
        RuntimeError,
        match="ADZUNA_API_KEY",
    ):
        require_environment_variable("ADZUNA_API_KEY")


def test_required_environment_variable_returns_value(monkeypatch):
    monkeypatch.setenv("ADZUNA_API_KEY", "test-key")

    assert require_environment_variable("ADZUNA_API_KEY") == "test-key"


def test_schema_errors_are_counted_without_logging_raw_payload(caplog):
    connector = ADZunaConnector(
        api_key="test-key",
        app_id="test-app",
    )
    raw_job = {
        "title": "Missing identifier",
        "secret": "must-not-appear-in-logs",
    }

    result = connector.normalize_with_accounting(raw_job)

    assert result is None
    assert connector.schema_error_count == 1
    assert "must-not-appear-in-logs" not in caplog.text


def test_factory_builds_configured_usajobs_connector(monkeypatch):
    monkeypatch.setenv("USAJOBS_API_KEY", "federal-key")
    monkeypatch.setenv("USAJOBS_EMAIL", "jobs@example.com")
    monkeypatch.delenv("ADZUNA_API_KEY", raising=False)
    monkeypatch.delenv("ADZUNA_APP_ID", raising=False)
    config = SimpleNamespace(
        bronze=SimpleNamespace(
            connectors=["usajobs"],
            max_pages=2,
            min_interval_seconds=None,
            connect_timeout_seconds=1.0,
            read_timeout_seconds=4.0,
            retry_total=2,
            retry_backoff_factor=0.25,
            retry_backoff_jitter=0.05,
        )
    )

    connectors = build_connectors(config)

    assert len(connectors) == 1
    assert isinstance(connectors[0], USAJobConnector)
    assert connectors[0].headers["User-Agent"] == "jobs@example.com"
    assert connectors[0].headers["Authorization-Key"] == "federal-key"
