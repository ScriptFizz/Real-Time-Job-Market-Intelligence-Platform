import pytest
from pydantic import ValidationError

from job_plat.ingestion.connectors import ADZunaConnector, USAJobConnector
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
