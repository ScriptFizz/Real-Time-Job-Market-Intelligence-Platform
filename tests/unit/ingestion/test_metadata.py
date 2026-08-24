from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from job_plat.ingestion.metadata import (
    IngestionRun,
    build_ingestion_metadata,
    write_metadata,
)


def build_run() -> IngestionRun:
    return IngestionRun(
        run_id="run-123",
        source="adzuna",
        query="data engineer",
        country="it",
        location="Rome",
        execution_date=datetime(2025, 3, 2, tzinfo=UTC),
        started_at=datetime(2025, 3, 3, 10, tzinfo=UTC),
        pipeline_version="1.0.0",
    )


def test_build_ingestion_metadata_distinguishes_logical_and_physical_time():
    metadata = build_ingestion_metadata(
        run=build_run(),
        row_count=12,
    )

    assert metadata == {
        "run_id": "run-123",
        "source": "adzuna",
        "query": "data engineer",
        "country": "it",
        "location": "Rome",
        "execution_date": "2025-03-02T00:00:00+00:00",
        "ingestion_date": "2025-03-02",
        "started_at": "2025-03-03T10:00:00+00:00",
        "pipeline_version": "1.0.0",
        "row_count": 12,
    }


def test_build_ingestion_metadata_rejects_negative_count():
    with pytest.raises(
        ValueError,
        match="must not be negative",
    ):
        build_ingestion_metadata(
            run=build_run(),
            row_count=-1,
        )


def test_write_metadata_uses_storage_backend():
    storage = MagicMock()

    write_metadata(
        storage=storage,
        path="gs://job-platform/bronze/run-123",
        run=build_run(),
        row_count=12,
    )

    storage.write_json.assert_called_once_with(
        build_ingestion_metadata(
            run=build_run(),
            row_count=12,
        ),
        "gs://job-platform/bronze/run-123/_metadata.json",
    )
