from datetime import UTC, datetime
from unittest.mock import MagicMock

from job_plat.context.contexts import BronzeContext
from job_plat.ingestion.metadata import IngestionRun
from job_plat.pipeline.stages.data.bronze_stage import BronzeStage


def test_bronze_produce_uses_storage_paths_and_logical_date():
    execution_date = datetime(2025, 3, 2, tzinfo=UTC)

    context = BronzeContext(
        execution_date=execution_date,
        root_path="gs://job-platform",
        query="data engineer",
        country="it",
        location="Rome",
    )

    connector = MagicMock()
    connector.name = "adzuna"
    connector.fetch.return_value = iter(
        [
            {
                "id": "source-job-1",
            }
        ]
    )
    connector.normalize.return_value.model_dump.return_value = {
        "source": "adzuna",
        "source_job_id": "source-job-1",
    }

    captured_records = []

    storage = MagicMock()

    def write_jsonl(records, path):
        captured_records.extend(records)
        assert path == (
            "gs://job-platform/bronze/jobs/"
            "ingestion_date=2025-03-02/"
            "source=adzuna/"
            "run_id=run-123/"
            "part-000.jsonl"
        )
        return len(captured_records)

    storage.write_jsonl.side_effect = write_jsonl

    stage = BronzeStage(
        bronze_ctx=context,
        storage=storage,
        connector=connector,
    )

    run = IngestionRun(
        run_id="run-123",
        source="adzuna",
        query="data engineer",
        country="it",
        location="Rome",
        execution_date=execution_date,
        started_at=datetime(2026, 8, 24, 10, tzinfo=UTC),
        pipeline_version="1.0.0",
    )

    row_count = stage.produce(
        run=run,
        logger=MagicMock(),
    )

    assert row_count == 1
    assert len(captured_records) == 1

    record_metadata = captured_records[0]["ingestion_metadata"]

    assert record_metadata["execution_date"] == "2025-03-02T00:00:00+00:00"
    assert record_metadata["ingestion_date"] == "2025-03-02"
    assert record_metadata["started_at"] == "2026-08-24T10:00:00+00:00"

    written_metadata_paths = {
        call.args[1] for call in storage.write_json.call_args_list
    }

    assert written_metadata_paths == {
        (
            "gs://job-platform/bronze/jobs/"
            "ingestion_date=2025-03-02/"
            "source=adzuna/"
            "run_id=run-123/"
            "_metadata.json"
        ),
        "gs://job-platform/_runs/run-123.json",
    }

    for call in storage.write_json.call_args_list:
        payload = call.args[0]

        assert payload["run_id"] == "run-123"
        assert payload["execution_date"] == "2025-03-02T00:00:00+00:00"
        assert payload["ingestion_date"] == "2025-03-02"
        assert payload["row_count"] == 1
