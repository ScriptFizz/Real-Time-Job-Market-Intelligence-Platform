from datetime import UTC, datetime

from job_plat.observability.run_manifest import RunManifestWriter
from job_plat.storage.paths import join_storage_path
from job_plat.storage.storages import LocalStorage


def test_run_manifest_records_operational_state(tmp_path):
    storage = LocalStorage()
    writer = RunManifestWriter(storage=storage, metadata_path=str(tmp_path))
    started_at = datetime(2025, 3, 2, tzinfo=UTC)

    writer.write(
        run_id="run-1",
        stage="silver",
        status="committed",
        started_at=started_at,
        finished_at=started_at,
        batch_id="batch-1",
        partitions=["2025-03-02"],
        inputs={"bronze_jobs": {"row_count": 10}},
        outputs={"silver_jobs": {"row_count": 8}},
        metrics={"rejected_record_count": 2, "dead_letter_count": 0},
    )

    path = join_storage_path(
        str(tmp_path),
        "run_manifests",
        "silver",
        "run-1.json",
    )
    payload = storage.read_json(path)

    assert payload is not None
    assert payload["status"] == "committed"
    assert payload["inputs"]["bronze_jobs"]["row_count"] == 10
    assert payload["outputs"]["silver_jobs"]["row_count"] == 8
    assert payload["metrics"]["rejected_record_count"] == 2
