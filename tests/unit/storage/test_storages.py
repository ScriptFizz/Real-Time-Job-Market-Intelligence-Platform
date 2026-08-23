from datetime import date

import pytest

from job_plat.pipeline.datasets.dataset import Dataset
from job_plat.storage import storages
from job_plat.storage.storages import LocalStorage


def test_gcs_storage_explains_missing_optional_dependency(monkeypatch):
    def missing_module(name):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(storages, "import_module", missing_module)

    with pytest.raises(
        RuntimeError,
        match="poetry install --with cloud",
    ):
        storages.GCStorage()


def test_local_storage_list_dirs_returns_materialized_strings(tmp_path):
    first = tmp_path / "ingestion_date=2025-03-01"
    second = tmp_path / "ingestion_date=2025-03-02"
    first.mkdir()
    second.mkdir()

    results = LocalStorage().list_dirs(
        path=str(tmp_path),
        pattern="ingestion_date=*",
    )

    assert results == [str(first), str(second)]
    assert all(isinstance(result, str) for result in results)


def test_local_storage_exists(tmp_path):
    storage = LocalStorage()
    dataset_path = tmp_path / "dataset"

    assert not storage.exists(str(dataset_path))

    dataset_path.mkdir()

    assert storage.exists(str(dataset_path))


def test_merge_is_retry_idempotent(
    spark,
    tmp_path,
):
    dataset = Dataset(
        name="job_dimension",
        path=str(tmp_path / "job-dimension"),
        storage=LocalStorage(),
        partition_columns=[],
        write_mode="merge",
        merge_keys=("job_id",),
        merge_order_column="ingestion_date",
        merge_order="desc",
    )

    first = spark.createDataFrame(
        [
            (
                "job-1",
                "Original title",
                date(2025, 3, 1),
            ),
            (
                "job-2",
                "Preserved title",
                date(2025, 3, 1),
            ),
        ],
        [
            "job_id",
            "job_title",
            "ingestion_date",
        ],
    )

    dataset.write(first)

    retry = spark.createDataFrame(
        [
            (
                "job-1",
                "Corrected title",
                date(2025, 3, 1),
            ),
        ],
        [
            "job_id",
            "job_title",
            "ingestion_date",
        ],
    )

    dataset.write(retry)

    rows = {row.job_id: row.job_title for row in dataset.read_all(spark).collect()}

    assert rows == {
        "job-1": "Corrected title",
        "job-2": "Preserved title",
    }


def test_merge_preserves_newer_record(
    spark,
    tmp_path,
):
    dataset = Dataset(
        name="job_dimension",
        path=str(tmp_path / "ordered-job-dimension"),
        storage=LocalStorage(),
        partition_columns=[],
        write_mode="merge",
        merge_keys=("job_id",),
        merge_order_column="ingestion_date",
        merge_order="desc",
    )

    newer = spark.createDataFrame(
        [
            (
                "job-1",
                "New title",
                date(2025, 3, 2),
            )
        ],
        [
            "job_id",
            "job_title",
            "ingestion_date",
        ],
    )

    older = spark.createDataFrame(
        [
            (
                "job-1",
                "Old title",
                date(2025, 3, 1),
            )
        ],
        [
            "job_id",
            "job_title",
            "ingestion_date",
        ],
    )

    dataset.write(newer)
    dataset.write(older)

    result = dataset.read_all(spark).first()

    assert result is not None
    assert result.job_title == "New title"
