from datetime import date

import pytest
from delta.tables import DeltaMergeBuilder, DeltaTable
from fixtures.storage_contract import assert_storage_discovery_contract

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


def test_local_storage_list_dirs_excludes_files(tmp_path):
    directory = tmp_path / "ingestion_date=2025-03-01"
    matching_file = tmp_path / "ingestion_date=not-a-directory"

    directory.mkdir()
    matching_file.write_text("data", encoding="utf-8")

    assert LocalStorage().list_dirs(
        path=str(tmp_path),
        pattern="ingestion_date=*",
    ) == [str(directory)]


def test_local_storage_list_dirs_is_sorted(tmp_path):
    later = tmp_path / "ingestion_date=2025-03-02"
    earlier = tmp_path / "ingestion_date=2025-03-01"

    later.mkdir()
    earlier.mkdir()

    assert LocalStorage().list_dirs(
        path=str(tmp_path),
        pattern="ingestion_date=*",
    ) == [
        str(earlier),
        str(later),
    ]


def test_local_storage_satisfies_discovery_contract(tmp_path):
    assert_storage_discovery_contract(
        storage=LocalStorage(),
        root=str(tmp_path / "bronze" / "jobs"),
    )


def test_local_storage_exists(tmp_path):
    storage = LocalStorage()
    dataset_path = tmp_path / "dataset"

    assert not storage.exists(str(dataset_path))

    dataset_path.mkdir()

    assert storage.exists(str(dataset_path))


def test_local_storage_json_round_trip(tmp_path):
    storage = LocalStorage()
    path = tmp_path / "state" / "metadata.json"

    assert storage.read_json(str(path)) is None

    storage.write_json(
        {
            "silver": [
                "2025-03-01",
                "2025-03-02",
            ]
        },
        str(path),
    )

    assert storage.read_json(str(path)) == {
        "silver": [
            "2025-03-01",
            "2025-03-02",
        ]
    }


def test_local_storage_rejects_non_object_json(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="Expected JSON object",
    ):
        LocalStorage().read_json(str(path))


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
        file_format="delta",
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
    latest_commit = DeltaTable.forPath(spark, dataset.path).history(1).first()

    assert latest_commit is not None
    assert latest_commit.operation == "MERGE"


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
        file_format="delta",
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


def test_delta_merge_deduplicates_incoming_keys_deterministically(
    spark,
    tmp_path,
):
    dataset = Dataset(
        name="job_dimension",
        path=str(tmp_path / "deduplicated-job-dimension"),
        storage=LocalStorage(),
        partition_columns=[],
        write_mode="merge",
        file_format="delta",
        merge_keys=("job_id",),
        merge_order_column="ingestion_date",
        merge_order="desc",
    )
    incoming = spark.createDataFrame(
        [
            ("job-1", "Alpha title", date(2025, 3, 1)),
            ("job-1", "Zulu title", date(2025, 3, 1)),
            ("job-2", "Other title", date(2025, 3, 1)),
        ],
        ["job_id", "job_title", "ingestion_date"],
    )

    dataset.write(incoming)
    dataset.write(incoming.repartition(2))

    rows = {row.job_id: row.job_title for row in dataset.read_all(spark).collect()}

    assert rows == {
        "job-1": "Zulu title",
        "job-2": "Other title",
    }


def test_failed_delta_merge_leaves_previous_version_readable(
    spark,
    tmp_path,
    monkeypatch,
):
    dataset = Dataset(
        name="job_dimension",
        path=str(tmp_path / "failed-job-dimension"),
        storage=LocalStorage(),
        partition_columns=[],
        write_mode="merge",
        file_format="delta",
        merge_keys=("job_id",),
        merge_order_column="ingestion_date",
    )
    original = spark.createDataFrame(
        [("job-1", "Original title", date(2025, 3, 1))],
        ["job_id", "job_title", "ingestion_date"],
    )
    update = spark.createDataFrame(
        [("job-1", "Updated title", date(2025, 3, 2))],
        ["job_id", "job_title", "ingestion_date"],
    )
    dataset.write(original)

    def fail_before_commit(_merge_builder):
        raise RuntimeError("injected merge failure")

    monkeypatch.setattr(DeltaMergeBuilder, "execute", fail_before_commit)

    with pytest.raises(RuntimeError, match="injected merge failure"):
        dataset.write(update)

    result = dataset.read_all(spark).first()
    assert result is not None
    assert result.job_title == "Original title"


def test_separate_key_updates_preserve_both_commits(spark, tmp_path):
    dataset = Dataset(
        name="job_dimension",
        path=str(tmp_path / "independent-key-updates"),
        storage=LocalStorage(),
        partition_columns=[],
        write_mode="merge",
        file_format="delta",
        merge_keys=("job_id",),
        merge_order_column="ingestion_date",
    )
    first = spark.createDataFrame(
        [("job-1", "First title", date(2025, 3, 1))],
        ["job_id", "job_title", "ingestion_date"],
    )
    second = spark.createDataFrame(
        [("job-2", "Second title", date(2025, 3, 1))],
        ["job_id", "job_title", "ingestion_date"],
    )

    dataset.write(first)
    dataset.write(second)

    rows = {row.job_id: row.job_title for row in dataset.read_all(spark).collect()}
    assert rows == {
        "job-1": "First title",
        "job-2": "Second title",
    }
