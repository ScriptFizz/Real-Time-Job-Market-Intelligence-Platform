from datetime import date

import pytest

from job_plat.pipeline.datasets.dataset import Dataset
from job_plat.storage.storages import LocalStorage


def test_delta_dataset_round_trip_creates_transaction_log(spark, tmp_path):
    table_path = tmp_path / "delta-jobs"
    dataset = Dataset(
        name="delta_jobs",
        path=str(table_path),
        storage=LocalStorage(),
        partition_columns=[],
        file_format="delta",
    )
    source = spark.createDataFrame([(1, "data engineer")], ["job_id", "title"])

    dataset.write(source)

    assert dataset.read_all(spark).collect() == source.collect()
    assert (table_path / "_delta_log").is_dir()


def test_list_partitions(spark, tmp_path):
    storage = LocalStorage()

    ds = Dataset(
        name="jobs",
        path=str(tmp_path),
        storage=storage,
        partition_columns=["ingestion_date"],
    )

    df = spark.createDataFrame(
        [(1, "2025-03-01"), (2, "2025-03-02")], ["job_id", "ingestion_date"]
    )

    ds.write(df)

    partitions = ds.list_partitions()

    assert len(partitions) == 2


def test_read_partitions_with_filters(spark, tmp_path):
    dataset = Dataset(
        name="jobs",
        path=str(tmp_path),
        storage=LocalStorage(),
        partition_columns=["ingestion_date"],
    )

    source = spark.createDataFrame(
        [(1, date(2025, 3, 1)), (2, date(2025, 3, 2))],
        ["job_id", "ingestion_date"],
    )
    dataset.write(source)

    result = dataset.read_partitions(
        spark=spark,
        filters=[date(2025, 3, 2)],
    )

    assert [row.job_id for row in result.collect()] == [2]


def test_replace_partitions_is_retry_idempotent(
    spark,
    tmp_path,
):
    first = date(2025, 3, 1)
    second = date(2025, 3, 2)

    dataset = Dataset(
        name="jobs",
        path=str(tmp_path / "replace-partitions"),
        storage=LocalStorage(),
        partition_columns=["ingestion_date"],
        write_mode="replace_partitions",
    )

    initial = spark.createDataFrame(
        [
            (1, "original", first),
            (2, "preserved", second),
        ],
        [
            "job_id",
            "value",
            "ingestion_date",
        ],
    )

    dataset.write(
        initial,
        expected_partitions=(first, second),
    )

    retry = spark.createDataFrame(
        [
            (1, "corrected", first),
        ],
        [
            "job_id",
            "value",
            "ingestion_date",
        ],
    )

    dataset.write(
        retry,
        expected_partitions=(first,),
    )

    result = dataset.read_all(spark)

    rows = {
        (
            row.job_id,
            row.value,
            row.ingestion_date,
        )
        for row in result.collect()
    }

    assert rows == {
        (1, "corrected", first),
        (2, "preserved", second),
    }


def test_replace_partitions_rejects_output_outside_batch(
    spark,
    tmp_path,
):
    expected = date(2025, 3, 1)
    unexpected = date(2025, 3, 2)

    dataset = Dataset(
        name="jobs",
        path=str(tmp_path / "protected-partitions"),
        storage=LocalStorage(),
        partition_columns=["ingestion_date"],
        write_mode="replace_partitions",
    )

    dataframe = spark.createDataFrame(
        [(1, unexpected)],
        ["job_id", "ingestion_date"],
    )

    with pytest.raises(
        ValueError,
        match="outside the active batch",
    ):
        dataset.write(
            dataframe,
            expected_partitions=(expected,),
        )
