from datetime import date

from job_plat.pipeline.datasets.dataset import Dataset
from job_plat.storage.storages import LocalStorage


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
