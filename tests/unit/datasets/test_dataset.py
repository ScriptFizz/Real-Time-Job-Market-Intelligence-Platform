import pytest
from job_plat.storage.storages import LocalStorage
from job_plat.pipeline.datasets.dataset import Dataset


def test_list_partitions(spark, tmp_path):
    
    storage = LocalStorage()
    
    ds = Dataset(
        name="jobs",
        path=tmp_path,
        storage=storage,
        partition_columns=["ingestion_date"]
    )
    
    df = spark.createDataFrame(
        [(1, "2025-03-01"), (2, "2025-03-02")],
    )
    
    ds.write(df)
    
    partitions = ds.list_partitions()
    
    assert len(partitions) == 2
