import pytest
from datetime import date
from types import SimpleNamespace
from job_plat.pipeline.core.read_strategy import IncrementalReadStrategy

class FakeDataset:
    partition_columns = ["ingestion_date"]
    
    def get_available_partitions(self, partition_manager, stage_name):
        return [date(2025, 3, 1)]
    
    def read_partitions(self, spark, partitions):
        return spark.createDataFrame(
            [(1, "2025-03-01")],
            ["job_id", "ingestion_date"]
        )


def test_incremental_read_strategy(spark):
    
    dataset = FakeDataset()
    
    strategy = IncrementalReadStrategy()
    
    stage = SimpleNamespace(
        spark=spark,
        partition_manager=None,
        STAGE_NAME="test_stage"
    )
    
    df, partitions = strategy.read(stage, dataset, "jobs")
    
    assert df.count() == 1
