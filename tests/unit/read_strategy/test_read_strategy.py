from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

from job_plat.pipeline.core.read_strategy import IncrementalReadStrategy, PartitionBatch
from job_plat.pipeline.stages.data.gold_stage import GoldStage


class FakeDataset:
    partition_columns = ["ingestion_date"]

    def __init__(self, available):
        self.available = available
        self.requested_partitions = None

    def list_partitions(self):
        return self.available

    def read_partitions(self, spark, partitions):
        del spark
        self.requested_partitions = partitions
        return MagicMock()


def test_incremental_strategy_reads_only_common_partitions():
    jobs = FakeDataset(
        [
            date(2025, 3, 1),
            date(2025, 3, 2),
        ]
    )

    skills = FakeDataset(
        [
            date(2025, 3, 1),
        ]
    )

    partition_manager = MagicMock()
    partition_manager.get_processed.return_value = set()

    stage = SimpleNamespace(
        spark=MagicMock(),
        partition_manager=partition_manager,
        STAGE_NAME="gold",
    )

    result = IncrementalReadStrategy().read(
        stage=stage,
        datasets={
            "jobs": jobs,
            "skills": skills,
        },
        execution_date=None,
    )

    assert result.batch.partitions == (date(2025, 3, 1),)

    assert jobs.requested_partitions == [
        date(2025, 3, 1),
    ]

    assert skills.requested_partitions == [
        date(2025, 3, 1),
    ]

    assert result.inputs["jobs"] is not None
    assert result.inputs["skills"] is not None


def test_incremental_strategy_excludes_processed_partitions():
    jobs = FakeDataset(
        [
            date(2025, 3, 1),
            date(2025, 3, 2),
        ]
    )
    skills = FakeDataset(
        [
            date(2025, 3, 1),
            date(2025, 3, 2),
        ]
    )

    partition_manager = MagicMock()
    partition_manager.get_processed.return_value = {date(2025, 3, 1)}

    stage = SimpleNamespace(
        spark=MagicMock(),
        partition_manager=partition_manager,
        STAGE_NAME="gold",
    )

    result = IncrementalReadStrategy().read(
        stage=stage,
        datasets={
            "jobs": jobs,
            "skills": skills,
        },
        execution_date=None,
    )

    assert result.batch.partitions == (date(2025, 3, 2),)


def test_incremental_strategy_waits_for_aligned_inputs():
    jobs = FakeDataset([date(2025, 3, 2)])
    skills = FakeDataset([date(2025, 3, 1)])

    partition_manager = MagicMock()
    partition_manager.get_processed.return_value = set()

    stage = SimpleNamespace(
        spark=MagicMock(),
        partition_manager=partition_manager,
        STAGE_NAME="gold",
    )

    result = IncrementalReadStrategy().read(
        stage=stage,
        datasets={
            "jobs": jobs,
            "skills": skills,
        },
        execution_date=None,
    )

    assert result.batch.is_empty
    assert result.inputs == {
        "jobs": None,
        "skills": None,
    }
    assert jobs.requested_partitions is None
    assert skills.requested_partitions is None


def test_acknowledge_marks_exact_batch():
    stage = object.__new__(GoldStage)
    stage.STAGE_NAME = "gold"
    stage.partition_manager = MagicMock()
    stage.logger = MagicMock()

    batch = PartitionBatch(
        partitions=(
            date(2025, 3, 1),
            date(2025, 3, 2),
        )
    )
    attempt = MagicMock()

    stage.acknowledge(batch, attempt)

    stage.partition_manager.mark_processed.assert_called_once_with(attempt)


def test_acknowledge_ignores_empty_batch():
    stage = object.__new__(GoldStage)
    stage.partition_manager = MagicMock()
    stage.logger = MagicMock()

    stage.acknowledge(PartitionBatch(partitions=()), None)

    stage.partition_manager.mark_processed.assert_not_called()
