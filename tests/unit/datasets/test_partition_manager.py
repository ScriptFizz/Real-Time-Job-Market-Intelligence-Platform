from datetime import date

from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.partitioning.state_store import (
    LocalStateStore,
    PartitionState,
    StateStore,
)


class InMemoryStateStore(StateStore):
    def __init__(self):
        self.state: PartitionState = {}

    def load(self) -> PartitionState:
        return {stage: list(partitions) for stage, partitions in self.state.items()}

    def save(self, state: PartitionState) -> None:
        self.state = {stage: list(partitions) for stage, partitions in state.items()}


def test_marks_processed_partitions():
    manager = PartitionManager(InMemoryStateStore())

    manager.mark_processed(
        stage_name="silver_jobs",
        partitions=[date(2025, 3, 1), date(2025, 3, 2)],
    )

    assert manager.get_processed("silver_jobs") == {
        date(2025, 3, 1),
        date(2025, 3, 2),
    }


def test_marking_same_partition_twice_is_idempotent():
    manager = PartitionManager(InMemoryStateStore())
    partition = date(2025, 3, 1)

    manager.mark_processed("silver_jobs", [partition])
    manager.mark_processed("silver_jobs", [partition])

    assert manager.get_processed("silver_jobs") == {partition}


def test_local_state_survives_manager_recreation(tmp_path):
    metadata_path = str(tmp_path)

    first_manager = PartitionManager(LocalStateStore(metadata_path))
    first_manager.mark_processed(
        "silver_jobs",
        [date(2025, 3, 1)],
    )

    second_manager = PartitionManager(LocalStateStore(metadata_path))

    assert second_manager.get_processed("silver_jobs") == {date(2025, 3, 1)}
