from datetime import UTC, date, datetime, timedelta

import pytest

from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.partitioning.processing_ledger import (
    BatchAlreadyCommittedError,
    BatchAlreadyRunningError,
    ProcessingLedger,
)


def start_attempt(
    manager: PartitionManager,
    *,
    stage_name: str,
    partitions: tuple[date, ...],
    attempt_id: str,
):
    attempt = manager.start_attempt(
        stage_name=stage_name,
        partitions=partitions,
        attempt_id=attempt_id,
        started_at=datetime(2025, 3, 3, tzinfo=UTC),
    )
    assert attempt is not None
    return attempt


def test_committed_partitions_are_processed(spark, tmp_path):
    manager = PartitionManager(ProcessingLedger(spark, str(tmp_path)))
    partitions = (date(2025, 3, 1), date(2025, 3, 2))
    attempt = start_attempt(
        manager,
        stage_name="silver_jobs",
        partitions=partitions,
        attempt_id="attempt-1",
    )

    manager.mark_processed(attempt)

    assert manager.get_processed("silver_jobs") == set(partitions)


def test_failed_attempt_does_not_process_partition(spark, tmp_path):
    manager = PartitionManager(ProcessingLedger(spark, str(tmp_path)))
    attempt = start_attempt(
        manager,
        stage_name="silver_jobs",
        partitions=(date(2025, 3, 1),),
        attempt_id="attempt-1",
    )

    manager.mark_failed(attempt, RuntimeError("write failed"))

    assert manager.get_processed("silver_jobs") == set()


def test_terminal_attempt_cannot_be_downgraded(spark, tmp_path):
    manager = PartitionManager(ProcessingLedger(spark, str(tmp_path)))
    partition = date(2025, 3, 1)
    attempt = start_attempt(
        manager,
        stage_name="silver_jobs",
        partitions=(partition,),
        attempt_id="attempt-1",
    )

    manager.mark_processed(attempt)
    manager.mark_failed(attempt, RuntimeError("late failure"))

    assert manager.get_processed("silver_jobs") == {partition}


def test_independent_managers_do_not_lose_commits(spark, tmp_path):
    metadata_path = str(tmp_path)
    first_manager = PartitionManager(ProcessingLedger(spark, metadata_path))
    second_manager = PartitionManager(ProcessingLedger(spark, metadata_path))
    first_partition = date(2025, 3, 1)
    second_partition = date(2025, 3, 2)

    first_attempt = start_attempt(
        first_manager,
        stage_name="silver_jobs",
        partitions=(first_partition,),
        attempt_id="attempt-1",
    )
    second_attempt = start_attempt(
        second_manager,
        stage_name="silver_jobs",
        partitions=(second_partition,),
        attempt_id="attempt-2",
    )

    first_manager.mark_processed(first_attempt)
    second_manager.mark_processed(second_attempt)

    assert first_manager.get_processed("silver_jobs") == {
        first_partition,
        second_partition,
    }


def test_batch_identity_is_deterministic():
    first = date(2025, 3, 1)
    second = date(2025, 3, 2)

    assert ProcessingLedger.build_batch_id("silver", (first, second)) == (
        ProcessingLedger.build_batch_id("silver", (second, first, second))
    )
    assert ProcessingLedger.build_batch_id("silver", (first,)) != (
        ProcessingLedger.build_batch_id("gold", (first,))
    )


def test_second_active_attempt_for_batch_is_rejected(spark, tmp_path):
    manager = PartitionManager(ProcessingLedger(spark, str(tmp_path)))
    partitions = (date(2025, 3, 1),)
    start_attempt(
        manager,
        stage_name="silver",
        partitions=partitions,
        attempt_id="attempt-1",
    )

    with pytest.raises(BatchAlreadyRunningError, match="active attempt"):
        start_attempt(
            manager,
            stage_name="silver",
            partitions=partitions,
            attempt_id="attempt-2",
        )


def test_committed_batch_cannot_be_reopened(spark, tmp_path):
    manager = PartitionManager(ProcessingLedger(spark, str(tmp_path)))
    partitions = (date(2025, 3, 1),)
    attempt = start_attempt(
        manager,
        stage_name="silver",
        partitions=partitions,
        attempt_id="attempt-1",
    )
    manager.mark_processed(attempt)

    with pytest.raises(BatchAlreadyCommittedError, match="already committed"):
        start_attempt(
            manager,
            stage_name="silver",
            partitions=partitions,
            attempt_id="attempt-2",
        )


def test_expired_attempt_can_be_replaced(spark, tmp_path):
    ledger = ProcessingLedger(
        spark,
        str(tmp_path),
        lease_duration=timedelta(minutes=5),
    )
    partitions = (date(2025, 3, 1),)
    first = ledger.start_attempt(
        stage_name="silver",
        partitions=partitions,
        attempt_id="attempt-1",
        started_at=datetime(2025, 3, 3, 10, 0, tzinfo=UTC),
    )
    second = ledger.start_attempt(
        stage_name="silver",
        partitions=partitions,
        attempt_id="attempt-2",
        started_at=datetime(2025, 3, 3, 10, 6, tzinfo=UTC),
    )

    assert first is not None
    assert second is not None
    assert first.batch_id == second.batch_id
