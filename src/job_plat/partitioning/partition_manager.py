from datetime import date, datetime

from job_plat.partitioning.processing_ledger import ProcessingAttempt, ProcessingLedger


class PartitionManager:
    def __init__(self, ledger: ProcessingLedger):
        self.ledger = ledger

    def get_processed(self, stage_name: str) -> set[date]:
        return self.ledger.get_committed_partitions(stage_name)

    def start_attempt(
        self,
        *,
        stage_name: str,
        partitions: tuple[date, ...],
        attempt_id: str,
        started_at: datetime,
    ) -> ProcessingAttempt | None:
        return self.ledger.start_attempt(
            stage_name=stage_name,
            partitions=partitions,
            attempt_id=attempt_id,
            started_at=started_at,
        )

    def mark_processed(self, attempt: ProcessingAttempt) -> None:
        self.ledger.mark_committed(attempt)

    def mark_failed(self, attempt: ProcessingAttempt, error: BaseException) -> None:
        self.ledger.mark_failed(attempt, error)
