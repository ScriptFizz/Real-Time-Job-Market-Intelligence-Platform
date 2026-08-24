import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Literal

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from job_plat.storage.delta_retry import run_with_delta_retry
from job_plat.storage.paths import join_storage_path

AttemptStatus = Literal["started", "committed", "failed"]
DEFAULT_LEASE_DURATION = timedelta(hours=1)


class BatchAdmissionError(RuntimeError):
    pass


class BatchAlreadyRunningError(BatchAdmissionError):
    pass


class BatchAlreadyCommittedError(BatchAdmissionError):
    pass


LEDGER_SCHEMA = StructType(
    [
        StructField("stage_name", StringType(), nullable=False),
        StructField("partition_date", DateType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
        StructField("attempt_id", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("started_at", TimestampType(), nullable=False),
        StructField("lease_expires_at", TimestampType(), nullable=False),
        StructField("committed_at", TimestampType(), nullable=True),
        StructField("failed_at", TimestampType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
    ]
)


@dataclass(frozen=True)
class ProcessingAttempt:
    stage_name: str
    partitions: tuple[date, ...]
    batch_id: str
    attempt_id: str


class ProcessingLedger:
    def __init__(
        self,
        spark: SparkSession,
        metadata_path: str,
        lease_duration: timedelta = DEFAULT_LEASE_DURATION,
    ):
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be greater than zero")

        self.spark = spark
        self.path = join_storage_path(metadata_path, "processing_ledger")
        self.lease_duration = lease_duration

    def get_committed_partitions(self, stage_name: str) -> set[date]:
        if not DeltaTable.isDeltaTable(self.spark, self.path):
            return set()

        rows = (
            self.spark.read.format("delta")
            .load(self.path)
            .filter((col("stage_name") == stage_name) & (col("status") == "committed"))
            .select("partition_date")
            .distinct()
            .collect()
        )
        return {row.partition_date for row in rows}

    def start_attempt(
        self,
        *,
        stage_name: str,
        partitions: Iterable[date],
        attempt_id: str,
        started_at: datetime | None = None,
    ) -> ProcessingAttempt | None:
        partition_batch = tuple(sorted(set(partitions)))
        if not partition_batch:
            return None

        batch_id = self.build_batch_id(stage_name, partition_batch)
        attempt = ProcessingAttempt(
            stage_name=stage_name,
            partitions=partition_batch,
            batch_id=batch_id,
            attempt_id=attempt_id,
        )
        timestamp = started_at or datetime.now(UTC)
        lease_expires_at = timestamp + self.lease_duration
        rows = [
            (
                stage_name,
                partition,
                batch_id,
                attempt_id,
                "started",
                timestamp,
                lease_expires_at,
                None,
                None,
                None,
            )
            for partition in partition_batch
        ]
        source = self.spark.createDataFrame(rows, schema=LEDGER_SCHEMA)

        if not DeltaTable.isDeltaTable(self.spark, self.path):
            try:
                source.write.format("delta").mode("errorifexists").save(self.path)
                return attempt
            except Exception:
                # Another writer may have initialized the ledger after our check.
                if not DeltaTable.isDeltaTable(self.spark, self.path):
                    raise

        run_with_delta_retry(lambda: self._admit_attempt(source))
        self._verify_admission(attempt)
        return attempt

    def _admit_attempt(self, source: DataFrame) -> None:
        target = DeltaTable.forPath(self.spark, self.path)
        (
            target.alias("target")
            .merge(source.alias("source"), self._batch_partition_condition())
            .whenMatchedUpdate(
                condition=(
                    "target.status = 'failed' OR "
                    "(target.status = 'started' AND "
                    "target.lease_expires_at <= source.started_at) OR "
                    "target.attempt_id = source.attempt_id"
                ),
                set={
                    "attempt_id": "source.attempt_id",
                    "status": "source.status",
                    "started_at": "source.started_at",
                    "lease_expires_at": "source.lease_expires_at",
                    "committed_at": "source.committed_at",
                    "failed_at": "source.failed_at",
                    "error_message": "source.error_message",
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    def _verify_admission(self, attempt: ProcessingAttempt) -> None:
        rows = (
            self.spark.read.format("delta")
            .load(self.path)
            .filter(
                (col("stage_name") == attempt.stage_name)
                & (col("batch_id") == attempt.batch_id)
            )
            .select("attempt_id", "status")
            .distinct()
            .collect()
        )

        if rows and all(
            row.attempt_id == attempt.attempt_id and row.status == "started"
            for row in rows
        ):
            return

        if any(row.status == "committed" for row in rows):
            raise BatchAlreadyCommittedError(
                f"Batch {attempt.batch_id} for stage {attempt.stage_name} "
                "is already committed"
            )

        raise BatchAlreadyRunningError(
            f"Batch {attempt.batch_id} for stage {attempt.stage_name} "
            "already has an active attempt"
        )

    def mark_committed(
        self,
        attempt: ProcessingAttempt,
        committed_at: datetime | None = None,
    ) -> None:
        self._finish_attempt(
            attempt=attempt,
            status="committed",
            finished_at=committed_at or datetime.now(UTC),
        )

    def mark_failed(
        self,
        attempt: ProcessingAttempt,
        error: BaseException,
        failed_at: datetime | None = None,
    ) -> None:
        self._finish_attempt(
            attempt=attempt,
            status="failed",
            finished_at=failed_at or datetime.now(UTC),
            error_message=str(error)[:2000],
        )

    def _finish_attempt(
        self,
        *,
        attempt: ProcessingAttempt,
        status: Literal["committed", "failed"],
        finished_at: datetime,
        error_message: str | None = None,
    ) -> None:
        if not DeltaTable.isDeltaTable(self.spark, self.path):
            raise RuntimeError("Cannot finish an attempt that was not started")

        committed_at = finished_at if status == "committed" else None
        failed_at = finished_at if status == "failed" else None
        rows = [
            (
                attempt.stage_name,
                partition,
                attempt.batch_id,
                attempt.attempt_id,
                status,
                finished_at,
                finished_at,
                committed_at,
                failed_at,
                error_message,
            )
            for partition in attempt.partitions
        ]
        source = self.spark.createDataFrame(rows, schema=LEDGER_SCHEMA)
        run_with_delta_retry(
            lambda: self._execute_finish_attempt(source=source, attempt=attempt)
        )

    def _execute_finish_attempt(
        self,
        *,
        source: DataFrame,
        attempt: ProcessingAttempt,
    ) -> None:
        target = DeltaTable.forPath(self.spark, self.path)
        (
            target.alias("target")
            .merge(
                source.alias("source"),
                self._batch_partition_condition()
                & (col("target.attempt_id") == attempt.attempt_id),
            )
            .whenMatchedUpdate(
                condition="target.status = 'started'",
                set={
                    "status": "source.status",
                    "committed_at": "source.committed_at",
                    "failed_at": "source.failed_at",
                    "error_message": "source.error_message",
                },
            )
            .execute()
        )

    @staticmethod
    def build_batch_id(stage_name: str, partitions: Iterable[date]) -> str:
        canonical_partitions = sorted(set(partitions))
        identity = ":".join(
            [stage_name, *(partition.isoformat() for partition in canonical_partitions)]
        )
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    @staticmethod
    def _batch_partition_condition() -> Column:
        return (
            (col("target.stage_name") == col("source.stage_name"))
            & (col("target.partition_date") == col("source.partition_date"))
            & (col("target.batch_id") == col("source.batch_id"))
        )
