import logging
import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from job_plat.config.logconfig import ContextLogger
from job_plat.ingestion.metadata import IngestionRun
from job_plat.observability.run_manifest import RunManifestWriter
from job_plat.storage.storages import Storage


class BaseSourceStage(ABC):
    def __init__(self, storage: Storage, metadata_path: str):
        self.storage = storage
        self.manifest_writer = RunManifestWriter(
            storage=storage,
            metadata_path=metadata_path,
        )
        self._base_logger = logging.getLogger(
            f"pipeline.{self.__module__}.{self.__class__.__name__}"
        )

    def execute(self) -> None:
        self.validate_config()

        run_context = self.create_context()
        partition = run_context.execution_date.date().isoformat()
        batch_id = sha256(
            f"{run_context.stage}:{run_context.source}:{partition}".encode()
        ).hexdigest()

        # Bind logger to execution
        logger = ContextLogger(
            self._base_logger,
            {
                "run_id": run_context.run_id,
                "stage": run_context.stage,
                "batch_id": batch_id,
                "partition_batch": [partition],
                "model_id": None,
                "model_version": None,
            },
        )

        start = time.time()
        logger.info("stage_started")
        self.manifest_writer.write(
            run_id=run_context.run_id,
            stage=run_context.stage,
            status="started",
            started_at=run_context.started_at,
            batch_id=batch_id,
            partitions=[partition],
            inputs={
                "connector": {
                    "source": run_context.source,
                    "query": run_context.query,
                    "country": run_context.country,
                    "location": run_context.location,
                }
            },
        )

        try:
            count = self.produce(run=run_context, logger=logger)
            metrics = {
                "rejected_record_count": 0,
                "dead_letter_count": 0,
                **self.observability_metrics(),
            }

            duration = round(time.time() - start, 2)
            logger.info(
                "stage_completed",
                extra={
                    "records_produced": count,
                    "duration_seconds": duration,
                    **metrics,
                },
            )
            self.manifest_writer.write(
                run_id=run_context.run_id,
                stage=run_context.stage,
                status="committed",
                started_at=run_context.started_at,
                finished_at=datetime.now(UTC),
                batch_id=batch_id,
                partitions=[partition],
                inputs={"connector": {"source": run_context.source}},
                outputs={"bronze_jobs": {"row_count": count}},
                metrics=metrics,
            )
        except Exception as error:
            duration = round(time.time() - start, 2)
            logger.error(
                "stage_failed",
                extra={
                    "duration_seconds": duration,
                },
                exc_info=True,
            )
            self.manifest_writer.write(
                run_id=run_context.run_id,
                stage=run_context.stage,
                status="failed",
                started_at=run_context.started_at,
                finished_at=datetime.now(UTC),
                batch_id=batch_id,
                partitions=[partition],
                inputs={"connector": {"source": run_context.source}},
                failure={
                    "type": type(error).__name__,
                    "message": str(error)[:2000],
                },
            )
            raise

    def observability_metrics(self) -> dict[str, Any]:
        return {}

    @abstractmethod
    def validate_config(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def produce(self, run: IngestionRun, logger: ContextLogger) -> int:
        raise NotImplementedError

    @abstractmethod
    def create_context(self) -> IngestionRun:
        raise NotImplementedError
