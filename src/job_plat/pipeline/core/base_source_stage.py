import logging
import time
from abc import ABC, abstractmethod

from job_plat.config.logconfig import ContextLogger
from job_plat.ingestion.metadata import IngestionRun
from job_plat.storage.storages import Storage


class BaseSourceStage(ABC):
    def __init__(self, storage: Storage):
        self.storage = storage
        self._base_logger = logging.getLogger(
            f"pipeline.{self.__module__}.{self.__class__.__name__}"
        )

    def execute(self) -> None:
        self.validate_config()

        run_context = self.create_context()

        # Bind logger to execution
        logger = ContextLogger(
            self._base_logger,
            {
                "run_id": run_context.run_id,
                "stage": run_context.stage,
            },
        )

        start = time.time()
        logger.info("stage_started")

        try:
            count = self.produce(run=run_context, logger=logger)

            duration = round(time.time() - start, 2)
            logger.info(
                "stage_completed",
                extra={
                    "records_produced": count,
                    "duration_seconds": duration,
                },
            )
        except Exception:
            duration = round(time.time() - start, 2)
            logger.error(
                "stage_failed",
                extra={
                    "duration_seconds": duration,
                },
                exc_info=True,
            )
            raise

    @abstractmethod
    def validate_config(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def produce(self, run: IngestionRun, logger: ContextLogger) -> int:
        raise NotImplementedError

    @abstractmethod
    def create_context(self) -> IngestionRun:
        raise NotImplementedError
