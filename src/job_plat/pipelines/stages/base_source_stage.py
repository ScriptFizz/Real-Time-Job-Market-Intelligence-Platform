from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import logging
import time
from job_plat.utils.storage import Storage
from job_plat.config.logconfig import ContextLogger
from job_plat.bronze.ingestion.metadata import StageExecutionContext, IngestionRun
from typing import Dict


class BaseSourceStage(ABC):
    
    def __init__(self, storage: Storage):
        self.storage = storage
        self._base_logger = logging.getLogger(
            f"pipeline.{self.__module__}.{self.__class__.__name__}"
        )
        
    def execute(self) -> None:
        
        run_context = self.create_context()
        
        # Bind logger to execution
        logger = ContextLogger(
            self._base_logger,
            {
                "run_id": context.run_id,
                "stage": context.stage,
            },
        )
        
        start = time.time()
        logger.info("stage_started")
        
        try:
            self.validate_config()
            count = self.produce(
                run=run_context,
                logger=logger
                )

        
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
        pass
    
    @abstractmethod
    def _enrich_with_ingestion_metadata(self records: Dict, run: IngestionRun) -> int:
        pass
        
    @abstractmethod
    def produce(self, context: StageExecutionContext, logger: logging.Logger) -> int:
        pass
    
    @abstractmethod
    def create_context(self) -> IngestionRun:
        pass

# class BaseSourceStage(ABC):
    
    # def __init__(self, storage: Storage):
        # self.storage = storage
        # self.logger = logging.getLogger(self.__class__.__name__)
        
    # def execute(self) -> None:
        # start = time.time()
        # self.logger.info("Starting source stage")
        
        # self.validate_config()
        # count = self.produce()
        
        # duration = time.time() - start
        # self.logger.info(f"Produced {count} records in {duration:.2f}s")
    
    # @abstractmethod
    # def validate_config(self) -> None:
        # pass
    
    # @abstractmethod
    # def _enrich_with_ingestion_metadata(self records: Dict, run: IngestionRun) -> int:
        # pass
        
    # @abstractmethod
    # def produce(self) -> int:
        # pass
