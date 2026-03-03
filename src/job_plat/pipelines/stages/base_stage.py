from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import logging
import time
from job_plat.config.logconfig import ContextLogger
from job_plat.utils.storage import Storage
from job_plat.bronze.ingestion.metadata import StageExecutionContext


class BaseStage(ABC):
    
    def __init__(self, spark: SparkSession, storage: Storage):
        self.spark = spark
        self.storage = storage
        self._base_logger = logging.getLogger(
            f"pipeline.{self.__module__}.{self.__class__.__name__}"
            )
    
    def execute(self) -> None:
        
        self.validate_inputs()
        
        run_context = self.create_context()
        self.logger = ContextLogger(
            self._base_logger,
            {
                "run_id": run_context.run_id,
                "stage": run_context.stage,
            },
        )
        
        start = time.time()
        
        self.logger.info("stage_started")
        
        try:
            inputs = self.read()
            #outputs = self.transform(**inputs, logger=logger)
            outputs = self.transform(inputs=inputs)
            self.validate_outputs(outputs)
            metrics = self.compute_metrics(outputs=outputs)
            if metrics:
                self.logger.info(f"stage_metrics", extra={"stage": run_context.stage, **metrics})
                self.evaluate_metrics(metrics=metrics)
            self.write(outputs)
            
            duration = round(time.time() - start, 2)
            self.logger.info(
                "stage_completed",
                extra={"duration_seconds": duration},
            )
        
        except Exception:
            duration = round(time.time() - start, 2)
            
            self.logger.error(
                "stage_failed",
                extra={"duration_seconds": duration},
                exc_info=True
            )
            raise
        
    @abstractmethod
    def validate_inputs(self) -> None:
        pass
    
    @abstractmethod
    def write(self, outputs: dict) -> None:
        pass
    
    @abstractmethod
    def read(self) -> dict:
        pass 
    
    @abstractmethod
    def transform(self, inputs: dict) -> dict:
        pass
    
    def validate_outputs(self, outputs: dict) -> None:
        for name, df in outputs.items():
            #if df.rdd.isEmpty():
            if df.limit(1).count() == 0:
                raise ValueError(f"{name} is empty!")
    
    def compute_metrics(self, outputs: dict) -> dict:
        return {}
    
    @abstractmethod
    def evaluate_metrics(self, metrics: dict) -> None:
        pass

    @abstractmethod
    def create_context(self) -> StageExecutionContext:
        pass

