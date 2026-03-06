from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import logging
import time
from job_plat.config.logconfig import ContextLogger
from job_plat.utils.storage import Storage
from job_plat.bronze.ingestion.metadata import StageExecutionContext


# class BaseStage(ABC):

    # PARTITION_COLUMN = "ingestion_date"

    # def get_available_partitions(self, dataset_path):

        # return list_partitions(dataset_path, self.PARTITION_COLUMN)

    # def get_unprocessed_partitions(self, dataset_path):

        # available = self.get_available_partitions(dataset_path)

        # processed = self.partition_manager.get_processed(self.stage_name)

        # return compute_unprocessed(available, processed)

    # def read_dataset_partitions(self, dataset_path, partitions):

        # return read_partitions(
            # self.spark,
            # dataset_path,
            # partitions,
            # self.PARTITION_COLUMN
        # )

    # def write_partitioned_dataset(self, df, path):

        # write_partitioned(df, path, self.PARTITION_COLUMN)


class BaseStage(ABC):
    
    def __init__(self, spark: SparkSession, storage: Storage, datasets: Datasets, partition_manager: PartitionManager):
        self.STAGE_NAME = ""
        self.spark = spark
        self.storage = storage
        self.datasets = datasets
        self.partition_manager = partition_manager
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
            if df.limit(1).count() == 0:
                raise ValueError(f"{name} is empty!")
    
    def compute_metrics(self, outputs: dict) -> dict:
        return {}
    
    #@abstractmethod
    def evaluate_metrics(self, metrics: dict) -> None:
        pass

    @abstractmethod
    def create_context(self) -> StageExecutionContext:
        pass




class BaseStage(ABC):
    
    def __init__(self, spark: SparkSession, storage: Storage, partition_manager: PartitionManager):
        self.PARTITION_COLUMN = "ingestion_date"
        self.STAGE_NAME = ""
        self.spark = spark
        self.storage = storage
        self.partition_manager = partition_manager
        self._base_logger = logging.getLogger(
            f"pipeline.{self.__module__}.{self.__class__.__name__}"
            )
    
    def get_available_partitions(self, dataset_path: Path, partition_col: str = self.PARTITION_COLUMN):
        pattern=f"{partition_col}=*"
        partitions=[]
        for path in dataset_path.glob(pattern):
            value = path.name.split("=")[1]
            partitions.append(date.fromisoformat(value))
        return sorted(set(partitions))
    
    def get_unprocessed_partitions(self, dataset_path: Path):
        available = self.get_available_partitions(dataset_path=dataset_path)
        processed = self.partition_manager.get_processed(stage_name = self.STAGE_NAME)
        return sorted(set(available) - set(processed))
    
    def read_dataset_partitions(self, dataset_path: Path, partitions: List, partition_col: str = self.PARTITION_COLUMN) -> DataFrame:
    
        if not partitions:
            return None
        
        paths = [
            f"{dataset_path}/{partition_col}={p}"
            for p in partitions
        ]
        
        return self.spark.read.parquet(paths)
    
    def write_partitioned_dataset(self, df: DataFrame, path: Path, mode: str, partition_col: str = self.PARTITION_COLUMN) -> None:
        df.write.mode(mode).partitionBy(partition_col).parquet(path)
    
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
            if df.limit(1).count() == 0:
                raise ValueError(f"{name} is empty!")
    
    def compute_metrics(self, outputs: dict) -> dict:
        return {}
    
    #@abstractmethod
    def evaluate_metrics(self, metrics: dict) -> None:
        pass

    @abstractmethod
    def create_context(self) -> StageExecutionContext:
        pass
