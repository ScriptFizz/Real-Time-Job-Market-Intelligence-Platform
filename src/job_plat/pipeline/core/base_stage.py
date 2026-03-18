from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import logging
import time
from pathlib import Path
from job_plat.context.contexts import BaseContext
from job_plat.pipeline.core.read_strategy import ReadStrategy, IncrementalReadStrategy
from job_plat.pipeline.datasets.dataset_definitions import DatasetDef
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.config.logconfig import ContextLogger
from job_plat.storage.storages import Storage
from job_plat.ingestion.metadata import StageExecutionContext
from job_plat.schemas.output_schemas import StageOutput
from job_plat.partitioning.partition_manager import PartitionManager
from typing import Any
from job_plat.utils.helpers import StageSkip


class BaseStage(ABC):
    
    STAGE_NAME: str
    INPUT_MAP: dict[str, DatasetDef]
    OUTPUT_TYPE: type[StageOutput]
    READ_STRATEGY: ReadStrategy = IncrementalReadStrategy()
    
    def __init__(self, spark: SparkSession, datasets: DatasetRegistry, partition_manager: PartitionManager, ctx: BaseContext):
        self.spark = spark
        self.datasets = datasets
        self.partition_manager = partition_manager
        self.ctx = ctx
        self._base_logger = logging.getLogger(
            f"pipeline.{self.__module__}.{self.__class__.__name__}"
            )
    
    #----------------------------
    
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
            outputs = self.transform(**inputs)
            self.validate_outputs(outputs)
            metrics = self.compute_metrics(outputs=outputs)
            if metrics:
                self.logger.info(f"stage_metrics", extra={"stage": self.STAGE_NAME, **metrics})
                self.evaluate_metrics(metrics=metrics)
            self.write(inputs=inputs, outputs=outputs)
            
            duration = round(time.time() - start, 2)
            self.logger.info(
                "stage_completed",
                extra={"duration_seconds": duration},
            )
        
        except StageSkip as e:
            self.logger.info("stage_skipped", extra={"reason": str(e)})
            
        except Exception:
            duration = round(time.time() - start, 2)
            
            self.logger.error(
                "stage_failed",
                extra={"duration_seconds": duration},
                exc_info=True
            )
            raise
            
    #---------------------
    # READ
    #---------------------
    
    def read(self) -> dict:
        inputs = {}
        self._input_partitions = {}
        
        for name, dataset_cls in self.INPUT_MAP.items():
            ds = self.datasets.get(dataset_cls)
            
            df, partitions = self.READ_STRATEGY.read(
                stage=self,
                dataset=ds,
                input_name=name,
                execution_date = self.ctx.execution_date
            )
            
            inputs[name] = df
            self._input_partitions[name] = partitions
            
        return inputs
    
    #------------------------
    # WRITE
    #------------------------
    
    def write(self, inputs: dict, outputs: StageOutput) -> None:
        if not outputs:
            self.logger.info("No outputs to write", extra = {"stage": self.STAGE_NAME})
            return
        
        dataset_map= outputs.__class__.dataset_map()
        write_strategy = {}
        
        for field_name, df in vars(outputs).items():
            
            dataset_cls = dataset_map[field_name]
            dataset = self.datasets.get(dataset_cls)
            dataset.write(df)
            write_strategy[field_name] = dataset.write_mode
        
        self.logger.info("write_strategy", extra=write_strategy)
        
        all_partitions = set()
        for partitions in self._input_partitions.values():
            if partitions:
                all_partitions.update(partitions)
        
        if all_partitions:
                self.partition_manager.mark_processed(
                    stage_name=self.STAGE_NAME,
                    partitions=sorted(all_partitions)
                )
    
    #------------------------
    # VALIDATION IO
    #------------------------
    
    def validate_inputs(self) -> None:
        missing = []
        for dataset_cls in self.INPUT_MAP.values():
            ds = self.datasets.get(dataset_cls)
            if not Path(ds.path).exists():
                missing.append(str(ds.path))
            
        if missing:
            raise FileNotFoundError(
                f"Missing input dataset(s): {', '.join(missing)}"
            )
    
    def validate_outputs(self, outputs: StageOutput) -> None:
        if not isinstance(outputs, self.OUTPUT_TYPE):
            raise TypeError(
                f"{self.STAGE_NAME}: expected output {self.OUTPUT_TYPE.__name__}, "
                f"got {type(outputs).__name__}"
            )
        for name, df in vars(outputs).items():
            if df is None:
                raise ValueError(f"{self.STAGE_NAME}: output '{name}' is None")
            
            if df.limit(1).count() == 0:
                raise ValueError(f"{self.STAGE_NAME}: output '{name}' is empty")
    
    #--------------------
    # OPTIONAL METHODS
    #--------------------

    def compute_metrics(self, outputs: StageOutput) -> dict:
        return {}
    
    def evaluate_metrics(self, metrics: dict) -> None:
        pass
    
    #---------------------
    # REQUIRED METHODS
    #---------------------
    
    @abstractmethod
    def transform(self, **kwargs: Any) -> StageOutput:
        pass

    @abstractmethod
    def create_context(self) -> StageExecutionContext:
        pass

