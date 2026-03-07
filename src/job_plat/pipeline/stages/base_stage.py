from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import logging
import time
from job_plat.config.logconfig import ContextLogger
from job_plat.utils.storage import Storage
from job_plat.bronze.ingestion.metadata import StageExecutionContext
from typing import Any


class BaseStage(ABC):
    
    STAGE_NAME: str
    INPUT_MAP: dict[str, DatasetDef]
    OUTPUT_TYPE: type[StageOutput]
    
    def __init__(self, spark: SparkSession, datasets: DatasetRegistry, partition_manager: PartitionManager):
        self.spark = spark
        self.datasets = datasets
        self.partition_manager = partition_manager
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
        
        for param, dataset_cls in self.INPUT_MAP.items():
            ds = self.datasets.get(dataset_cls)
            
            partitions = ds.get_available_partitions(
                partition_manager=self.partition_manager,
                stage_name=self.STAGE_NAME
            )
            
            if partitions:
                df = ds.read_partitions(spark=self.spark, partitions=partitions)
            else:
                df = None
            
            inputs[param] = df
            self._input_partitions[param] = partitions
            
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
            if not ds.path.exists():
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
        for name, df in vars(ouputs).items():
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

########################### 06-03

# class BaseStage(ABC):
    
    # STAGE_NAME: str
    # INPUT_DATASETS: list[Dataset]
    # OUTPUT_DATASETS: list[Dataset]
    
    # def __init__(self, spark: SparkSession, datasets: DatasetRegistry, partition_manager: PartitionManager):
        # self.spark = spark
        # self.datasets = datasets
        # self.partition_manager = partition_manager
        # self._base_logger = logging.getLogger(
            # f"pipeline.{self.__module__}.{self.__class__.__name__}"
            # )
    
    # def execute(self) -> None:
        
        # self.validate_inputs()
        
        # run_context = self.create_context()
        # self.logger = ContextLogger(
            # self._base_logger,
            # {
                # "run_id": run_context.run_id,
                # "stage": run_context.stage,
            # },
        # )
        
        # start = time.time()
        
        # self.logger.info("stage_started")
        
        # try:
            # inputs = self.read()
            # outputs = self.transform(inputs=inputs)
            # self.validate_outputs(outputs)
            # metrics = self.compute_metrics(outputs=outputs)
            # if metrics:
                # self.logger.info(f"stage_metrics", extra={"stage": self.STAGE_NAME, **metrics})
                # self.evaluate_metrics(metrics=metrics)
            # self.write(inputs=inputs, outputs=outputs)
            
            # duration = round(time.time() - start, 2)
            # self.logger.info(
                # "stage_completed",
                # extra={"duration_seconds": duration},
            # )
        
        # except StageSkip as e:
            # self.logger.info("stage_skipped", extra={"reason": str(e)})
            
        # except Exception:
            # duration = round(time.time() - start, 2)
            
            # self.logger.error(
                # "stage_failed",
                # extra={"duration_seconds": duration},
                # exc_info=True
            # )
            # raise
    
    # def read(self) -> dict:
        # inputs = {}
        # for dataset_cls in self.INPUT_DATASETS:
            # ds = self.datasets.get(dataset_cls.name)
            
            # partitions = ds.get_available_partitions(
                # partition_manager=self.partition_manager,
                # stage_name=self.STAGE_NAME
            # )
            
            # if partitions:
                # df = ds.read_partitions(spark=self.spark, partitions=partitions)
            # else:
                # df = None
            
            # inputs[dataset_cls] = {
                # "df": df,
                # "partitions": partitions,
                # "dataset": ds
            # }
        # return inputs

    # def validate_inputs(self) -> None:
        # missing = []
        # for name in self.INPUT_DATASETS:
            # ds = self.datasets.get(name)
            # if not ds.path.exists():
                # missing.append(str(ds.path))
            
        # if missing:
            # raise FileNotFoundError(
                # f"Missing input dataset(s): {', '.join(missing)}"
            # )
    
    # def write(self, inputs: dict, outputs: dict) -> None:
        # if not outputs:
            # self.logger.info("No outputs to write", extra = {"stage": self.STAGE_NAME})
            # return
        
        # write_strategy = {}
        
        # for dataset_cls, df in outputs.items():
            
            # dataset = self.datasets.get(dataset_cls.name)
            # dataset.write(df)
            # write_strategy[name] = dataset.write_mode
        
        # self.logger.info("write_strategy", extra=write_strategy)
        
        # for data in inputs.values():
            
            # partitions = data["partitions"]
            
            # if partitions:
                # self.partition_manager.mark_processed(
                    # stage_name=self.STAGE_NAME,
                    # partitions=partitions
                # )
            
    
    # @abstractmethod
    # def transform(self, inputs: dict) -> dict:
        # pass
    
    # def _validate_output_names(self, outputs: dict[Dataset, DataFrame]):
        # expected = set(self.OUTPUT_DATASETS)
        # produced = set(outputs.keys())
        
        # if produced != expected:
            # raise ValueError(
                # f"{self.STAGE_NAME}: outputs mismatch.\n"
                # f"Expected: {expected}\n"
                # f"Produced: {produced}"
            # )
    
    # def validate_outputs(self, outputs: dict) -> None:
        # for name, df in outputs.items():
            # if df.limit(1).count() == 0:
                # raise ValueError(f"{name} is empty!")
    
    # def compute_metrics(self, outputs: dict) -> dict:
        # return {}
    
    # def evaluate_metrics(self, metrics: dict) -> None:
        # pass

    # @abstractmethod
    # def create_context(self) -> StageExecutionContext:
        # pass

########################### 05-03

# class BaseStage(ABC):
    
    # STAGE_NAME: str
    # INPUT_DATASETS: list[str]
    # OUTPUT_DATASETS: list[str]
    
    # def __init__(self, spark: SparkSession, datasets: DatasetRegistry, partition_manager: PartitionManager):
        # self.spark = spark
        # self.datasets = datasets
        # self.partition_manager = partition_manager
        # self._base_logger = logging.getLogger(
            # f"pipeline.{self.__module__}.{self.__class__.__name__}"
            # )
    
    # def execute(self) -> None:
        
        # self.validate_inputs()
        
        # run_context = self.create_context()
        # self.logger = ContextLogger(
            # self._base_logger,
            # {
                # "run_id": run_context.run_id,
                # "stage": run_context.stage,
            # },
        # )
        
        # start = time.time()
        
        # self.logger.info("stage_started")
        
        # try:
            # inputs = self.read()
            # outputs = self.transform(inputs=inputs)
            # self.validate_outputs(outputs)
            # metrics = self.compute_metrics(outputs=outputs)
            # if metrics:
                # self.logger.info(f"stage_metrics", extra={"stage": self.STAGE_NAME, **metrics})
                # self.evaluate_metrics(metrics=metrics)
            # self.write(inputs=inputs, outputs=outputs)
            
            # duration = round(time.time() - start, 2)
            # self.logger.info(
                # "stage_completed",
                # extra={"duration_seconds": duration},
            # )
        
        # except Exception:
            # duration = round(time.time() - start, 2)
            
            # self.logger.error(
                # "stage_failed",
                # extra={"duration_seconds": duration},
                # exc_info=True
            # )
            # raise
    
    # def read(self) -> dict:
        # inputs = {}
        # for name in self.INPUT_DATASETS:
            # ds = self.datasets.get(name)
            
            # partitions = ds.get_available_partitions(
                # partition_manager=self.partition_manager,
                # stage_name=self.STAGE_NAME
            # )
            
            # if partitions:
                # df = ds.read_partitions(spark=self.spark, partitions=partitions)
            # else:
                # df = None
            
            # inputs[name] = {
                # "df": df,
                # "partitions": partitions,
                # "dataset": ds
            # }
        # return inputs

    # def validate_inputs(self) -> None:
        # missing = []
        # for name in self.INPUT_DATASETS:
            # ds = self.datasets.get(name)
            # if not ds.path.exists():
                # missing.append(str(ds.path))
            
        # if missing:
            # raise FileNotFoundError(
                # f"Missing input dataset(s): {', '.join(missing)}"
            # )
    
    # def write(self, inputs: dict, outputs: dict) -> None:
        # if not outputs:
            # self.logger.info("No outputs to write", extra = {"stage": self.STAGE_NAME})
            # return
        
        # write_strategy = {}
        
        # for name, df in outputs.items():
            
            # dataset = self.datasets.get(name)
            # dataset.write(df)
            # write_strategy[name] = dataset.write_mode
        
        # self.logger.info("write_strategy", extra=write_strategy)
        
        # for data in inputs.values():
            
            # partitions = data["partitions"]
            
            # if partitions:
                # self.partition_manager.mark_processed(
                    # stage_name=self.STAGE_NAME,
                    # partitions=partitions
                # )
            
    
    # @abstractmethod
    # def transform(self, inputs: dict) -> dict:
        # pass
    
    # def _validate_output_names(self, outputs: dict[str, DataFrame]):
        # expected = set(self.OUTPUT_DATASETS)
        # produced = set(outputs.keys())
        
        # if produced != expected:
            # raise ValueError(
                # f"{self.STAGE_NAME}: outputs mismatch.\n"
                # f"Expected: {expected}\n"
                # f"Produced: {produced}"
            # )
    
    # def validate_outputs(self, outputs: dict) -> None:
        # for name, df in outputs.items():
            # if df.limit(1).count() == 0:
                # raise ValueError(f"{name} is empty!")
    
    # def compute_metrics(self, outputs: dict) -> dict:
        # return {}
    
    # def evaluate_metrics(self, metrics: dict) -> None:
        # pass

    # @abstractmethod
    # def create_context(self) -> StageExecutionContext:
        # pass


#########################

# class BaseStage(ABC):
    
    # def __init__(self, spark: SparkSession, datasets: DatasetRegistry, partition_manager: PartitionManager):
        # self.STAGE_NAME = ""
        # self.spark = spark
        # self.datasets = datasets
        # self.partition_manager = partition_manager
        # self._base_logger = logging.getLogger(
            # f"pipeline.{self.__module__}.{self.__class__.__name__}"
            # )
    
    # def execute(self) -> None:
        
        # self.validate_inputs()
        
        # run_context = self.create_context()
        # self.logger = ContextLogger(
            # self._base_logger,
            # {
                # "run_id": run_context.run_id,
                # "stage": run_context.stage,
            # },
        # )
        
        # start = time.time()
        
        # self.logger.info("stage_started")
        
        # try:
            # inputs = self.read()
            # outputs = self.transform(inputs=inputs)
            # self.validate_outputs(outputs)
            # metrics = self.compute_metrics(outputs=outputs)
            # if metrics:
                # self.logger.info(f"stage_metrics", extra={"stage": run_context.stage, **metrics})
                # self.evaluate_metrics(metrics=metrics)
            # self.write(inputs=inputs, outputs=outputs)
            
            # duration = round(time.time() - start, 2)
            # self.logger.info(
                # "stage_completed",
                # extra={"duration_seconds": duration},
            # )
        
        # except Exception:
            # duration = round(time.time() - start, 2)
            
            # self.logger.error(
                # "stage_failed",
                # extra={"duration_seconds": duration},
                # exc_info=True
            # )
            # raise
        
    # @abstractmethod
    # def validate_inputs(self) -> None:
        # pass
    
    # @abstractmethod
    # def write(self, inputs: dict, outputs: dict) -> None:
        # pass
    
    # @abstractmethod
    # def read(self) -> dict:
        # pass 
    
    # @abstractmethod
    # def transform(self, inputs: dict) -> dict:
        # pass
    
    # def validate_outputs(self, outputs: dict) -> None:
        # for name, df in outputs.items():
            # if df.limit(1).count() == 0:
                # raise ValueError(f"{name} is empty!")
    
    # def compute_metrics(self, outputs: dict) -> dict:
        # return {}
    
    # #@abstractmethod
    # def evaluate_metrics(self, metrics: dict) -> None:
        # pass

    # @abstractmethod
    # def create_context(self) -> StageExecutionContext:
        # pass

########################

# class BaseStage(ABC):
    
    # def __init__(self, spark: SparkSession, storage: Storage, partition_manager: PartitionManager):
        # self.PARTITION_COLUMN = "ingestion_date"
        # self.STAGE_NAME = ""
        # self.spark = spark
        # self.storage = storage
        # self.partition_manager = partition_manager
        # self._base_logger = logging.getLogger(
            # f"pipeline.{self.__module__}.{self.__class__.__name__}"
            # )
    
    # def get_available_partitions(self, dataset_path: Path, partition_col: str = self.PARTITION_COLUMN):
        # pattern=f"{partition_col}=*"
        # partitions=[]
        # for path in dataset_path.glob(pattern):
            # value = path.name.split("=")[1]
            # partitions.append(date.fromisoformat(value))
        # return sorted(set(partitions))
    
    # def get_unprocessed_partitions(self, dataset_path: Path):
        # available = self.get_available_partitions(dataset_path=dataset_path)
        # processed = self.partition_manager.get_processed(stage_name = self.STAGE_NAME)
        # return sorted(set(available) - set(processed))
    
    # def read_dataset_partitions(self, dataset_path: Path, partitions: List, partition_col: str = self.PARTITION_COLUMN) -> DataFrame:
    
        # if not partitions:
            # return None
        
        # paths = [
            # f"{dataset_path}/{partition_col}={p}"
            # for p in partitions
        # ]
        
        # return self.spark.read.parquet(paths)
    
    # def write_partitioned_dataset(self, df: DataFrame, path: Path, mode: str, partition_col: str = self.PARTITION_COLUMN) -> None:
        # df.write.mode(mode).partitionBy(partition_col).parquet(path)
    
    # def execute(self) -> None:
        
        # self.validate_inputs()
        
        # run_context = self.create_context()
        # self.logger = ContextLogger(
            # self._base_logger,
            # {
                # "run_id": run_context.run_id,
                # "stage": run_context.stage,
            # },
        # )
        
        # start = time.time()
        
        # self.logger.info("stage_started")
        
        # try:
            # inputs = self.read()
            # outputs = self.transform(inputs=inputs)
            # self.validate_outputs(outputs)
            # metrics = self.compute_metrics(outputs=outputs)
            # if metrics:
                # self.logger.info(f"stage_metrics", extra={"stage": run_context.stage, **metrics})
                # self.evaluate_metrics(metrics=metrics)
            # self.write(outputs)
            
            # duration = round(time.time() - start, 2)
            # self.logger.info(
                # "stage_completed",
                # extra={"duration_seconds": duration},
            # )
        
        # except Exception:
            # duration = round(time.time() - start, 2)
            
            # self.logger.error(
                # "stage_failed",
                # extra={"duration_seconds": duration},
                # exc_info=True
            # )
            # raise
        
    # @abstractmethod
    # def validate_inputs(self) -> None:
        # pass
    
    # @abstractmethod
    # def write(self, outputs: dict) -> None:
        # pass
    
    # @abstractmethod
    # def read(self) -> dict:
        # pass 
    
    # @abstractmethod
    # def transform(self, inputs: dict) -> dict:
        # pass
    
    # def validate_outputs(self, outputs: dict) -> None:
        # for name, df in outputs.items():
            # if df.limit(1).count() == 0:
                # raise ValueError(f"{name} is empty!")
    
    # def compute_metrics(self, outputs: dict) -> dict:
        # return {}
    
    # #@abstractmethod
    # def evaluate_metrics(self, metrics: dict) -> None:
        # pass

    # @abstractmethod
    # def create_context(self) -> StageExecutionContext:
        # pass
