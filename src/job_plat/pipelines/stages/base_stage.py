from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import logging
import time


class BaseStage(ABC):
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def execute(self) -> None:
        start = time.time()
        
        self.logger.info("Starting stage execution")
        
        self.validate_inputs()
        inputs = self.read()
        outputs = self.transform(**inputs)
        self.validate_outputs(outputs)
        self.write(outputs)
        
        duration = time.time() - start
        self.logger.info(f"Stage completed in {duration: .2f} seconds")
        
    @abstractmethod
    def validate_inputs(self) -> None:
        pass
    
    @abstractmethod
    def write(self, outputs: dict) -> None:
        pass
    
    def validate_outputs(self, outputs: dict) -> None:
        for name, df in outputs.items():
            if df.rdd.isEmpty():
                raise ValueError(f"{name} is empty!")



