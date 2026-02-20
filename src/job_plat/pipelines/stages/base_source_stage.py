from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import logging
import time
from job_plat.utils.storage import Storage


class BaseSourceStage(ABC):
    
    def __init__(self, storage: Storage):
        self.storage = storage
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def execute(self) -> None:
        start = time.time()
        self.logger.info("Starting source stage")
        
        self.validate_config()
        count = self.produce()
        
        duration = time.time() - start
        self.logger.info(f"Produced {count} records in {duration:.2f}s")
    
    @abstractmethod
    def validate_config(self) -> None:
        pass
    
    @abstractmethod
    def produce(self) -> int:
        pass
