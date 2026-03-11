from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import logging
import time
from typing import Any
from datetime import datetime, timedelta
from pyspark.sql.functions import col

class ReadStrategy(ABC):
    @abstractmethod
    def read(self, stage, dataset, input_name):
        pass


class IncrementalReadStrategy(ReadStrategy):
    
    def read(self, stage, dataset, input_name):
        
        if not dataset.partition_columns:
            df = dataset.read_partitions(spark=stage.spark)
            partitions = []
        
        else:
            partitions = dataset.get_available_partitions(
                partition_manager = stage.partition_manager,
                stage_name=stage.STAGE_NAME
            )
            
            if partitions:
                df = dataset.read_partitions(
                    spark=stage.spark,
                    partitions=partitions
                )
            else:
                df = None
        
        return df, partitions

class TimeWindowReadStrategy(ReadStrategy):
    
    def __init__(self, window_days: int):
        self.window_days = window_days
    
    def read(self, stage, dataset, input_name):
        
        df = stage.spark.read.parquet(str(dataset.path))
        
        if dataset.time_window_column is not None:
                
            window_end = datetime.utcnow()
            window_start = window_end - timedelta(days=self.window_days)

            df = df.filter(
                (col(dataset.time_window_column) >= window_start) &
                (col(dataset.time_window_column) < window_end)
            )
            
        return df, []

