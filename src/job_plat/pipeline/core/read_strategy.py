from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import logging
import time
from typing import Any
from datetime import datetime, timedelta


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
        
        df = stage.spark.read.parquet(dataset.path)
        
        if dataset.time_window_column is not None:
                
            window_end = datetime.utcnow()
            window_start = window_end - timedelta(days=self.window_days)

            df = df.filter(
                (col(dataset.time_window_column) >= window_start) &
                (col(dataset.time_window_column) < window_end)
            )
            
        return df, []


# class TimeWindowReadStrategy(ReadStrategy):
    
    # def __init__(self, window_days: int):
        # self.window_days = window_days
    
    # def read(self, stage, dataset, input_name):
        
        # if dataset.time_window_column is None:
            # raise ValueError(
                # f"{dataset.name} does not support time window reads"
            # )
        
        # window_end = datetime.utcnow()
        # window_start = window_end - timedelta(days=self.window_days)
        
        # df = stage.spark.read.parquet(dataset.path)
        
        # df = df.filter(
            # (col(dataset.time_window_column) >= window_start) &
            # (col(dataset.time_window_column) < window_end)
        # )
        
        # return df, []
