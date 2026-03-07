from dataclasses import dataclass
from pathlib import Path
from datetime import date
from typing import List, Literal, Optional
from pyspark.sql import SparkSession, DataFrame


@dataclass
class Dataset:
    name: str
    path: Path
    storage: Storage
    partition_column: str = "ingestion_date"
    write_mode: Literal["append", "overwrite"] = "append"
    
    def list_partitions(self) -> List[date]:
        pattern = f"{self.partition_column}=*"
        dirs = self.storage.list_dirs(path=self.path, pattern=pattern)
        partitions = []
        
        for p in dirs:
            value = p.name.split("=")[1]
            partitions.append(date.fromisoformat(value))
        
        return sorted(set(partitions))
    
    def read_partitions(self, spark: SparkSession, partitions: List[date]) -> DataFrame:
        
        if not partitions:
            raise ValueError(f"No partitions to read for dataset {self.name}")
        
        paths = [
            str(f"{self.path}/{self.partition_column}={p}")
            for p in partitions
        ]
        
        return self.storage.read_parquet(spark=spark, paths=paths)
    
    
    def write(self, df: DataFrame, mode: Optional[str] = None) -> None:
        actual_mode = mode or self.write_mode
        self.storage.write_parquet(
            df=df,
            path=str(self.path),
            partition_cols=[self.partition_column],
            mode=actual_mode
        )

    def get_unprocessed_partitions(self, partition_manager: PartitionManager, stage_name: str) -> Lisrt[date]:
        available = self.list_partitions()
        processed = partition_manager.get_processed(stage_name=stage_name)
        return sorted(set(available) - set(processed))
    
    # def write(self, df: DataFrame, mode="append"):
        # (
        # df.write
        # .mode(mode)
        # .partitionBy(self.partition_column)
        # .parquet(self.path)
        # )

    # def get_unprocessed_partitions(self, partition_manager: PartitionManager, stage_name: str):
        # available = self.list_partitions()
        # processed = partition_manager.get_processed(stage_name=stage_name)
        # return sorted(set(available) - set(processed))
