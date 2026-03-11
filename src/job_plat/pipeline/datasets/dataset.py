from dataclasses import dataclass, field
from pathlib import Path
from datetime import date
from typing import List, Literal, Optional
from pyspark.sql import SparkSession, DataFrame
from job_plat.storage.storages import Storage
from job_plat.partitioning.partition_manager import PartitionManager


@dataclass
class Dataset:
    name: str
    path: Path
    storage: Storage
    partition_columns: List[str]  = field(default_factory=lambda: ["ingestion_date"])
    time_window_column: str | None  = field(default_factory=lambda: None)
    write_mode: Literal["append", "overwrite"] = "append"
    file_format: Literal["parquet", "jsonl"] = "parquet"
    
    def list_partitions(self) -> List[date]:
        
        if not self.partition_columns:
            return []
        
        partition_col = self.partition_columns[0]
        
        pattern = f"{partition_col}=*"
        dirs = self.storage.list_dirs(path=self.path, pattern=pattern)
        partitions = []
        
        for p in dirs:
            value = p.name.split("=")[1]
            partitions.append(date.fromisoformat(value))
        
        return sorted(set(partitions))
    
    def read_partitions(
        self,
        spark: SparkSession,
        partitions: List[date] | None = None,
        filters: List[date] | None = None,
    ) -> DataFrame:
        
        if not self.partition_columns:
            if self.file_format == "parquet":
                return spark.read.parquet(str(self.path))
            else:
                return spark.read.option("multiline", False).json(str(self.path))
                
        partition_col = self.partition_columns[0]
        
        if not partitions and not filters:
            raise ValueError(f"No partitions to read for dataset {self.name}")
    
        base_path = str(self.path)
    
    
        if self.file_format == "parquet":
            spark_reader = spark.read.parquet
            storage_reader = self.storage.read_parquet
    
        elif self.file_format == "jsonl":
            spark_reader = spark.read.option("multiline", False).json
            storage_reader = self.storage.read_jsonl
    
        else:
            raise ValueError(f"Unsupported format {self.file_format}")
    
        if partitions:
    
            paths = [
                f"{base_path}/{partition_col}={p}"
                for p in partitions
            ]
    
            return storage_reader(
                spark=spark,
                base_path=base_path,
                paths=paths
            )
    
        df = spark_reader(base_path)
    
        df = df.filter(
            col(partition_col).isin(filters)
        )
    
        return df
    
    
    def write(self, df: DataFrame, mode: Optional[str] = None) -> None:
        actual_mode = mode or self.write_mode
        
        partition_cols = self.partition_columns if self.partition_columns else None
        
        self.storage.write_parquet(
            df=df,
            path=str(self.path),
            partition_cols=partition_cols,
            mode=actual_mode
        )

    def get_available_partitions(self, partition_manager: PartitionManager, stage_name: str) -> List[date]:
        available = self.list_partitions()
        processed = partition_manager.get_processed(stage_name=stage_name)
        return sorted(set(available) - set(processed))
    
