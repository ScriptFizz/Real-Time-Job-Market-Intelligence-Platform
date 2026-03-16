from abc import ABC, abstractmethod
from typing import Dict, List
import os
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
import shutil
#from google.cloud import storage
from pyspark.sql import SparkSession, DataFrame

class Storage(ABC):
    
    @abstractmethod
    def read_parquet(self, spark: SparkSession, paths: List[str]) -> DataFrame:
        pass
    
    @abstractmethod
    def write_parquet(
        self,
        df: DataFrame,
        path: str,
        mode: str,
        partition_cols: list[str] | None = None
    ) -> None:
        pass 
    
    @abstractmethod
    def write_jsonl(
        self,
        records,
        path: str
    ) -> int:
        pass

    @abstractmethod
    def list_dirs(self, path: str | Path, pattern: str) -> List:
        pass

class LocalStorage(Storage):
    
    def read_parquet(self, spark: SparkSession, base_path: str, paths: List[str]) -> DataFrame:
        return spark.read.option("basePath", base_path).parquet(*paths)
    
    def write_parquet(self, df, path, mode, partition_cols=None):
        writer = df.write.mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(path)
    
    def read_jsonl(self, spark: SparkSession, base_path: str, paths: List[str]) -> DataFrame:
        return spark.read.option("basePath", base_path).option("multiLine", False).json(paths)
    
    def write_jsonl(self, records, path: str) -> int: 
        
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        count = 0
        with NamedTemporaryFile("w", delete=False, encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)
            
            for record in records:
                tmp.write(json.dumps(record) + "\n")
                count += 1
        
        shutil.move(str(tmp_path), str(path))
         
        return count
    
    def write_df_to_json(self, df, path, mode, partition_cols=None):
        writer = df.write.mode(mode).option("compression", "none")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.json(path)
    
    def list_dirs(self, path: str, pattern: str) -> List[Path]:
        return Path(path).glob(pattern)

###############
### GCS Storage
################

class GCStorage(Storage):
    
    def __init__(self):
        self.client = storage.Client()
        
    def read_parquet(self, spark: SparkSession, base_path: str, paths: List[str]) -> DataFrame:
        return spark.read.option("basePath", base_path).parquet(*paths)
    
    def write_parquet(self, df: DataFrame, path: str, mode: str, partition_cols=None) -> None:
        writer = df.write.mode(mode)
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.parquet(path)
    
    def read_jsonl(self, spark: SparkSession, base_path: str, paths: List[str]) -> DataFrame:
        
        return (
            spark.read
            .option("basePath", base_path)
            .option("multiLine", False)
            .json(paths)
        )

    def write_jsonl(self, records, path: str) -> int:
        
        if not path.startswith("gs://"):
            raise ValueError("GCStorage requires gs:// path")
            
        _, rest = path.split("gs://", 1)
        bucket_name, blob_path = rest.split("/", 1)
        
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        count = 0
        lines = []
        
        for record in records:
            lines.append(json.dumps(record))
            count += 1
        
        blob.upload_from_string("\n".join(lines))
        
        return count
    
    def list_dirs(self, path: str, pattern: str) -> List[str]:
        
        if not path.startswith("gs://"):
            raise ValueError("GCStorage requires gs:// path")
        
        _, rest = path.split("gs://", 1)
        bucket_name, prefix = rest.split("/", 1)
        
        bucket = self.client.bucket(bucket_name)
        
        blobs = self.client.list_blobs(bucket, prefix=prefix)
        
        results = []
        
        for blob in blobs:
            p = Path(blob.name)
            
            if p.match(pattern):
                results.append(p)
        
        return results



def get_storage(storage_type: str | None) -> Storage:
    
    #storage_config = config["storage"]["type"]
    if not storage_type:
        raise ValueError("Storage settings not configured.")
        
    if storage_type == "local":
        return LocalStorage()
    elif storage_type == "gcs":
        return GCStorage()
    else:
        raise ValueError(f"Type of storage {storage_type} is not recognized")
