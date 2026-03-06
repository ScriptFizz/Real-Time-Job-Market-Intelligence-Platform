from pathlib import Path
from datetime import date
from pyspark.sql import SparkSession
from typing import List

def list_partitions(dataset_path: Path, partition_col: str ="ingestion_date"):
    
    pattern=f"{partition_col}=*"
    partitions=[]
    for path in dataset_path.glob(pattern):
        value = path.name.split("=")[1]
        partitions.append(date.fromisoformat(value))
    return sorted(set(partitions))


def compute_unprocessed(available, processed):
    return sorted(set(available) - set(processed))


def read_partitions(spark: SparkSession, dataset_path: Path, partitions: List, partition_col: str = "ingestion_date"):
    
    if not partitions:
        return None
    
    paths = [
        f"{dataset_path}/{partition_col}={p}"
        for p in partitions
    ]
    
    return spark.read.parquet(paths)
