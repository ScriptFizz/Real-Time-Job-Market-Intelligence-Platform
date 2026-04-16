import json
from pathlib import Path
from datetime import date
from typing import List
from pyspark.sql import SparkSession, DataFrame

class PartitionManager:
    
    def __init__(self, spark: SparkSession, metadata_path: str):
        self.spark = spark
        self.metadata_path = metadata_path
        self.metadata_filepath = str(Path(self.metadata_path) / "partitions_metadata.parquet")
        
        #if not self.metadata_filepath.exists():
        #    self.metadata_filepath.write_text("{}")
    
    def _load(self) -> DataFrame | None:
        try:
            return self.spark.read.parquet(self.metadata_filepath)
        except:
            return None
    

    def _save(self, df: DataFrame, mode: str = "append", partition_column: str = "stage_name") -> None:
        df.write.mode(mode).partitionBy(partition_column).parquet(self.metadata_filepath)
        return

    def get_processed(self, stage_name: str) -> set[date]:
        df = self._load()
        if df is None:
            return set()
            
        rows = (
            df.filter(col("stage_name") == stage_name)
                .select("partition_date")
                .distinct()
                .collect()
        )
        
        return {r["partition_date"] for r in rows}
    
    def mark_processed(self, stage_name: str, partitions: List) -> None:
        
        if not partitions:
            return
        
        data = [(stage_name, p) for p in partitions]
        
        df = self.spark.createDataFrame(
            data, ["stage_name", "partition_date"]
        )
        
        self._save(df)




# class PartitionManager:
    
    # def __init__(self, metadata_path: str | Path):
        # self.metadata_path = Path(metadata_path)
        # self.metadata_filepath = self.metadata_path / "partitions_metadata.json"
        
        # if not self.metadata_filepath.exists():
            # self.metadata_filepath.write_text("{}")
    
    # def _load(self) -> dict:
        # return json.loads(self.metadata_filepath.read_text())
    
    # def _save(self, state: dict) -> None:
        # self.metadata_filepath.write_text(json.dumps(state, indent=2))

    # def get_processed(self, stage_name: str) -> dict:
        
        # state = self._load()
        # values = state.get(stage_name, [])
        # return {date.fromisoformat(v) for v in values}
    
    # def mark_processed(self, stage_name: str, partitions: List) -> None:
        
        # state = self._load()
        # existing = set(state.get(stage_name, []))
        # new_values = {p.isoformat() for p in partitions}
        # state[stage_name] = sorted(existing.union(new_values))
        # self._save(state) 
