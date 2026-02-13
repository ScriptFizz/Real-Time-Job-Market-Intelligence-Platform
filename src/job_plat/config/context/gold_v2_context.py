from dataclasses import dataclass
from pathlib import Path
from datetime import date
from pyspark.sql import SparkSession

@dataclass
class GoldV2Context:
    data_date: date
    base_path: Path
    spark: SparkSession
    
    @property
    def job_cluster_membership_path(self) -> Path:
        return self.base_path / f"job_cluster_membership_{self.data_date}.parquet"
    
    @property
    def job_cluster_path(self) -> Path:
        return self.base_path / f"job_cluster_{self.data_date}.parquet"

    @property
    def cluster_metadata_path(self) -> Path:
        return self.base_path / f"cluster_metadata_{self.data_date}.parquet"
        
    @property
    def job_embeddings_path(self) -> Path:
        return self.base_path / f"job_embeddings_{self.data_date}.parquet"
        
    @property
    def skill_embeddings_path(self) -> Path:
        return self.base_path / f"skill_embeddings_{self.data_date}.parquet"
