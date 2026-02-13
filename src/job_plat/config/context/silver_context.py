from dataclasses import dataclass
from pathlib import Path
from datetime import date
from pyspark.sql import SparkSession


@dataclass
class SilverContext:
    data_date: date
    base_path: Path
    spark: SparkSession
    
    @property
    def jobs_path(self) -> Path:
        return self.base_path / "jobs" / f"data_date={self.data_date}.parquet"
    
    @property
    def job_skills_path(self) -> Path:
        return self.base_path / "job_skills" / f"data_date={self.data_date}.parquet"
    
