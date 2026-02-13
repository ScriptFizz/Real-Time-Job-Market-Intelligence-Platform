from dataclasses import dataclass
from pathlib import Path
from datetime import date
from pyspark.sql import SparkSession


@dataclass
class BronzeContext:
    data_date: date
    base_path: Path
    
    @property
    def raw_jobs_path(self) -> Path:
        return self.base_path / f"{self.data_date}.jsonl"
    
