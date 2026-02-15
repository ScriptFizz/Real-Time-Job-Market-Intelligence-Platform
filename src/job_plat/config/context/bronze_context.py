from dataclasses import dataclass
from pathlib import Path
from datetime import date
from pyspark.sql import SparkSession
from typing import List


@dataclass
class BronzeContext:
    data_date: date
    base_path: Path
    
    @property
    def indeed_jobs_path(self) -> Path:
        return self.base_path / "indeed" / f"{self.data_date}.jsonl"
    
    @property
    def linkedin_jobs_path(self) -> Path:
        return self.base_path / "linkedin" / f"{self.data_date}.jsonl"
        
    @property
    def jobs_path_list(self) -> List[Path]:
        return [
            self.indeed_jobs_path,
            self.linkedin_jobs_path
        ]
    
    
