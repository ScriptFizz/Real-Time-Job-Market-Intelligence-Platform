from dataclasses import dataclass
from pathlib import Path
from datetime import date
from pyspark.sql import SparkSession

@dataclass
class GoldV1Context:
    data_date: date
    base_path: Path
    spark: SparkSession
    
    @property
    def dim_jobs_path(self) -> Path:
        return self.base_path / "dim_jobs"
    
    @property
    def dim_skills_path(self) -> Path:
        return self.base_path / "dim_skills"

    @property
    def fact_job_skill_path(self) -> Path:
        return self.base_path / "fact_job_skill"




# @dataclass
# class GoldV1Context:
    # data_date: date
    # base_path: Path
    # spark: SparkSession
    
    # @property
    # def dim_jobs_path(self) -> Path:
        # return self.base_path / f"dim_jobs_{self.data_date}.parquet"
    
    # @property
    # def dim_skills_path(self) -> Path:
        # return self.base_path / f"dim_skills_{self.data_date}.parquet"

    # @property
    # def fact_job_skill_path(self) -> Path:
        # return self.base_path / f"fact_job_skill_{self.data_date}.parquet"
