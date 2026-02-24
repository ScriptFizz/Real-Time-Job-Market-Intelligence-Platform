from dataclasses import dataclass
from pathlib import Path
from datetime import date
from pyspark.sql import SparkSession
from typing import List

# BRONZE CONTEXT

@dataclass
class BronzeContext:
    data_date: date
    base_path: Path
    query: str,
    location: str
    domain: str
    
    @property
    def bronze_root(self) -> str:
        return str(self.base_path)
        

# SILVER CONTEXT

@dataclass
class SilverContext:
    data_date: date
    base_path: Path
    spark: SparkSession
    
    @property
    def jobs_path(self) -> Path:
        return self.base_path / "jobs"
    
    @property
    def job_skills_path(self) -> Path:
        return self.base_path / "job_skills"


# GOLD V1 CONTEXT

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
        

# GOLD V2 CONTEXT

@dataclass
class GoldV2Context:
    data_date: date
    base_path: Path
    spark: SparkSession
    
    @property
    def job_cluster_membership_path(self) -> Path:
        return self.base_path / f"job_cluster_membership"
    
    @property
    def job_cluster_path(self) -> Path:
        return self.base_path / f"job_cluster"

    @property
    def cluster_metadata_path(self) -> Path:
        return self.base_path / f"cluster_metadata"
        
    @property
    def job_embeddings_path(self) -> Path:
        return self.base_path / f"job_embeddings"
        
    @property
    def skill_embeddings_path(self) -> Path:
        return self.base_path / f"skill_embeddings"
        

# FULL PIPELINE CONTEXT

@dataclass
class PipelineContext:
    data_date: date
    env: str
    spark: SparkSession
    bronze: BronzeContext
    silver: SilverContext
    gold_v1: GoldV1Context
    gold_v2: GoldV2Context



# @dataclass
# class BronzeContext:
    # data_date: date
    # base_path: Path
    # query: str,
    # location: str
    
    # @property
    # def indeed_jobs_path(self) -> Path:
        # return self.base_path / "indeed" / f"{self.data_date}.jsonl"
    
    # @property
    # def linkedin_jobs_path(self) -> Path:
        # return self.base_path / "linkedin" / f"{self.data_date}.jsonl"
        
    # @property
    # def jobs_path_list(self) -> List[Path]:
        # return [
            # self.indeed_jobs_path,
            # self.linkedin_jobs_path
        # ]


# @dataclass
# class GoldV2Context:
    # data_date: date
    # base_path: Path
    # spark: SparkSession
    
    # @property
    # def job_cluster_membership_path(self) -> Path:
        # return self.base_path / f"job_cluster_membership_{self.data_date}.parquet"
    
    # @property
    # def job_cluster_path(self) -> Path:
        # return self.base_path / f"job_cluster_{self.data_date}.parquet"

    # @property
    # def cluster_metadata_path(self) -> Path:
        # return self.base_path / f"cluster_metadata_{self.data_date}.parquet"
        
    # @property
    # def job_embeddings_path(self) -> Path:
        # return self.base_path / f"job_embeddings_{self.data_date}.parquet"
        
    # @property
    # def skill_embeddings_path(self) -> Path:
        # return self.base_path / f"skill_embeddings_{self.data_date}.parquet"
