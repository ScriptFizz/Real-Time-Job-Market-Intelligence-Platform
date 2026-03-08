from dataclasses import dataclass
from pathlib import Path
from datetime import date
from pyspark.sql import SparkSession
from typing import List, Optional

#  EXECUTION PARAMS

@dataclass
class ExecutionParams:
    query: str | None = None
    location: str | None = None

@dataclass
class BronzeContext:
    query: str | None = None
    location: str | None = None


# SILVER CONTEXT

@dataclass
class SilverContext:
    spark: SparkSession
    

# GOLD V1 CONTEXT

@dataclass
class GoldV1Context:
    fact_per_job_ratio_threshold: int
    spark: SparkSession
        

# GOLD V2 CONTEXT

@dataclass
class GoldV2Context:
    min_clusters: int
    min_silhouette: float
    spark: SparkSession
        

# FULL PIPELINE CONTEXT

@dataclass
class PipelineContext:
    env: str
    spark: SparkSession
    bronze: BronzeContext
    silver: SilverContext
    gold_v1: GoldV1Context
    gold_v2: GoldV2Context

####################### 06-03

# # DATE RANGE

# @dataclass
# class DateRange:
    # start_date: Optional[date] = None
    # end_date: Optional[date] = None
    
    # def is_full_load(self) -> bool:
        # return self.start_date is None and self.end_date is None
    
    # def is_single_day(self) -> bool:
        # return (
            # self.start_date is not None and
            # self.end_date is not None and
            # self.start_date == self.end_date
        # )


# #  EXECUTION PARAMS

# @dataclass
# class ExecutionParams:
    # query: str | None = None
    # location: str | None = None
    # date_range: DateRange | None = None

# # BRONZE CONTEXT

# @dataclass
# class BronzeContext:
    # base_path: Path
    # query: str | None = None
    # location: str | None = None
    
    # @property
    # def bronze_root(self) -> Path:
        # return self.base_path
        

# # SILVER CONTEXT

# @dataclass
# class SilverContext:
    # date_range: DateRange | None
    # base_path: Path
    # spark: SparkSession
    
    # @property
    # def jobs_path(self) -> Path:
        # return self.base_path / "jobs"
    
    # @property
    # def job_skills_path(self) -> Path:
        # return self.base_path / "job_skills"


# # GOLD V1 CONTEXT

# @dataclass
# class GoldV1Context:
    # date_range: DateRange | None
    # base_path: Path
    # fact_per_job_ratio_threshold: int
    # spark: SparkSession
    
    # @property
    # def dim_jobs_path(self) -> Path:
        # return self.base_path / "dim_jobs"
    
    # @property
    # def dim_skills_path(self) -> Path:
        # return self.base_path / "dim_skills"

    # @property
    # def fact_job_skill_path(self) -> Path:
        # return self.base_path / "fact_job_skill"
        

# # GOLD V2 CONTEXT

# @dataclass
# class GoldV2Context:
    # date_range: DateRange | None
    # base_path: Path
    # min_clusters: int
    # min_silhouette: float
    # spark: SparkSession
    
    # @property
    # def job_cluster_membership_path(self) -> Path:
        # return self.base_path / f"job_cluster_membership"
    
    # @property
    # def job_cluster_path(self) -> Path:
        # return self.base_path / f"job_cluster"

    # @property
    # def job_centroids_path(self) -> Path:
        # return self.base_path / f"job_centroids"

    # @property
    # def cluster_metadata_path(self) -> Path:
        # return self.base_path / f"cluster_metadata"
        
    # @property
    # def job_embeddings_path(self) -> Path:
        # return self.base_path / f"job_embeddings"
        
    # @property
    # def skill_embeddings_path(self) -> Path:
        # return self.base_path / f"skill_embeddings"
        

# # FULL PIPELINE CONTEXT

# @dataclass
# class PipelineContext:
    # #date_range: DateRange | None
    # env: str
    # spark: SparkSession
    # bronze: BronzeContext
    # silver: SilverContext
    # gold_v1: GoldV1Context
    # gold_v2: GoldV2Context
