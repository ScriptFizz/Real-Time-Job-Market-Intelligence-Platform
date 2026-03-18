from dataclasses import dataclass
from pathlib import Path
from datetime import date
from pyspark.sql import SparkSession
from typing import List, Optional
from datetime import datetime
import uuid

#  EXECUTION PARAMS

@dataclass
class ExecutionParams:
    query: str | None = None
    location: str | None = None
    country: str | None = None
    

@dataclass
class BaseContext:
    execution_date: datetime | None

@dataclass
class BronzeContext(BaseContext):
    root_path: str
    query: str | None = None
    location: str | None = None
    country: str | None = None


# SILVER CONTEXT

@dataclass
class SilverContext(BaseContext):
    spark: SparkSession
    

# GOLD CONTEXT

@dataclass
class GoldContext(BaseContext):
    fact_per_job_ratio_threshold: int
    spark: SparkSession


# FEATURE CONTEXT

@dataclass
class FeatureContext(BaseContext):
    spark: SparkSession
    window_days: int        

# ML CONTEXT

@dataclass
class MLContext(BaseContext):
    min_clusters: int
    min_silhouette: float
    spark: SparkSession
        

# DATA PIPELINE CONTEXT

@dataclass
class DataPipelineContext(BaseContext):
    env: str
    spark: SparkSession
    bronze: BronzeContext
    silver: SilverContext
    gold: GoldContext


# ML PIPELINE CONTEXT

@dataclass
class MLPipelineContext(BaseContext):
    env: str
    spark: SparkSession
    feature: FeatureContext
    ml: MLContext



@dataclass(kw_only=True)
class StageExecutionContext:
    stage: str
    pipeline_version: str
    run_id: str = str(uuid.uuid4())
    started_at: datetime = datetime.utcnow()
