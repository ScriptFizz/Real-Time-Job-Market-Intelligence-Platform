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
    country: str | None = None

@dataclass
class BronzeContext:
    root_path: str
    query: str | None = None
    location: str | None = None
    country: str | None = None


# SILVER CONTEXT

@dataclass
class SilverContext:
    spark: SparkSession
    

# GOLD CONTEXT

@dataclass
class GoldContext:
    fact_per_job_ratio_threshold: int
    spark: SparkSession


# FEATURE CONTEXT

@dataclass
class FeatureContext:
    spark: SparkSession
    window_days: int        

# ML CONTEXT

@dataclass
class MLContext:
    min_clusters: int
    min_silhouette: float
    spark: SparkSession
        

# DATA PIPELINE CONTEXT

@dataclass
class DataPipelineContext:
    env: str
    spark: SparkSession
    bronze: BronzeContext
    silver: SilverContext
    gold: GoldContext


# ML PIPELINE CONTEXT

@dataclass
class MLPipelineContext:
    env: str
    spark: SparkSession
    #gold: GoldContext
    feature: FeatureContext
    ml: MLContext
