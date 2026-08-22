import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pyspark.sql import SparkSession

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
class SparkStageContext(BaseContext):
    spark: SparkSession


@dataclass
class BronzeContext(BaseContext):
    root_path: str
    query: str | None = None
    location: str | None = None
    country: str | None = None


# SILVER CONTEXT


@dataclass
class SilverContext(SparkStageContext):
    pass


# GOLD CONTEXT


@dataclass
class GoldContext(SparkStageContext):
    fact_per_job_ratio_threshold: int


# FEATURE CONTEXT


@dataclass
class FeatureContext(SparkStageContext):
    window_days: int


# ML CONTEXT


@dataclass
class MLContext(SparkStageContext):
    min_clusters: int
    min_silhouette: float


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
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
