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
    metadata_path: str | None = None
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
    embedding_model_name: str = "all-MiniLM-L6-v2"
    embedding_model_version: str = "v1"
    embedding_model_provider: str = "sentence-transformers"
    embedding_batch_size: int = 128
    max_driver_skills: int = 50_000
    job_embedding_aggregation: str = "weighted_mean"


# ML CONTEXT


@dataclass
class MLContext(SparkStageContext):
    min_clusters: int
    min_silhouette: float
    artifact_root: str | None = None
    model_version: str = "v1"
    k_values: tuple[int, ...] = (10, 15, 20, 25, 30)
    seed: int = 42
    promote_model: bool = False


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
