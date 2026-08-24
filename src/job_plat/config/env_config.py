from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field

ConnectorName = Literal["adzuna", "usajobs"]


def default_connectors() -> list[ConnectorName]:
    return ["adzuna"]


class PathsConfig(BaseModel):
    root: str
    metadata: str
    artifacts: str | None = None
    # bronze: str
    # silver: str
    # gold_v1: str
    # gold_v2: str


class SparkConfig(BaseModel):
    app_name: str
    master: str
    config: dict[str, Any] = Field(default_factory=dict)


class StorageConfig(BaseModel):
    type: Literal["local", "gcs"]


class BronzeConfig(BaseModel):
    connectors: list[ConnectorName] = Field(default_factory=default_connectors)
    query: str | None = None
    location: str | None = None
    country: str | None = None
    max_pages: int | None = Field(default=None, gt=0)
    min_interval_seconds: float | None = Field(default=None, gt=0)
    connect_timeout_seconds: float = Field(default=5.0, gt=0)
    read_timeout_seconds: float = Field(default=30.0, gt=0)
    retry_total: int = Field(default=3, ge=0, le=10)
    retry_backoff_factor: float = Field(default=0.5, ge=0)
    retry_backoff_jitter: float = Field(default=0.1, ge=0)


class GoldConfig(BaseModel):
    fact_per_job_ratio_threshold: int


class MLConfig(BaseModel):
    min_clusters: int
    min_silhouette: float
    window_days: int
    embedding_model_name: str = "all-MiniLM-L6-v2"
    embedding_model_version: str = "v1"
    embedding_model_provider: str = "sentence-transformers"
    embedding_batch_size: int = Field(default=128, gt=0)
    max_driver_skills: int = Field(default=50_000, gt=0)
    job_embedding_aggregation: Literal["weighted_mean"] = "weighted_mean"
    clustering_model_version: str = "v1"
    clustering_k_values: list[Annotated[int, Field(ge=2)]] = Field(
        default_factory=lambda: [10, 15, 20, 25, 30],
        min_length=1,
    )
    clustering_seed: int = 42
    promote_model: bool = False


class EnvironmentConfig(BaseModel):
    env: str
    paths: PathsConfig
    spark: SparkConfig
    storage: StorageConfig
    bronze: BronzeConfig
    gold: GoldConfig
    ml: MLConfig
    logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]
