from pydantic import BaseModel, Field
from typing import Optional, Literal, Any


class PathsConfig(BaseModel):
    root: str
    metadata: str
    bronze: str
    silver: str
    gold_v1: str
    gold_v2: str

class SparkConfig(BaseModel):
    app_name: str
    master: str
    config: dict[str, Any] = Field(default_factory=dict)

class StorageConfig(BaseModel):
    type: Literal["local", "gcs"]

class BronzeConfig(BaseModel):
    query: Optional[str] = None
    location: Optional[str] = None
    country: Optional[str] = None
    max_pages: Optional[int] = Field(default=None, gt=0)
    min_interval_seconds: Optional[float] = Field(default=None, gt=0)

class GoldConfig(BaseModel):
    fact_per_job_ratio_threshold: int

class MLConfig(BaseModel):
    min_clusters: int
    min_silhouette: float
    window_days: int


class EnvironmentConfig(BaseModel):
    env: str
    paths: PathsConfig
    spark: SparkConfig
    storage: StorageConfig
    bronze: BronzeConfig
    gold: GoldConfig
    ml: MLConfig
    logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]


###############################à 09-03

# class PathsConfig(BaseModel):
    # root: str
    # metadata: str
    # bronze: str
    # silver: str
    # gold_v1: str
    # gold_v2: str

# class SparkConfig(BaseModel):
    # app_name: str
    # master: str
    # config: dict[str, Any] = Field(default_factory=dict)

# class StorageConfig(BaseModel):
    # type: Literal["local", "gcs"]

# class BronzeConfig(BaseModel):
    # query: Optional[str] = None
    # location: Optional[str] = None
    # country: Optional[str] = None
    # max_pages: Optional[int] = Field(default=None, gt=0)
    # min_interval_seconds: Optional[float] = Field(default=None, gt=0)

# class GoldV1Config(BaseModel):
    # fact_per_job_ratio_threshold: int

# class GoldV2Config(BaseModel):
    # min_clusters: int
    # min_silhouette: float


# class EnvironmentConfig(BaseModel):
    # env: str
    # paths: PathsConfig
    # spark: SparkConfig
    # storage: StorageConfig
    # bronze: BronzeConfig
    # gold_v1: GoldV1Config
    # gold_v2: GoldV2Config
    # logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]
