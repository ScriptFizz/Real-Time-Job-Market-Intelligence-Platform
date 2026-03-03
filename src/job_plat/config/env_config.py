from pydantic import BaseModel, Field
from typing import Optional, Literal, Any


class PathsConfig(BaseModel):
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
    max_pages: Optional[int] = Field(default=None, gt=0)
    min_interval_seconds: Optional[float] = Field(default=None, gt=0)


class EnvironmentConfig(BaseModel):
    env: str
    paths: PathsConfig
    spark: SparkConfig
    storage: StorageConfig
    bronze: BronzeConfig
    logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]

