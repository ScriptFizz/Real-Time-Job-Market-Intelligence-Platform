from typing import Literal

WriteMode = Literal[
    "append",
    "overwrite",
    "replace_partitions",
    "merge",
]

MergeOrder = Literal["asc", "desc"]

class DatasetDef:
    NAME: str
    RELATIVE_PATH: str

    PARTITION_COLUMNS: list[str] = ["ingestion_date"]
    TIME_WINDOW_COLUMN: str | None = None
    WRITE_MODE: WriteMode = "append"
    FILE_FORMAT: Literal["parquet", "jsonl"] = "parquet"
    MERGE_KEYS: tuple[str, ...] = ()
    MERGE_ORDER_COLUMN: str | None = None
    MERGE_ORDER: MergeOrder = "desc"


##################
#   BRONZE
##################


class BronzeJobs(DatasetDef):
    NAME = "bronze_jobs"
    RELATIVE_PATH = "bronze/jobs"
    FILE_FORMAT = "jsonl"


##################
#   SILVER
##################


class SilverJobs(DatasetDef):
    NAME = "silver_jobs"
    RELATIVE_PATH = "silver/jobs"
    WRITE_MODE = "replace_partitions"


class SilverJobSkills(DatasetDef):
    NAME = "silver_job_skills"
    RELATIVE_PATH = "silver/job_skills"
    WRITE_MODE = "replace_partitions"


##################
#   GOLD_V1
##################


class GoldDimJobs(DatasetDef):
    NAME = "gold_dim_jobs"
    RELATIVE_PATH = "gold/dim_jobs"
    PARTITION_COLUMNS = []
    TIME_WINDOW_COLUMN = "posted_at"
    WRITE_MODE = "merge"
    MERGE_KEYS = ("job_id",)
    MERGE_ORDER_COLUMN = "ingestion_date"
    # A job dimension should reflect the latest observed attributes.
    MERGE_ORDER = "desc"


class GoldDimSkills(DatasetDef):
    NAME = "gold_dim_skills"
    RELATIVE_PATH = "gold/dim_skills"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("skill_id",)
    MERGE_ORDER_COLUMN = "ingestion_date"
    # A skill dimension’s ingestion date represents when the skill was first observed, 
    # so the earliest date should survive.
    MERGE_ORDER = "asc"


class GoldFactJobSkills(DatasetDef):
    NAME = "gold_fact_job_skills"
    RELATIVE_PATH = "gold/fact_job_skills"
    TIME_WINDOW_COLUMN = "posted_at"
    WRITE_MODE = "replace_partitions"


##################
#   FEATURE
##################


class FeatureSkillEmbeddings(DatasetDef):
    NAME = "feature_skill_embeddings"
    RELATIVE_PATH = "feature/skill_embeddings"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("skill_id", "model_version")
    MERGE_ORDER_COLUMN = "generated_at"
    MERGE_ORDER = "desc"


class FeatureJobEmbeddings(DatasetDef):
    NAME = "feature_job_embeddings"
    RELATIVE_PATH = "feature/job_embeddings"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("job_id", "model_version")
    MERGE_ORDER_COLUMN = "generated_at"
    MERGE_ORDER = "desc"


##################
#   ML
##################


class MLJobClusters(DatasetDef):
    NAME = "ml_job_clusters"
    RELATIVE_PATH = "ml/job_clusters"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("model_id", "cluster_id")
    MERGE_ORDER_COLUMN = "created_at"
    MERGE_ORDER = "desc"


class MLJobMembership(DatasetDef):
    NAME = "ml_job_membership"
    RELATIVE_PATH = "ml/job_membership"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("model_id", "job_id")
    MERGE_ORDER_COLUMN = "assigned_at"
    MERGE_ORDER = "desc"


class MLJobCentroids(DatasetDef):
    NAME = "ml_job_centroids"
    RELATIVE_PATH = "ml/job_centroids"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("model_id", "cluster_id")
    MERGE_ORDER_COLUMN = "created_at"
    MERGE_ORDER = "desc"


class MLJobClusterMetadata(DatasetDef):
    NAME = "ml_job_cluster_metadata"
    RELATIVE_PATH = "ml/job_cluster_metadata"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("model_id",)
    MERGE_ORDER_COLUMN = "created_at"
    MERGE_ORDER = "desc"


DATASET_DEFS: list[type[DatasetDef]] = [
    BronzeJobs,
    SilverJobs,
    SilverJobSkills,
    GoldDimJobs,
    GoldDimSkills,
    GoldFactJobSkills,
    FeatureSkillEmbeddings,
    FeatureJobEmbeddings,
    MLJobClusters,
    MLJobMembership,
    MLJobCentroids,
    MLJobClusterMetadata,
]
