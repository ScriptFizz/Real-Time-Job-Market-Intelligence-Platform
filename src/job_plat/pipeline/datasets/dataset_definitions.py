from typing import Literal

WriteMode = Literal[
    "append",
    "overwrite",
    "replace_partitions",
    "merge",
]

MergeOrder = Literal["asc", "desc"]
FileFormat = Literal["delta", "jsonl", "parquet"]


class DatasetDef:
    NAME: str
    RELATIVE_PATH: str

    PARTITION_COLUMNS: list[str] = ["ingestion_date"]
    TIME_WINDOW_COLUMN: str | None = None
    WRITE_MODE: WriteMode = "append"
    FILE_FORMAT: FileFormat = "delta"
    MERGE_KEYS: tuple[str, ...] = ()
    MERGE_ORDER_COLUMN: str | None = None
    MERGE_ORDER: MergeOrder = "desc"
    REQUIRED_COLUMNS: tuple[str, ...] = ()
    EXPECTED_TYPES: dict[str, str] = {}
    NON_NULL_COLUMNS: tuple[str, ...] = ()
    UNIQUE_KEYS: tuple[str, ...] = ()
    FRESHNESS_COLUMN: str | None = None


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
    REQUIRED_COLUMNS = (
        "source",
        "job_id",
        "job_title",
        "description",
        "ingestion_date",
        "ingested_at",
    )
    EXPECTED_TYPES = {"source": "string"}
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = ("source", "job_id")
    FRESHNESS_COLUMN = "ingested_at"


class SilverJobSkills(DatasetDef):
    NAME = "silver_job_skills"
    RELATIVE_PATH = "silver/job_skills"
    WRITE_MODE = "replace_partitions"
    REQUIRED_COLUMNS = (
        "job_id",
        "skills",
        "skill_confidence",
        "processed_at",
        "ingestion_date",
    )
    EXPECTED_TYPES = {"skills": "string"}
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = ("job_id", "skills")
    FRESHNESS_COLUMN = "processed_at"


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
    REQUIRED_COLUMNS = ("job_id", "source", "job_title", "ingestion_date")
    EXPECTED_TYPES = {"source": "string"}
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = MERGE_KEYS
    FRESHNESS_COLUMN = "posted_at"


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
    REQUIRED_COLUMNS = ("skill_id", "skills", "ingestion_date")
    EXPECTED_TYPES = {"skill_id": "string", "skills": "string"}
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = MERGE_KEYS


class GoldFactJobSkills(DatasetDef):
    NAME = "gold_fact_job_skills"
    RELATIVE_PATH = "gold/fact_job_skills"
    TIME_WINDOW_COLUMN = "posted_at"
    WRITE_MODE = "replace_partitions"
    REQUIRED_COLUMNS = (
        "job_id",
        "skill_id",
        "skill_confidence",
        "processed_at",
        "ingestion_date",
    )
    EXPECTED_TYPES = {"skill_id": "string"}
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = ("job_id", "skill_id")
    FRESHNESS_COLUMN = "processed_at"


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
    REQUIRED_COLUMNS = (
        "skill_id",
        "embedding",
        "embedding_dim",
        "model_name",
        "model_version",
        "generated_at",
    )
    EXPECTED_TYPES = {
        "skill_id": "string",
        "embedding": "array<double>",
        "embedding_dim": "int",
    }
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = MERGE_KEYS
    FRESHNESS_COLUMN = "generated_at"


class FeatureJobEmbeddings(DatasetDef):
    NAME = "feature_job_embeddings"
    RELATIVE_PATH = "feature/job_embeddings"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("job_id", "model_version")
    MERGE_ORDER_COLUMN = "generated_at"
    MERGE_ORDER = "desc"
    REQUIRED_COLUMNS = (
        "job_id",
        "embedding_normalized",
        "embedding_dim",
        "model_version",
        "generated_at",
    )
    EXPECTED_TYPES = {
        "job_id": "string",
        "embedding_normalized": "array<double>",
        "embedding_dim": "int",
    }
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = MERGE_KEYS
    FRESHNESS_COLUMN = "generated_at"


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
    REQUIRED_COLUMNS = ("model_id", "cluster_id", "cluster_size", "created_at")
    EXPECTED_TYPES = {"model_id": "string", "cluster_id": "int"}
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = MERGE_KEYS
    FRESHNESS_COLUMN = "created_at"


class MLJobMembership(DatasetDef):
    NAME = "ml_job_membership"
    RELATIVE_PATH = "ml/job_membership"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("model_id", "job_id")
    MERGE_ORDER_COLUMN = "assigned_at"
    MERGE_ORDER = "desc"
    REQUIRED_COLUMNS = ("model_id", "job_id", "cluster_id", "assigned_at")
    EXPECTED_TYPES = {
        "model_id": "string",
        "job_id": "string",
        "cluster_id": "int",
    }
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = MERGE_KEYS
    FRESHNESS_COLUMN = "assigned_at"


class MLJobCentroids(DatasetDef):
    NAME = "ml_job_centroids"
    RELATIVE_PATH = "ml/job_centroids"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("model_id", "cluster_id")
    MERGE_ORDER_COLUMN = "created_at"
    MERGE_ORDER = "desc"
    REQUIRED_COLUMNS = (
        "model_id",
        "cluster_id",
        "centroid_vector",
        "embedding_dim",
        "created_at",
    )
    EXPECTED_TYPES = {
        "model_id": "string",
        "cluster_id": "int",
        "centroid_vector": "array<double>",
        "embedding_dim": "int",
    }
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = MERGE_KEYS
    FRESHNESS_COLUMN = "created_at"


class MLJobClusterMetadata(DatasetDef):
    NAME = "ml_job_cluster_metadata"
    RELATIVE_PATH = "ml/job_cluster_metadata"
    PARTITION_COLUMNS = []
    WRITE_MODE = "merge"
    MERGE_KEYS = ("model_id",)
    MERGE_ORDER_COLUMN = "created_at"
    MERGE_ORDER = "desc"
    REQUIRED_COLUMNS = (
        "model_id",
        "model_version",
        "training_data_fingerprint",
        "artifact_uri",
        "lifecycle_status",
        "created_at",
    )
    EXPECTED_TYPES = {"model_id": "string", "model_version": "string"}
    NON_NULL_COLUMNS = REQUIRED_COLUMNS
    UNIQUE_KEYS = MERGE_KEYS
    FRESHNESS_COLUMN = "created_at"


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
