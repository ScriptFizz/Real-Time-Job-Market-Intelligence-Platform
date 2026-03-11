from typing import Literal, List

class DatasetDef:
    NAME: str
    RELATIVE_PATH: str
    
    PARTITION_COLUMNS: List[str] = ["ingestion_date"]
    TIME_WINDOW_COLUMN: str | None = None
    WRITE_MODE: Literal["overwrite", "append"] = "append"
    FILE_FORMAT: Literal["parquet", "jsonl"] = "parquet"

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

class SilverJobSkills(DatasetDef):
    NAME = "silver_job_skills"
    RELATIVE_PATH = "silver/job_skills"

##################
#   GOLD_V1
##################

class GoldDimJobs(DatasetDef):
    NAME = "gold_dim_jobs"
    RELATIVE_PATH = "gold/dim_jobs"
    PARTITION_COLUMNS = []
    TIME_WINDOW_COLUMN = "posted_at"


class GoldDimSkills(DatasetDef):
    NAME = "gold_dim_skills"
    RELATIVE_PATH = "gold/dim_skills"
    PARTITION_COLUMNS = []
    

class GoldFactJobSkills(DatasetDef):
    NAME = "gold_fact_job_skills"
    RELATIVE_PATH = "gold/fact_job_skills"
    TIME_WINDOW_COLUMN = "posted_at"

##################
#   FEATURE
##################

class FeatureSkillEmbeddings(DatasetDef):
    NAME = "feature_skill_embeddings"
    RELATIVE_PATH = "feature/skill_embeddings"
    PARTITION_COLUMNS = []

class FeatureJobEmbeddings(DatasetDef):
    NAME = "feature_job_embeddings"
    RELATIVE_PATH = "feature/job_embeddings"
    PARTITION_COLUMNS = []

##################
#   ML
##################


class MLJobClusters(DatasetDef):
    NAME = "ml_job_clusters"
    RELATIVE_PATH = "gold_v2/job_clusters"
    PARTITION_COLUMNS = []
    
class MLJobMembership(DatasetDef):
    NAME = "ml_job_membership"
    RELATIVE_PATH = "ml/job_membership"
    PARTITION_COLUMNS = []

class MLJobCentroids(DatasetDef):
    NAME = "ml_job_centroids"
    RELATIVE_PATH = "ml/job_centroids"
    PARTITION_COLUMNS = []
    
class MLJobClusterMetadata(DatasetDef):
    NAME = "ml_job_cluster_metadata"
    RELATIVE_PATH = "ml/job_cluster_metadata"
    PARTITION_COLUMNS = []

DATASET_DEFS = [
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
    MLJobClusterMetadata
]
