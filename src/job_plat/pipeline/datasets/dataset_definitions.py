from typing import Literal, List

class DatasetDef:
    NAME: str
    RELATIVE_PATH: str
    
    PARTITION_COLUMNS: List[str] = ["ingestion_date"]
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

class GoldV1DimJobs(DatasetDef):
    NAME = "gold_v1_dim_jobs"
    RELATIVE_PATH = "gold_v1/dim_jobs"
    PARTITION_COLUMNS = []

class GoldV1DimSkills(DatasetDef):
    NAME = "gold_v1_dim_skills"
    RELATIVE_PATH = "gold_v1/dim_skills"
    PARTITION_COLUMNS = []

class GoldV1FactJobSkills(DatasetDef):
    NAME = "gold_v1_fact_job_skills"
    RELATIVE_PATH = "gold_v1/fact_job_skills"

##################
#   GOLD_V2
##################

class GoldV2SkillEmbeddings(DatasetDef):
    NAME = "gold_v2_skill_embeddings"
    RELATIVE_PATH = "gold_v2/skill_embeddings"

class GoldV2JobEmbeddings(DatasetDef):
    NAME = "gold_v2_job_embeddings"
    RELATIVE_PATH = "gold_v2/job_embeddings"

class GoldV2JobClusters(DatasetDef):
    NAME = "gold_v2_job_clusters"
    RELATIVE_PATH = "gold_v2/job_clusters"
    
class GoldV2JobMembership(DatasetDef):
    NAME = "gold_v2_job_membership"
    RELATIVE_PATH = "gold_v2/job_membership"

class GoldV2JobCentroids(DatasetDef):
    NAME = "gold_v2_job_centroids"
    RELATIVE_PATH = "gold_v2/job_centroids"
    
class GoldV2JobClusterMetadata(DatasetDef):
    NAME = "gold_v2_job_cluster_metadata"
    RELATIVE_PATH = "gold_v2/job_cluster_metadata"

DATASET_DEFS = [
    BronzeJobs,
    SilverJobs,
    SilverJobSkills,
    GoldV1DimJobs,
    GoldV1DimSkills,
    GoldV1FactJobSkills,
    GoldV2SkillEmbeddings,
    GoldV2JobEmbeddings,
    GoldV2JobClusters,
    GoldV2JobMembership,
    GoldV2JobCentroids,
    GoldV2JobClusterMetadata
]

############################# 08-03

# class DatasetDef:
    # NAME: str
    # RELATIVE_PATH: str
    
    # PARTITION_COLUMN: str = "ingestion_date"
    # WRITE_MODE: Literal["overwrite", "append"] = "append"
    # FILE_FORMAT: Literal["parquet", "jsonl"] = "parquet"

# ##################
# #   BRONZE
# ##################

# class BronzeJobs(DatasetDef):
    # NAME = "bronze_jobs"
    # RELATIVE_PATH = "bronze/jobs"
    # FILE_FORMAT = "jsonl"

# ##################
# #   SILVER
# ##################

# class SilverJobs(DatasetDef):
    # NAME = "silver_jobs"
    # RELATIVE_PATH = "silver/jobs"

# class SilverJobSkills(DatasetDef):
    # NAME = "silver_job_skills"
    # RELATIVE_PATH = "silver/job_skills"

# ##################
# #   GOLD_V1
# ##################

# class GoldV1DimJobs(DatasetDef):
    # NAME = "gold_v1_dim_jobs"
    # RELATIVE_PATH = "gold_v1/dim_jobs"

# class GoldV1DimSkills(DatasetDef):
    # NAME = "gold_v1_dim_skills"
    # RELATIVE_PATH = "gold_v1/dim_skills"

# class GoldV1FactJobSkills(DatasetDef):
    # NAME = "gold_v1_fact_job_skills"
    # RELATIVE_PATH = "gold_v1/fact_job_skills"

# ##################
# #   GOLD_V2
# ##################

# class GoldV2SkillEmbeddings(DatasetDef):
    # NAME = "gold_v2_skill_embeddings"
    # RELATIVE_PATH = "gold_v2/skill_embeddings"

# class GoldV2JobEmbeddings(DatasetDef):
    # NAME = "gold_v2_job_embeddings"
    # RELATIVE_PATH = "gold_v2/job_embeddings"

# class GoldV2JobClusters(DatasetDef):
    # NAME = "gold_v2_job_clusters"
    # RELATIVE_PATH = "gold_v2/job_clusters"
    
# class GoldV2JobMembership(DatasetDef):
    # NAME = "gold_v2_job_membership"
    # RELATIVE_PATH = "gold_v2/job_membership"

# class GoldV2JobCentroids(DatasetDef):
    # NAME = "gold_v2_job_centroids"
    # RELATIVE_PATH = "gold_v2/job_centroids"
    
# class GoldV2JobClusterMetadata(DatasetDef):
    # NAME = "gold_v2_job_cluster_metadata"
    # RELATIVE_PATH = "gold_v2/job_cluster_metadata"

# DATASET_DEFS = [
    # BronzeJobs,
    # SilverJobs,
    # SilverJobSkills,
    # GoldV1DimJobs,
    # GoldV1DimSkills,
    # GoldV1FactJobSkills,
    # GoldV2SkillEmbeddings,
    # GoldV2JobEmbeddings,
    # GoldV2JobClusters,
    # GoldV2JobMembership,
    # GoldV2JobCentroids,
    # GoldV2JobClusterMetadata
# ]
