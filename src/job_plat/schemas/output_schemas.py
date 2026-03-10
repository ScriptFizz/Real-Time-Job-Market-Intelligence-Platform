from dataclasses import dataclass, fields, field
from pyspark.sql import DataFrame
from job_plat.pipeline.datasets.dataset_definitions import (
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
)

@dataclass
class StageOutput:
    """Base class for typed stage ouputs."""
    @classmethod
    def dataset_map(cls) -> dict[str, type]:
        return {f.name: f.metadata["dataset"] for f in fields(cls) if "dataset" in f.metadata}
    
@dataclass
class SilverOutputs(StageOutput):
    
    # DATASETS = {
        # "silver_jobs": SilverJobs,
        # "silver_job_skills": SilverJobSkills
    # }
    silver_jobs: DataFrame = field(metadata={"dataset": SilverJobs})
    silver_job_skills: DataFrame = field(metadata={"dataset": SilverJobSkills})

@dataclass 
class GoldV1Outputs(StageOutput):
    
    # DATASETS = {
        # "dim_jobs": GoldV1DimJobs,
        # "dim_skills": GoldV1DimSkills,
        # "fact_job_skills": GoldV1FactJobSkills
    # }
    
    dim_jobs: DataFrame = field(metadata={"dataset": GoldV1DimJobs})
    dim_skills: DataFrame = field(metadata={"dataset": GoldV1DimSkills})
    fact_job_skills: DataFrame = field(metadata={"dataset": GoldV1FactJobSkills})


@dataclass
class GoldV2Outputs(StageOutput):
    # DATASETS = {
        # "skill_embeddings": GoldV2SkillEmbeddings,
        # "job_embeddings": GoldV2JobEmbeddings,
        # "job_clusters": GoldV2JobClusters,
        # "job_membership": GoldV2JobMembership,
        # "job_centroids": GoldV2JobCentroids,
        # "job_cluster_metadata": GoldV2JobClusterMetadata
    # }
    
    skill_embeddings: DataFrame = field(metadata={"dataset": GoldV2SkillEmbeddings})
    job_embeddings: DataFrame = field(metadata={"dataset": GoldV2JobEmbeddings})
    job_clusters: DataFrame = field(metadata={"dataset": GoldV2JobClusters})
    job_membership: DataFrame = field(metadata={"dataset": GoldV2JobMembership})
    job_centroids: DataFrame = field(metadata={"dataset": GoldV2JobCentroids})
    job_cluster_metadata: DataFrame = field(metadata={"dataset": GoldV2JobClusterMetadata})

###################################

# @dataclass
# class StageOutput:
    # """Base class for typed stage ouputs."""

# @dataclass
# class SilverOutputs(StageOutput):
    
    # DATASETS = {
        # "silver_jobs": SilverJobs,
        # "silver_job_skills": SilverJobSkills
    # }
    # silver_jobs: DataFrame
    # silver_job_skills: DataFrame

# @dataclass 
# class GoldV1Outputs(StageOutput):
    
    # DATASETS = {
        # "dim_jobs": GoldV1DimJobs,
        # "dim_skills": GoldV1DimSkills,
        # "fact_job_skills": GoldV1FactJobSkills
    # }
    
    # dim_jobs: DataFrame
    # dim_skills: DataFrame
    # fact_job_skills: DataFrame


# @dataclass
# class GoldV2Outputs(StageOutput):
    # DATASETS = {
        # "skill_embeddings": GoldV2SkillEmbeddings,
        # "job_embeddings": GoldV2JobEmbeddings,
        # "job_clusters": GoldV2JobClusters,
        # "job_membership": GoldV2JobMembership,
        # "job_centroids": GoldV2JobCentroids,
        # "job_cluster_metadata": GoldV2JobClusterMetadata
    # }
    
    # sill_embeddings: DataFrame
    # job_embeddings: DataFrame
    # job_clusters: DataFrame
    # job_membership: DataFrame
    # job_centroids: DataFrame
    # job_cluster_metadata: DataFrame
