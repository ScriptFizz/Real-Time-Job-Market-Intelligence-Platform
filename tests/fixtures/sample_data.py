import pytest
from job_plat.pipeline.datasets.dataset_definitions import (
    BronzeJobs,
    SilverJobs,
    SilverJobSkills,
    GoldDimSkills
)
    
@pytest.fixture
def bronze_jobs_data(dataset_registry, job_bronze_df):
    
    ds = dataset_registry.get(BronzeJobs)
    
    ds.write(job_bronze_df, mode="overwrite")
    
    assert ds.list_partitions()
    
    return job_bronze_df


@pytest.fixture
def silver_jobs_data(dataset_registry, silver_jobs_df):
    
    ds = dataset_registry.get(SilverJobs)
    
    ds.write(silver_jobs_df)
    
    return silver_jobs_df


@pytest.fixture
def silver_job_skills_data(dataset_registry, silver_job_skills_df):
    
    ds = dataset_registry.get(SilverJobSkills)
    
    ds.write(silver_job_skills_df)
    
    return silver_job_skills_df


@pytest.fixture
def gold_dim_skills_data(dataset_registry, gold_dim_skills_df):
    
    ds = dataset_registry.get(GoldDimSkills)
    
    ds.write(gold_dim_skills_df)
    
    return gold_dim_skills_df
