import pytest
# from test.fixtures.datasets import dataset_registry
# from test.fixtures.sample_dataframe import (
    # bronze_jobs_df, 
    # silver_jobs_df, 
    # silver_job_skills_df
# )
from job_plat.pipeline.datasets.dataset_definitions import (
    BronzeJobs,
    SilverJobs,
    SilverJobSkills
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
