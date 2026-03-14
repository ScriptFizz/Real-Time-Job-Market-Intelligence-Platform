import pytest
from job_plat.pipeline.datasets.dataset import Dataset
from job_plat.pipeline.datasets.dataset_definitions import DATASET_DEFS
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.storage.storages import LocalStorage


@pytest.fixture
def storage():
    return LocalStorage()

@pytest.fixture
def dataset_registry(tmp_path, storage):
    
    return DatasetRegistry(
        tmp_path,
        storage,
        DATASET_DEFS
    )


@pytest.fixture
def bronze_jobs_dataset(tmp_path, storage):
    
    return Dataset(
        name="bronze_jobs",
        #path="bronze/jobs",
        path=tmp_path / "bronze/jobs",
        storage=storage,
        partition_columns=["ingestion_date"],
        file_format="jsonl",
    )


@pytest.fixture
def silver_jobs_dataset(tmp_path, storage):
    
    return Dataset(
        name="silver_jobs",
        path=tmp_path / "silver/jobs",
        storage=storage,
        partition_columns=["ingestion_date"],
        file_format="parquet",
    )


@pytest.fixture
def silver_job_skills_dataset(tmp_path, storage):
    
    return Dataset(
        name="silver_job_skills",
        path=tmp_path / "silver/job_skills",
        storage=storage,
        partition_columns=["ingestion_date"],
        file_format="parquet",
    )


@pytest.fixture
def gold_dim_skills(tmp_path, storage):
    
    return Dataset(
        name="god_dim_skills",
        path= tmp_path / "gold/dim_skills",
        partition_columns = [],
        file_format="parquet"
    )
