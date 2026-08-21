from job_plat.pipeline.datasets.dataset_definitions import DatasetDef
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.storage.storages import LocalStorage


class ExampleJobs(DatasetDef):
    NAME = "example_jobs"
    RELATIVE_PATH = "bronze/jobs"


def test_registry_accepts_path_root(tmp_path):
    registry = DatasetRegistry(
        root=tmp_path,
        storage=LocalStorage(),
        dataset_defs=[ExampleJobs],
    )

    dataset = registry.get(ExampleJobs)

    assert dataset.path == f"{tmp_path}/bronze/jobs"


def test_registry_preserves_uri_root():
    registry = DatasetRegistry(
        root="gs://example-bucket/data/",
        storage=LocalStorage(),
        dataset_defs=[ExampleJobs],
    )

    dataset = registry.get(ExampleJobs)

    assert dataset.path == "gs://example-bucket/data/bronze/jobs"


def test_registry_builds_dataset_from_definition(tmp_path, storage):
    registry = DatasetRegistry(
        root=tmp_path,
        storage=storage,
        dataset_defs=[ExampleJobs],
    )

    dataset = registry.get(ExampleJobs)

    assert dataset.name == "example_jobs"
    assert dataset.partition_columns == ["ingestion_date"]
    assert dataset.file_format == "parquet"
