from job_plat.pipeline.datasets.dataset_definitions import (
    DatasetDef,
    FeatureJobEmbeddings,
    FeatureSkillEmbeddings,
    MLJobCentroids,
    MLJobClusterMetadata,
    MLJobClusters,
    MLJobMembership,
)
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
    assert dataset.file_format == "delta"


def test_feature_embeddings_use_versioned_merge_identity():
    assert FeatureSkillEmbeddings.WRITE_MODE == "merge"
    assert FeatureSkillEmbeddings.MERGE_KEYS == ("skill_id", "model_version")

    assert FeatureJobEmbeddings.WRITE_MODE == "merge"
    assert FeatureJobEmbeddings.MERGE_KEYS == ("job_id", "model_version")


def test_ml_outputs_use_training_run_merge_identity():
    assert MLJobMembership.MERGE_KEYS == ("model_id", "job_id")
    assert MLJobClusters.MERGE_KEYS == ("model_id", "cluster_id")
    assert MLJobCentroids.MERGE_KEYS == ("model_id", "cluster_id")
    assert MLJobClusterMetadata.MERGE_KEYS == ("model_id",)
