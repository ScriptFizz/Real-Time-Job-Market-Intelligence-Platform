from pathlib import Path

import pytest
import yaml

from job_plat.config.config_loader import ConfigLoader


def write_settings(path: Path, environments: dict) -> None:
    path.write_text(
        yaml.safe_dump({"environments": environments}),
        encoding="utf-8",
    )


def production_config() -> dict:
    return {
        "spark": {
            "app_name": "job-platform-prod",
            "master": "yarn",
            "config": {
                "spark.sql.shuffle.partitions": 200,
            },
        },
        "storage": {
            "type": "gcs",
        },
        "paths": {
            "root": "gs://job-pipeline",
            "metadata": "gs://job-pipeline/metadata",
        },
        "bronze": {
            "query": "data engineer",
            "location": "London",
            "max_pages": 5,
        },
        "gold": {
            "fact_per_job_ratio_threshold": 20,
        },
        "ml": {
            "min_clusters": 3,
            "min_silhouette": 0.2,
            "window_days": 30,
        },
        "logging_level": "INFO",
    }


def test_load_gcs_environment_preserves_cloud_uris(tmp_path):
    write_settings(
        tmp_path / "settings.yaml",
        environments={"prod": production_config()},
    )

    config = ConfigLoader(
        config_path="settings.yaml",
        env="prod",
        project_root=tmp_path,
    ).load_env()

    assert config.storage.type == "gcs"
    assert config.paths.root == "gs://job-pipeline"
    assert config.paths.metadata == "gs://job-pipeline/metadata"


def test_loader_rejects_non_mapping_yaml(tmp_path):
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        "- dev\n- prod\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="top-level YAML value must be a mapping",
    ):
        ConfigLoader(
            config_path="settings.yaml",
            env="dev",
            project_root=tmp_path,
        )


def test_load_gcs_environment_rejects_non_gcs_metadata_uri(tmp_path):
    config_data = production_config()
    config_data["paths"]["metadata"] = "/local/metadata"

    write_settings(
        tmp_path / "settings.yaml",
        environments={"prod": config_data},
    )

    with pytest.raises(
        ValueError,
        match="GCS metadata path must start with 'gs://'",
    ):
        ConfigLoader(
            config_path="settings.yaml",
            env="prod",
            project_root=tmp_path,
        ).load_env()
