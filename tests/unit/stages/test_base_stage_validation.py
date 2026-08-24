from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from job_plat.pipeline.stages.data.gold_stage import GoldStage


def build_stage_with_input(*, path: str, exists: bool):
    storage = MagicMock()
    storage.exists.return_value = exists

    dataset = SimpleNamespace(
        path=path,
        storage=storage,
    )

    registry = MagicMock()
    registry.get.return_value = dataset

    stage = object.__new__(GoldStage)
    stage.datasets = registry

    return stage, storage


def test_stage_input_validation_uses_storage_backend():
    path = "gs://example-bucket/silver/jobs"
    stage, storage = build_stage_with_input(
        path=path,
        exists=True,
    )

    stage.validate_inputs()

    assert storage.exists.call_count == len(stage.INPUT_MAP)
    storage.exists.assert_called_with(path)


def test_stage_input_validation_reports_missing_storage_path():
    path = "gs://example-bucket/silver/jobs"
    stage, storage = build_stage_with_input(
        path=path,
        exists=False,
    )

    with pytest.raises(
        FileNotFoundError,
        match=path,
    ):
        stage.validate_inputs()

    assert storage.exists.call_count == len(stage.INPUT_MAP)
