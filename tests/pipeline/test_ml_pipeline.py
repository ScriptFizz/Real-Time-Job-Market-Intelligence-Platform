import pytest
from unittest.mock import MagicMock
from job_plat.orchestration.ml_pipeline import run_full_ml_pipeline


def test_ml_pipeline_orchestration(monkeypatch):
    
    feature_mock = MagicMock()
    ml_mock = MagicMock()

    monkeypatch.setattr(
        "job_plat.orchestration.ml_pipeline.run_feature_pipeline",
        feature_mock
    )

    monkeypatch.setattr(
        "job_plat.orchestration.ml_pipeline.run_ml_pipeline",
        ml_mock
    )
    
    ctx = MagicMock()
    datasets = MagicMock()
    partition_manager = MagicMock()
    
    # Code to check for calls order
    
    call_order = []
    
    def feature_side_effect(*args, **kwargs):
        call_order.append("feature")
    
    def ml_side_effect(*args, **kwargs):
        call_order.append("ml")
    
        
    feature_mock.side_effect = feature_side_effect
    ml_mock.side_effect = ml_side_effect
    
    run_full_ml_pipeline(
        ctx=ctx,
        datasets=datasets,
        partition_manager=partition_manager
    )
    
    assert feature_mock.called
    assert ml_mock.called
    assert call_order == ["feature", "ml"]
