import pytest
from unittest.mock import MagicMock
from job_plat.orchestration.data_pipeline import run_data_pipeline


def test_data_pipeline_orchestration(monkeypatch):
    
    bronze_mock = MagicMock()
    silver_mock = MagicMock()
    gold_mock = MagicMock()

    monkeypatch.setattr(
        "job_plat.orchestration.data_pipeline.run_bronze_pipeline",
        bronze_mock
    )

    monkeypatch.setattr(
        "job_plat.orchestration.data_pipeline.run_silver_pipeline",
        silver_mock
    )

    monkeypatch.setattr(
        "job_plat.orchestration.data_pipeline.run_gold_pipeline",
        gold_mock
    )
    
    connectors = [MagicMock(), MagicMock()]
    ctx = MagicMock()
    storage = MagicMock()
    datasets = MagicMock()
    partition_manager = MagicMock()
    
    # Code to check for calls order
    
    call_order = []
    
    def bronze_side_effect(*args, **kwargs):
        call_order.append("bronze")
    
    def silver_side_effect(*args, **kwargs):
        call_order.append("silver")
    
    def gold_side_effect(*args, **kwargs):
        call_order.append("gold")
        
    bronze_mock.side_effect = bronze_side_effect
    silver_mock.side_effect = silver_side_effect
    gold_mock.side_effect = gold_side_effect
    
    run_data_pipeline(
        ctx=ctx,
        datasets=datasets,
        partition_manager=partition_manager,
        storage=storage,
        connectors=connectors
    )
    
    assert bronze_mock.call_count == len(connectors)
    assert silver_mock.called
    assert gold_mock.called
    assert call_order == ["bronze", "bronze", "silver", "gold"]
