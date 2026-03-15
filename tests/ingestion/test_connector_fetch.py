import pytest
from unittest.mock import MagicMock
from job_plat.ingestion.connectors import PaginatedAPIConnector

def test_paginated_connector_fetch(monkeypatch):
    
    responses = {
        1: {"count": 4, "results": [{"id": 1}, {"id": 2}]},
        2: {"count": 4, "results": [{"id": 3}, {"id": 4}]},
        3: {"count": 4, "results": []},
    }

    class FakeConnector(PaginatedAPIConnector):
        
        name = "fake"
        
        def _api_call(self, criteria, page):
            return responses[page]
        
        def _extract_results(self, data):
            return data["results"]

        def normalize(self, raw_job):
            pass

    connector = FakeConnector(max_pages=10)

    results = list(connector.fetch(criteria=MagicMock()))
    
    assert len(results) == 4
    assert results[0]["id"] == 1
    assert results[-1]["id"] == 4
