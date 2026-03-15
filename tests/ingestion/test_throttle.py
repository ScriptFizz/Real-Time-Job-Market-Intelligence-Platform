import pytest
import time
from unittest.mock import MagicMock
from job_plat.ingestion.connectors import PaginatedAPIConnector

def test_connector_throttle(monkeypatch):
    
    sleep_mock = MagicMock()
    monkeypatch.setattr("time.sleep", sleep_mock)
    
    class FakeConnector(PaginatedAPIConnector):
        
        name = "fake"
        
        def _api_call(self, criteria, page):
            return responses[page]
        
        def _extract_results(self, data):
            return data["results"]

        def normalize(self, raw_job):
            pass

    connector = FakeConnector(min_interval_seconds=1)

    connector._last_request_ts = time.time()
    
    connector._throttle()
    
    assert sleep_mock.called
