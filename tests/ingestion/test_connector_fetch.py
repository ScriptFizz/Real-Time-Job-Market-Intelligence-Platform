from unittest.mock import MagicMock

from job_plat.ingestion.connectors import PaginatedAPIConnector
from job_plat.ingestion.job_schema import JobSource


def test_paginated_connector_fetch():
    expected_criteria = MagicMock()

    responses = {
        1: {"count": 4, "results": [{"id": 1}, {"id": 2}]},
        2: {"count": 4, "results": [{"id": 3}, {"id": 4}]},
        3: {"count": 4, "results": []},
    }

    class FakeConnector(PaginatedAPIConnector):
        name: JobSource = "adzuna"

        def _api_call(self, criteria, page):
            assert criteria is expected_criteria
            return responses[page]

        def _extract_results(self, data):
            return data["results"]

        def normalize(self, raw_job):
            raise NotImplementedError

    connector = FakeConnector(max_pages=10)

    results = list(connector.fetch(expected_criteria))

    assert len(results) == 4
    assert results[0]["id"] == 1
    assert results[-1]["id"] == 4
