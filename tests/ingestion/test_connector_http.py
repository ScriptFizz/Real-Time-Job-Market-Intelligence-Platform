import logging
from unittest.mock import MagicMock

import pytest
import requests

from job_plat.ingestion.connectors import (
    ConnectorHTTPError,
    ConnectorRequestError,
    ConnectorResponseError,
    PaginatedAPIConnector,
)
from job_plat.ingestion.job_schema import JobSource


class HTTPTestConnector(PaginatedAPIConnector):
    name: JobSource = "adzuna"

    def _api_call(self, criteria, page):
        raise NotImplementedError

    def _extract_results(self, _data):
        return []

    def normalize(self, raw_job):
        raise NotImplementedError


@pytest.mark.parametrize("transient_status", [429, 500, 502, 503, 504])
def test_retries_transient_statuses_and_honors_retry_after(transient_status):
    connector = HTTPTestConnector(
        retry_total=2,
        retry_backoff_factor=0.25,
        retry_backoff_jitter=0.1,
    )
    adapter = connector.session.get_adapter("https://")
    retry_policy = adapter.max_retries

    assert retry_policy.is_retry(
        "GET", transient_status, has_retry_after=transient_status == 429
    )
    assert retry_policy.total == 2
    assert retry_policy.backoff_factor == 0.25
    assert retry_policy.backoff_jitter == 0.1
    assert retry_policy.respect_retry_after_header is True


def test_permanent_client_error_is_not_retried():
    session = MagicMock(spec=requests.Session)
    response = MagicMock(spec=requests.Response)
    response.status_code = 400
    session.get.return_value = response
    connector = HTTPTestConnector(retry_total=3, session=session)

    retry_policy = (
        HTTPTestConnector(retry_total=3).session.get_adapter("https://").max_retries
    )
    assert retry_policy.is_retry("GET", 400) is False

    with pytest.raises(ConnectorHTTPError) as error:
        connector._api_get_response(url="https://example.invalid", params={})

    assert error.value.status_code == 400
    session.get.assert_called_once()


def test_uses_configured_connect_and_read_timeouts():
    session = MagicMock(spec=requests.Session)
    response = MagicMock(spec=requests.Response)
    response.status_code = 200
    response.json.return_value = {"results": []}
    session.get.return_value = response
    connector = HTTPTestConnector(
        connect_timeout_seconds=1.5,
        read_timeout_seconds=7.0,
        session=session,
    )

    connector._api_get_response(url="https://example.invalid", params={})

    session.get.assert_called_once_with(
        "https://example.invalid",
        params={},
        headers=None,
        timeout=(1.5, 7.0),
    )


def test_invalid_json_raises_defensive_response_error():
    session = MagicMock(spec=requests.Session)
    response = MagicMock(spec=requests.Response)
    response.status_code = 200
    response.json.side_effect = ValueError("invalid JSON")
    session.get.return_value = response
    connector = HTTPTestConnector(session=session)

    with pytest.raises(ConnectorResponseError, match="invalid JSON"):
        connector._api_get_response(url="https://example.invalid", params={})


def test_transport_errors_do_not_log_credentials(caplog):
    session = MagicMock(spec=requests.Session)
    session.get.side_effect = requests.ConnectionError(
        "connection failed for https://example.invalid/?app_key=super-secret"
    )
    connector = HTTPTestConnector(session=session)

    with caplog.at_level(logging.ERROR), pytest.raises(ConnectorRequestError) as error:
        connector._api_get_response(
            url="https://example.invalid",
            params={"app_key": "super-secret"},
        )

    assert "super-secret" not in caplog.text
    assert "super-secret" not in str(error.value)
