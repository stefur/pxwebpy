import pytest
from unittest.mock import patch, MagicMock
from requests import Response
from requests.exceptions import HTTPError
from pxweb._internal.client import Client
import time as real_time


def make_response(status_code=200, json_data=None, headers=None):
    mock = MagicMock(spec=Response)
    mock.status_code = status_code
    mock.ok = status_code == 200
    mock.json.return_value = json_data or {}
    mock.headers = headers or {}
    mock.reason = "OK" if status_code == 200 else "Error"
    return mock


@pytest.fixture
def config_response():
    return {
        "maxDataCells": 99999,
        "maxCallsPerTimeWindow": 3,
        "timeWindow": 1,
        "defaultLanguage": "en",
        "apiVersion": "2.0.0",
    }


@pytest.fixture
def client(config_response):
    with patch("requests_cache.CachedSession.send") as mock_send:
        mock_send.return_value = make_response(json_data=config_response)
        return Client(
            url="https://some.pxweb.api", timeout=10, disable_cache=True
        )


def test_init_sets_defaults(client):
    # Ensure it's all set up as expected
    assert client.max_calls == 3
    assert client.time_window == 1
    assert client.params["lang"] == "en"


def test_call_get_request(client):
    # Simulate GET response
    mock_response = make_response(json_data={"data": "value"})

    with patch.object(client.session, "send", return_value=mock_response):
        result = client.call("/test-get")
        assert result["data"] == "value"


def test_call_post_request(client):
    # Simulate POST response
    mock_response = make_response(json_data={"result": "posted"})
    query = {"foo": "bar"}

    with patch.object(client.session, "send", return_value=mock_response):
        result = client.call("/submit", query=query)
        assert result["result"] == "posted"


def test_call_respects_retry(client):
    # Simulate one 429, then success
    first = make_response(429, headers={"Retry-After": "1"})
    second = make_response(200, json_data={"done": True})

    with patch.object(
        client.session, "send", side_effect=[first, second]
    ) as mock_send:
        with patch("time.sleep", return_value=None) as mock_sleep:
            result = client.call("/rate-limited")
            assert result["done"] is True
            assert mock_send.call_count == 2
            mock_sleep.assert_called_once_with(1)


def test_call_raises_http_error(client):
    error_json = {
        "type": "Failure",
        "title": "Bad Request",
        "status": 400,
        "detail": "Invalid input",
        "instance": "/bad",
    }

    bad_response = make_response(400, json_data=error_json)

    with patch.object(client.session, "send", return_value=bad_response):
        with pytest.raises(HTTPError) as err:
            client.call("/bad-input")

        assert "400" in str(err.value)
        assert "Invalid input" in str(err.value)


def test_rate_limit_blocks_when_limit_exceeded(client):
    # Simulate 3 calls right now (equal to max_calls in the made up config)
    now = real_time.monotonic()
    client.call_timestamps = [now - 0.1, now - 0.2, now - 0.3]

    with patch("time.sleep", return_value=None) as mock_sleep:
        client.rate_limit()
        assert mock_sleep.called


def test_rate_limit_allows_when_under_limit(client):
    # Simulate 1 old call outside window
    client.call_timestamps = [real_time.monotonic() - 2]

    with patch("time.sleep", return_value=None) as mock_sleep:
        client.rate_limit()
        assert not mock_sleep.called
