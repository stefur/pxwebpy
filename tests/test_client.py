import time as real_time
from collections.abc import Iterator
from unittest.mock import patch

import pytest
import responses
from requests.exceptions import HTTPError
from utils import BASE_URL, load_response

from pxweb._internal.client import Client


@pytest.fixture
def client() -> Iterator[Client]:
    with responses.RequestsMock() as rsps:
        rsps.add(
            method=responses.GET,
            url=BASE_URL + "/config",
            json=load_response("config.json"),
        )
        yield Client(url=BASE_URL, timeout=10, disable_cache=True)


def test_init_sets_defaults(client: Client) -> None:
    assert client.configuration["maxCallsPerTimeWindow"] == 3
    assert client.configuration["timeWindow"] == 1
    assert client.params["lang"] == "sv"


@responses.activate
def test_call_adds_output_format(client: Client) -> None:
    responses.add(
        method=responses.GET,
        url=BASE_URL + "/tables/TAB6471/data",
        json={"data": "value"},
    )
    client.call("/tables/TAB6471/data")
    assert (
        responses.calls[-1].request.url
        == BASE_URL + "/tables/TAB6471/data?lang=sv&outputFormat=json-stat2"
    )


@responses.activate
def test_call_respects_retry(client: Client) -> None:
    responses.add(
        method=responses.GET,
        url=BASE_URL + "/rate-limited",
        status=429,
        headers={"Retry-After": "1"},
    )
    responses.add(
        method=responses.GET,
        url=BASE_URL + "/rate-limited",
        json={"done": True},
        status=200,
    )
    with patch("time.sleep", return_value=None) as mock_sleep:
        result = client.call("/rate-limited")
        assert result["done"] is True
        assert len(responses.calls) == 2
        mock_sleep.assert_called_once_with(1.0)


@responses.activate
def test_call_raises_http_error(client: Client) -> None:
    """HTTPError should be raised on bad input. Since the logic in call() is non-trivial it makes sense to ensure this works and extracts the errors properly."""
    responses.add(
        method=responses.GET,
        url=BASE_URL + "/bad-input",
        json={
            "type": "Failure",
            "title": "Bad Request",
            "status": 400,
            "detail": "Invalid input",
            "instance": "/bad",
        },
        status=400,
    )
    with pytest.raises(HTTPError) as err:
        client.call("/bad-input")
    assert "400" in str(err.value)
    assert "Invalid input" in str(err.value)


def test_rate_limit_blocks_when_limit_exceeded(client: Client) -> None:
    # Simulate 3 calls right now (equal to max_calls in the made up config)
    now = real_time.monotonic()
    client.call_timestamps = [now - 0.1, now - 0.2, now - 0.3]

    with patch("time.sleep", return_value=None) as mock_sleep:
        client.rate_limit()
        assert mock_sleep.called


def test_rate_limit_allows_when_under_limit(client: Client) -> None:
    # Simulate 1 old call outside window
    client.call_timestamps = [real_time.monotonic() - 2]

    with patch("time.sleep", return_value=None) as mock_sleep:
        client.rate_limit()
        assert not mock_sleep.called
