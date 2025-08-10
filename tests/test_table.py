"""Tests"""

import json
from unittest.mock import patch

import pytest
from requests.exceptions import HTTPError

from pxwebpy import PxTable


@pytest.fixture
def url():
    return "api.some_pxweb.com"


@pytest.fixture
def query_dict():
    return {
        "query": [
            {
                "code": "Region",
                "selection": {
                    "filter": "vs:RegionKommun07EjAggr",
                    "values": ["0180", "1280", "1480"],
                },
            },
            {
                "code": "Alder",
                "selection": {"filter": "item", "values": ["tot20+"]},
            },
            {
                "code": "ContentsCode",
                "selection": {"filter": "item", "values": ["HE0110J7"]},
            },
            {
                "code": "Tid",
                "selection": {"filter": "item", "values": ["2021"]},
            },
        ],
        "response": {"format": "json-stat2"},
    }


@pytest.fixture
def query_str(query_dict):
    return json.dumps(query_dict)


@pytest.fixture
def query_file():
    return "tests/queries/valid_query.json"


@pytest.mark.parametrize(
    "query_fixture",
    ["query_str", "query_dict", "query_file"],
)
def test_query_setter_valid(query_fixture, url, query_dict, request):
    """The query should handle valid inputs correctly (string, file path, dict)"""
    query = request.getfixturevalue(query_fixture)
    table = PxTable(url=url, query=query)
    assert table.query == query_dict


def test_query_setter_invalid(url):
    """Invalid query should produce an error"""
    with pytest.raises(ValueError):
        PxTable(url=url, query="tests/queries/invalid_query.json")

    with pytest.raises(TypeError):
        PxTable(url=url, query=[1, 2, 3])


def test_get_data_invalid_url(query_dict):
    """Invalid URL should raise a ValueError"""
    with pytest.raises(ValueError):
        table = PxTable(url="invalid_url", query=query_dict)
        table.get_data()


def test_create_query(url, snapshot):
    """Creating a query requires a specific format"""
    table = PxTable(url=url)

    # Values must always be strings
    with pytest.raises(ValueError):
        table.create_query({"År": [2023, 2024, 2025]})  # type: ignore

    # Values must be in a list
    with pytest.raises(ValueError):
        table.create_query({"Län": "Stockholms län"})  # type: ignore

    # Create a query
    with open(
        "tests/mock/response_table_variables.json", "r"
    ) as expected_response:
        mock_response = json.load(expected_response)

    with patch("requests_cache.CachedSession.get") as mock_get:
        mock_get.return_value.ok = True
        mock_get.return_value.json.return_value = mock_response
        table.create_query({"Region": ["Riket"], "Tid": ["*"]})

    assert snapshot == table.query, "Created query does not match expected."


def test_invalid_table_variables(url):
    """Invalid JSON structure in response should raise a KeyError"""
    table = PxTable(url=url)

    with pytest.raises(KeyError):
        with patch("requests_cache.CachedSession.get") as mock_get:
            mock_get.return_value.ok = True
            mock_get.return_value.json.return_value = {"key": "value"}

            table.get_table_variables()


def test_get_table_variables(url):
    """Getting table variables should return a dict"""
    table = PxTable(url=url)
    with open(
        "tests/mock/response_table_variables.json", "r"
    ) as expected_response:
        mock_response = json.load(expected_response)

    with patch("requests_cache.CachedSession.get") as mock_get:
        mock_get.return_value.ok = True
        mock_get.return_value.json.return_value = mock_response

        variables = table.get_table_variables()

        assert isinstance(variables, dict)


def test_send_request(url):
    """Sending a request and receiving an error response should raise an exception"""
    table = PxTable(url=url)

    with pytest.raises(HTTPError):
        with patch("requests_cache.CachedSession.get") as mock_get:
            mock_get.return_value.ok = False

            table.get_table_variables()


@pytest.mark.parametrize(
    "api_endpoint",
    [
        "fi",  # https://statfin.stat.fi:443/PxWeb/api/v1/sv/StatFin/vaerak/statfin_vaerak_pxt_11rc.px
        "gl",  # https://bank.stat.gl:443/api/v1/en/Greenland/UD/UD40/UD4040/UDXUMG3.px
        "no",  # https://data.ssb.no/api/v0/no/table/07221/
        "se",  # https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1
    ],
)
def test_get_data(url, snapshot, api_endpoint):
    """Checks functionality of get_data() against mock Px Web API responses"""
    with open(
        f"tests/queries/query_{api_endpoint}.json", mode="r"
    ) as json_file:
        query = json.load(json_file)

    with open(
        f"tests/mock/response_{api_endpoint}.json", mode="r", encoding="utf-8"
    ) as response:
        mock_response = json.load(response)

    # Patch requests post method with a mock response
    with patch("requests_cache.CachedSession.post") as mock_post:
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = mock_response

        # PxTable expects a URL and a query to get data
        table = PxTable(url=url, query=query)
        table.get_data()

        # Compare the dataset with the expected result
        assert snapshot == table.dataset, (
            f"Dataset does not match expected result for '{api_endpoint}'"
        )
