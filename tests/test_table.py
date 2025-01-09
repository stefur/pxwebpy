"""Tests"""

import json
from unittest.mock import patch

import pytest
from requests.exceptions import HTTPError

from pxwebpy import PxTable

URL = "https://www.example.com"

QUERY = """
{
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:RegionKommun07EjAggr",
        "values": [
          "0180",
          "1280",
          "1480"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "tot20+"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "HE0110J7"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2021"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
"""


def test_query_setter_with_string():
    """The query should be able to handle a string representing a JSON structure"""
    table = PxTable(url=URL, query=QUERY)
    json_query = json.loads(QUERY)
    assert table.query == json_query


def test_query_setter_with_file():
    """The query should be able to handle a string representing a path to a file containing a JSON structure"""
    table = PxTable(url=URL, query="tests/valid_query.json")
    with open("tests/valid_query.json", mode="r", encoding="utf-8") as read_file:
        json_query = json.load(read_file)

    assert table.query == json_query


def test_query_setter_with_dict():
    """The query should be able to handle a dict representing a JSON structure"""
    json_query = json.loads(QUERY)
    table = PxTable(url=URL, query=QUERY)
    assert table.query == json_query


def test_query_setter_with_invalid_json():
    """Invalid query should produce an error"""
    with pytest.raises(ValueError):
        PxTable(url=URL, query="tests/invalid_query.json")


def test_query_setter_with_invalid_type():
    """Wrong data type should raise a TypeError"""
    with pytest.raises(TypeError):
        PxTable(url=URL, query=[QUERY])


def test_get_data_invalid_url():
    """Invalid URL should raise a ValueError"""
    with pytest.raises(ValueError):
        table = PxTable(url="invalid_url", query=QUERY)
        table.get_data()


def test_create_query():
    """Creating a query requires a specific format"""
    table = PxTable(url=URL)

    # Values must always be strings
    with pytest.raises(ValueError):
        table.create_query({"År": [2023, 2024, 2025]})  # type: ignore

    # Values must be in a list
    with pytest.raises(ValueError):
        table.create_query({"Län": "Stockholms län"})  # type: ignore

    # Create a query
    with open("tests/mock/response_table_variables.json", "r") as expected_response:
        mock_response = json.load(expected_response)

    with patch("requests_cache.CachedSession.get") as mock_get:
        mock_get.return_value.ok = True
        mock_get.return_value.json.return_value = mock_response
        table.create_query({"Region": ["Riket"], "Tid": ["*"]})


def test_invalid_table_variables():
    """Invalid JSON structure in response should raise a KeyError"""
    table = PxTable(url=URL)

    with pytest.raises(KeyError):
        with patch("requests_cache.CachedSession.get") as mock_get:
            mock_get.return_value.ok = True
            mock_get.return_value.json.return_value = {"key": "value"}

            table.get_table_variables()


def test_get_table_variables():
    """Getting table variables should return a dict"""
    table = PxTable(url=URL)
    with open("tests/mock/response_table_variables.json", "r") as expected_response:
        mock_response = json.load(expected_response)

    with patch("requests_cache.CachedSession.get") as mock_get:
        mock_get.return_value.ok = True
        mock_get.return_value.json.return_value = mock_response

        variables = table.get_table_variables()

        assert isinstance(variables, dict)


def test_send_request():
    """Sending a request and receiving an error response should raise an exception"""
    table = PxTable(url=URL)

    with pytest.raises(HTTPError):
        with patch("requests_cache.CachedSession.get") as mock_get:
            mock_get.return_value.ok = False

            table.get_table_variables()


def test_get_data():
    """Checks functionality of get_data() against mock Px Web API responses"""
    with open("tests/queries.json", "r") as queries:
        query_data = json.load(queries)

    for query in query_data["queries"]:
        url = query["url"]
        query_params = query["query"]
        response_json = query["expected_response"]
        result_json = query["expected_result"]

        with open(result_json, "r") as result_file:
            expected_result = json.load(result_file)

        with open(response_json, mode="r", encoding="utf-8") as response_file:
            mock_response = json.load(response_file)

        # Patch requests post method with a mock response
        with patch("requests_cache.CachedSession.post") as mock_post:
            mock_post.return_value.ok = True
            mock_post.return_value.json.return_value = mock_response

            # PxTable expects a URL and a query to get data
            table = PxTable(url=url, query=query_params)
            table.get_data()

            # Compare the dataset with the expected result
            assert table.dataset == expected_result, (
                f"Dataset does not match expected result for query: {url}, {query_params}"
            )
