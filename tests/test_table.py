"""Tests"""

import json
from unittest.mock import Mock, patch

import pytest
from pxwebpy.table import PxTable

URL = "https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1"

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
    table = PxTable(query=QUERY)
    json_query = json.loads(QUERY)
    assert table.query == json_query


def test_query_setter_with_file():
    """The query should be able to handle a string representing a path to a file containing a JSON structure"""
    table = PxTable(query="tests/valid_query.json")
    with open("tests/valid_query.json", mode="r", encoding="utf-8") as read_file:
        json_query = json.load(read_file)

    assert table.query == json_query


def test_query_setter_with_dict():
    """The query should be able to handle a dict representing a JSON structure"""
    json_query = json.loads(QUERY)
    table = PxTable(query=QUERY)
    assert table.query == json_query


def test_query_setter_with_invalid_json():
    """Invalid query should produce an error"""
    with pytest.raises(ValueError):
        PxTable(query="tests/invalid_query.json")


def test_query_setter_with_invalid_type():
    """Wrong data type should raise a TypeError"""
    with pytest.raises(TypeError):
        PxTable(query=[QUERY])


def test_get_data_invalid_url():
    """Invalid URL should raise a ValueError"""
    with pytest.raises(ValueError):
        table = PxTable(url="invalid_url", query=QUERY)
        table.get_data()


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
        with patch("requests.post") as mock_post:
            mock_post.return_value = Mock(status_code=200)
            mock_post.return_value.json.return_value = mock_response

            # PxTable expects a URL and a query to get data
            table = PxTable(url=url, query=query_params)
            table.get_data()

            # Compare the dataset with the expected result
            assert (
                table.dataset == expected_result
            ), f"Dataset does not match expected result for query: {url}, {query_params}"
