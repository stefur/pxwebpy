"""Tests"""

import json

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
    table = PxTable(url=URL, query=QUERY)
    json_query = json.loads(QUERY)
    assert table.query == json_query


def test_query_setter_with_file():
    """The query should be able to handle a string representing a path to a file containing a JSON structure"""
    table = PxTable(url=URL, query="tests/valid_query.json")
    with open("tests/valid_query.json", mode="r", encoding="utf-8") as read_file:
        json_query = json.load(read_file)

    assert table.query == json_query


def test_query_setter_with_invalid_json():
    """Invalid query should produce an error"""
    with pytest.raises(ValueError):
        PxTable(url=URL, query="tests/invalid_query.json")


def test_query_setter_with_invalid_type():
    """Wrong data type should raise a TypeError"""
    with pytest.raises(TypeError):
        PxTable(url=URL, query=[QUERY])


def test_get_data():
    """Getting data"""
    table = PxTable(url=URL, query=QUERY)
    assert table.dataset is None
    table.get_data()
    assert table.dataset is not None


def test_get_data_failure():
    """Invalid URL should raise a ValueError"""
    with pytest.raises(ValueError):
        table = PxTable(url="invalid_url", query=QUERY)
        table.get_data()


def test_mock_responses():
    """This test checks functionality against mock Px Web API responses"""
    with open("tests/queries.json", "r") as queries:
        query_data = json.load(queries)

    for query in query_data["queries"]:
        url = query["url"]
        query_params = query["query"]
        expected_response = query["expected_response"]
        expected_result = query["expected_result"]

        mock_api = PxTable("api.example.com", QUERY)

        # Load the expected response data from the referenced file
        with open(expected_response, mode="r", encoding="utf-8") as response_file:
            mock_response = json.load(response_file)

        unpacked_response = mock_api._unpack_data(mock_response)

        with open(expected_result, "r") as result_file:
            mock_result = json.load(result_file)

        # Compare the unpacked response wi data with the mock r response data
        assert (
            unpacked_response == mock_result
        ), f"Response does not match for query: {url}, {query_params}"
