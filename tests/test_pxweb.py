"""Tests"""
from pathlib import Path
import json
import pytest
from pxwebpy import PxWeb

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
    "format": "json-stat"
  }
}"""


def test_query_setter_with_json_string():
    """The query should also be able to handle a JSON string"""
    pxweb = PxWeb(url=URL, query=QUERY, autofetch=False)
    json_query = json.loads(QUERY)
    assert pxweb.query == json_query


def test_query_setter_with_invalid_json_string():
    """Invalid query should produce an error"""
    with pytest.raises(ValueError):
        PxWeb(url=URL, query=Path("tests/invalid_query.json"))


def test_query_setter_with_invalid_type():
    """Wrong data type should raise a TypeError"""
    with pytest.raises(TypeError):
        PxWeb(url=URL, query=[QUERY])


def test_get_data():
    """Getting data with and without autofetch"""
    fetch = PxWeb(url=URL, query=QUERY)
    assert fetch.dataset is not None
    no_fetch = PxWeb(url=URL, query=QUERY, autofetch=False)
    assert no_fetch.dataset is None


def test_get_data_failure():
    """Invalid URL should raise a ValueError"""
    with pytest.raises(ValueError):
        PxWeb(url="invalid_url", query=QUERY)


def test_toggle_autofetch():
    """It should be possible to toggle the autofetch"""
    pxweb = PxWeb(url=URL, query=QUERY)
    assert pxweb.autofetch is True
    pxweb.toggle_autofetch(False)
    assert pxweb.autofetch is False
