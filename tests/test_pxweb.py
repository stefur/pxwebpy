"""Tests"""
from pathlib import Path
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
    pxweb = PxWeb()
    pxweb.query = '{"key": "value"}'
    assert pxweb.query == {"key": "value"}


def test_query_setter_with_invalid_json_string():
    """Invalid query should produce an error"""
    pxweb = PxWeb()
    with pytest.raises(ValueError):
        pxweb.query = Path("tests/invalid_query.json")


def test_query_setter_with_invalid_type():
    """Wrong data type should raise a TypeError"""
    pxweb = PxWeb()
    with pytest.raises(TypeError):
        pxweb.query = ["query"]


def test_get_data_success():
    """Getting data should not fail"""
    pxweb = PxWeb(url=URL)
    pxweb.query = QUERY
    pxweb.get_data()
    assert pxweb.data is not None


def test_get_data_failure():
    """Invalid URL should raise a ValueError"""
    pxweb = PxWeb(url="invalid_url")
    pxweb.query = QUERY
    with pytest.raises(ValueError):
        pxweb.get_data()


def test_toggle_autofetch():
    """It should be possible to toggle the autofetch"""
    pxweb = PxWeb()
    assert pxweb.autofetch is True
    pxweb.toggle_autofetch(False)
    assert pxweb.autofetch is False
