"""pxwebpy pulling some data to Pandas and Polars respectively"""
from pathlib import Path

import pandas as pd
import polars as pl

from pxwebpy import pxwebpy

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
    "format": "csv"
  }
}"""

# From a string
data1 = pxwebpy.get_data(QUERY, URL)

# Same query, from a file
data2 = pxwebpy.get_data(Path("example_query.json"), URL)

pandas_df = pd.DataFrame.from_dict(data1)

polars_df = pl.DataFrame(data2)
