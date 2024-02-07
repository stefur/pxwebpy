"""pxwebpy pulling some data to Pandas and Polars respectively"""
from pathlib import Path

import pandas as pd
import polars as pl  # type: ignore
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
    "format": "json-stat2"
  }
}
"""

# From a string
data1 = PxWeb(URL, QUERY)

# Same query, from a file
data2 = PxWeb(URL, Path("example_query.json"))

# The object can be instantiated without running the query immediately
# by setting the autofetch flag to `False`
data3 = PxWeb(URL, QUERY, autofetch=False)

# Instead we can fetch the data later on if we want to
data3.get_data()

# The get_data() function can be used to refresh data as well

# Everytime get_data() is called, the field "last_refresh"
# is updated with a timestamp (datetime).
data3.last_refresh

# The objects also contain the metadata of the PxWeb table
# such as "label", "source" and "updated".
data3.metadata

# The dataset itself can easily be turned into a pandas dataframe...
pandas_df = pd.DataFrame(data3.dataset)

# ...or a polars dataframe
polars_df = pl.DataFrame(data3.dataset)
