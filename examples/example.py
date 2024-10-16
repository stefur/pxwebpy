"""pxwebpy pulling some data to Pandas and Polars respectively"""

import pandas as pd
import polars as pl

from pxwebpy import PxTable

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
data1 = PxTable(URL, QUERY)

# Same query, from a file
data2 = PxTable(URL, "example_query.json")

# The object is instantiated without running the query immediately
data3 = PxTable(URL, QUERY)

# We can fetch the data later on
data3.get_data()

# The get_data() function can be used to refresh data as well.
# Though data is cached for 10 minutes.

# Everytime get_data() is called, the field "fetched"
# is updated with a timestamp (datetime).
data3.fetched

# The objects also contain the metadata of the PxTable table
# such as "label", "source" and "updated".
data3.metadata

# The dataset itself can easily be turned into a pandas dataframe...
pandas_df = pd.DataFrame(data3.dataset)

# ...or a polars dataframe
polars_df = pl.DataFrame(data3.dataset)
