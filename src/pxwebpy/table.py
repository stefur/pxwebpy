"""Helper to make fetching data from PxWeb a bit easier"""

import itertools
import json
from datetime import datetime
from pathlib import Path
from warnings import warn

import requests


class PxTable:
    """
    A table object to get data from the PxWeb API.
    The table dataset is collected as a long format.

    Parameters
    ----------
    url : str | None
        The PxWeb API URL for the table to query.
    query :  str | dict | None
        The query must be a JSON structure, supplied either as a dict, string or a string representing a path to a file.

    Example
    --------
    Fetching data with a very simple query and turning it into a Pandas dataframe:

    >>> from pxwebpy import PxTable
    >>> import pandas as pd

    >>> URL = "https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1"

    >>> QUERY = {
                "query": [
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

    >>> tbl = PxTable(URL, QUERY)
    >>> print(tbl)

    PxTable(url='https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1',
        query={'query': [{'code': 'Tid', 'selection': {'filter': 'item', 'values': ['2021']}}], 'response': {'format': 'json-stat2'}},
        metadata={'label': 'Sammanräknad förvärvsinkomst för boende i Sverige hela året efter ålder, tabellinnehåll och år', 'source': 'SCB', 'updated': '2023-01-10T10:42:00Z'},
        last_refresh=2023-10-29 14:21:57.628639,
        dataset=[{'ålder': 'totalt 16+ år', 'tabellinnehåll': 'Medelinkomst, tkr', 'år': '2021' ...

    >>> df = pd.DataFrame(tbl.dataset)
    >>> print(df)
                ålder      tabellinnehåll    år      value
    0   totalt 16+ år   Medelinkomst, tkr  2021      331.5
    1   totalt 16+ år  Medianinkomst, tkr  2021      301.5
    2   totalt 16+ år    Totalsumma, mnkr  2021  2779588.9
    3   totalt 16+ år      Antal personer  2021  8383640.0
    4        16-19 år   Medelinkomst, tkr  2021       28.1
    ..            ...                 ...   ...        ...
    71       80-84 år      Antal personer  2021   290684.0
    72         85+ år   Medelinkomst, tkr  2021      214.4
    73         85+ år  Medianinkomst, tkr  2021      200.1
    74         85+ år    Totalsumma, mnkr  2021    57529.3
    75         85+ år      Antal personer  2021   268320.0

    [76 rows x 4 columns]
    """

    def __init__(self, url=None, query=None) -> None:
        self.url: str | None = url
        self.query: dict | None = query
        self.dataset: list[dict] | None = None
        self.metadata: dict = {
            key: None for key in ["label", "note", "source", "updated"]
        }
        self.last_refresh: datetime | None = None

        if query:
            try:
                response_format = self.query["response"]["format"]
                if response_format != "json-stat2":
                    raise TypeError(
                        f"""Response format must be 'json-stat2', \
                        got '{self.query["response"]["format"]}'."""
                    )
            # TODO This should be a proper exception.
            except Exception as err:
                print(f"An error occured: {err}")
                raise Exception("Invalid query format.") from err

    def __repr__(self) -> str:
        return f"""PxTable(url='{self.url}',
        query={self.query},
        metadata={self.metadata},
        last_refresh={self.last_refresh},
        dataset={self.dataset})"""

    def get_data(self) -> None:
        """Get data from the API"""
        if self.url:
            response = requests.post(self.url, json=self.query, timeout=10)
            if response.status_code == 200:
                json_data = json.loads(response.text)

                self.dataset = self._unpack_data(json_data)

                metadata_keys = ["label", "note", "source", "updated"]

                self.metadata = self.metadata = {
                    key: json_data.get(key) for key in metadata_keys
                }

                self.last_refresh = datetime.now()

            else:
                warn(
                    f"Failed to retrieve data: {response.status_code}: {response.reason}"
                )
        else:
            warn("Cannot retrieve data. URL is empty.")

    def _unpack_data(self, response: dict) -> list[dict]:
        """
        Takes the response json-stat2 and turns it into a list of dicts that can
        be used to convert into a dataframe, using either pandas or polars.
        """
        if response is None:
            raise ValueError("response cannot be None.")

        response_dims = response["dimension"]

        category_labels = {}
        for dim in response_dims:
            label = response_dims[dim]["category"]["label"]
            category_labels.update({response_dims[dim]["label"]: label.values()})

        result = [
            dict(zip(category_labels.keys(), x))
            for x in itertools.product(*category_labels.values())
        ]

        all_values = response["value"]

        for value, dict_row in zip(all_values, result):
            dict_row.update({"value": value})

        return result

    def _is_path(self, query: str) -> bool:
        """Check if query is a path or not"""
        try:
            path = Path(query)
            return path.exists()
        except Exception:
            return False

    @property
    def query(self) -> dict:
        """
        Getter for the query.
        """
        return self.__query

    @query.setter
    def query(self, query: str | dict | None) -> None:
        """
        Set the JSON query from a string representing a path or a JSON structure that is either a string or a dict.
        """

        if not isinstance(query, str | dict | None):
            raise TypeError(
                f"""Invalid input for `query`.
                Expected `str`, `dict` or `None`, got {type(query)!r}."""
            )

        if isinstance(query, str):
            if self._is_path(query):
                with open(query, mode="r", encoding="utf-8") as read_file:
                    self.__query = json.load(read_file)
            else:
                try:
                    self.__query = json.loads(query)
                except Exception as err:
                    raise ValueError(
                        "Provided value could not be decoded as JSON."
                    ) from err
        else:
            self.__query = query
