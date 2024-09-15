"""Helper to make fetching data from PxWeb a bit easier"""

import itertools
import json
from datetime import datetime
from enum import Enum
from pathlib import Path

import requests


class Context(Enum):
    QUERY = "create a query"
    VARIABLES = "get variables"
    DATA = "get data"


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"


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

    >>> tbl = PxTable(url="https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1")

    >>> tbl.create_query({"år": ["2021"]})

    >>> tbl.get_data()

    >>> print(tbl)

    PxTable(url='https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1',
        query={'query': [{'code': 'Tid', 'selection': {'filter': 'item', 'values': ['2021']}}], 'response': {'format': 'json-stat2'}},
        metadata={'label': 'Sammanräknad förvärvsinkomst för boende i Sverige hela året efter ålder, tabellinnehåll och år', 'source': 'SCB', 'updated': '2023-01-10T10:42:00Z'},
        fetched=2023-10-29 14:21:57.628639,
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
        self.fetched: datetime | None = None

        if query:
            try:
                response_format = self.query["response"]["format"]
                if response_format != "json-stat2":
                    raise TypeError(
                        f"""Response format must be 'json-stat2', \
                        got '{self.query["response"]["format"]}'."""
                    )
            except KeyError as err:
                raise KeyError(f"Missing key: {err}") from err

    def __repr__(self) -> str:
        return f"""PxTable(url='{self.url}',
        query={self.query},
        metadata={self.metadata},
        fetched={self.fetched},
        dataset={self.dataset})"""

    def get_data(self) -> None:
        """Get data from the API"""
        json_data = self.__send_request(HttpMethod.POST, Context.DATA)

        self.dataset = self.__unpack_data(json_data)

        metadata_keys = ["label", "note", "source", "updated"]

        self.metadata = self.metadata = {
            key: json_data.get(key) for key in metadata_keys
        }

        self.fetched = datetime.now()

    def __unpack_data(self, json_data: dict) -> list[dict]:
        """
        Takes the json-stat2 and turns it into a list of dicts that can
        be used to convert into a dataframe, using either pandas or polars.
        """

        dimension_categories = {}

        # Go over each dimension
        for dim in json_data["dimension"]:
            # If the dimension has extension data along with a key for show, use
            # that to determine the values shown in the output
            try:
                show_value = json_data["dimension"][dim]["extension"]["show"]

                if show_value == "code_value":
                    values = [
                        str(k) + " " + str(v)
                        for k, v in json_data["dimension"][dim]["category"][
                            "label"
                        ].items()
                    ]

                elif show_value == "code":
                    values = [
                        str(k)
                        for k in json_data["dimension"][dim]["category"]["label"].keys()
                    ]

                elif show_value == "value":
                    values = [
                        str(v)
                        for v in json_data["dimension"][dim]["category"][
                            "label"
                        ].values()
                    ]
                    # Raise an error if we hit some value in the show key that we don't know how to handle

                else:
                    raise ValueError(
                        f"""Unexpected show value. Expected "code", "value" or "code_value", got: {show_value}"""
                    )

            # If there's no show key at all we default to using the label values
            except KeyError:
                values = [
                    str(v)
                    for v in json_data["dimension"][dim]["category"]["label"].values()
                ]

            dimension_categories.update({json_data["dimension"][dim]["label"]: values})

        # The result is a list of dicts with the dimension as key and product of the category labels for values
        result = [
            dict(zip(dimension_categories.keys(), x))
            for x in itertools.product(*dimension_categories.values())
        ]

        # Finally add the value for each dict representing a row
        for value, dict_row in zip(json_data["value"], result):
            dict_row["value"] = value

        return result

    def __is_path(self, query: str) -> bool:
        """Check if query is a path or not"""
        try:
            path = Path(query)
            return path.exists()
        except OSError:
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
            if self.__is_path(query):
                with open(query, mode="r", encoding="utf-8") as read_file:
                    self.__query = json.load(read_file)
            else:
                try:
                    self.__query = json.loads(query)
                except json.JSONDecodeError as err:
                    raise ValueError(
                        "Provided value could not be decoded as JSON."
                    ) from err
        else:
            self.__query = query

    def __send_request(self, method: HttpMethod, context: Context) -> dict:
        """Send either a GET or POST request to the API"""
        if not self.url:
            raise ValueError("No URL is set.")

        if method == HttpMethod.GET:
            response = requests.get(self.url, timeout=10)
        elif method == HttpMethod.POST:
            if not self.query:
                raise ValueError("No query is set.")
            response = requests.post(self.url, json=self.query, timeout=10)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise RuntimeError(
                f"Failed to {context.value}: {response.status_code}: {response.reason}"
            )

        return response.json()

    def create_query(self, query: dict[str, list[str]]) -> None:
        """
        This function assumes the keys with list of values are the textual display names and will convert them into the identifier code and values
        that the API expects.

        Parameters
        ----------
        query : dict[str, list[str]]
            A dict where each key represents a variable name and its values is a list of values to filter.

        Example
        --------
        Creating a simple query:

        >>> tbl = PxTable(url="https://api.scb.se/OV0104/v1/doris/sv/ssd/START/ME/ME0104/ME0104D/ME0104T4")

        >>> tbl.create_query({"valår": ["2014", "2018", "2022"], "region": "Riket")

        >>> tbl.query

        {'query': [{'code': 'Region',
        'selection': {'filter': 'item', 'values': ['00']}}],
        'response': {'format': 'json-stat2'}}
        """

        # Check that the query structure holds up
        for key, value in query.items():
            if not isinstance(key, str):
                raise ValueError("Keys in the query must be `string`.")

            if not isinstance(value, list) or not all(
                isinstance(v, str) for v in value
            ):
                raise ValueError("Values in the quest must be a `list` of `strings`.")

        json_data = self.__send_request(HttpMethod.GET, Context.QUERY)
        conversion_mapping = {}

        # TODO Handle possible key errors. Also cache code responses.
        for variable in json_data["variables"]:
            text = variable["text"]
            code = variable["code"]
            value_texts = variable["valueTexts"]
            values = variable["values"]

            conversion_mapping[text] = {
                "code": code,
                "values": {
                    value_text: value for value_text, value in zip(value_texts, values)
                },
            }

        conversion = []

        for key, values in query.items():
            if key in conversion_mapping:
                code = conversion_mapping[key]["code"]
                value_map = conversion_mapping[key]["values"]
                converted_values = [value_map[value] for value in values]
                conversion.append(
                    {
                        "code": code,
                        "selection": {
                            "filter": "item",
                            "values": converted_values,
                        },
                    }
                )
        self.query = {"query": conversion, "response": {"format": "json-stat2"}}

    def get_table_variables(self) -> dict | None:
        """
        Returns a dict of variables and the respective values from the table.
        """
        json_data = self.__send_request(HttpMethod.GET, Context.VARIABLES)
        try:
            result = {}

            for var in json_data["variables"]:
                text = var["text"]
                values = var["valueTexts"]
                if text and values:
                    result[text] = values

            return result
        except KeyError as err:
            raise KeyError(f"Failed to unpack, missing key: {err}") from err
