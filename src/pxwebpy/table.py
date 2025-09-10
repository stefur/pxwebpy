"""Make fetching data from PxWeb a bit easier"""

import json
from datetime import datetime
from pathlib import Path
from tempfile import gettempdir
from typing import Optional, Union

from requests_cache import CachedSession

from . import _api, _data


class PxTable:
    """
    A table object to retrieve data from the PxWeb API.

    Parameters
    ----------
    url : str
        The PxWeb API URL for the table to query.
    query : str | dict | None
        The query must be a JSON structure, supplied either as a dict, string or a string representing a path to a file.
    timeout : int
        The timeout (in seconds) for the query sent to the API. This defines the maximum time the client will wait
        for the server to respond before a a `ReadTimeout` exception will be raised. Defaults to 30 seconds.

    Example
    --------
    Fetching data with a very simple query and turning it into a Pandas dataframe:

    >>> from pxwebpy import PxTable
    >>> import pandas as pd

    >>> tbl = PxTable(url="https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1")

    >>> tbl.create_query({"region": ["17*"], "år": ["*"], "ålder": ["totalt 16+ år"], "ContentsCode": ["Medianinkomst, tkr"]})

    >>> tbl.get_data()

    >>> print(tbl)

    PxTable(url='https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1',
            query={'query': [{'code': 'Region', 'selection': {'filter': 'all', 'values': ['17*']}}, {'code': 'Tid', 'selection': {'filter': 'all', 'values': ['*']}}, {'code': 'Alder', 'selection': {'filter': 'item', 'values': ['tot16+']}}, {'code': 'ContentsCode', 'selection': {'filter': 'item', 'values': ['HE0110J8']}}], 'response': {'format': 'json-stat2'}},
            metadata={'label': 'Sammanräknad förvärvsinkomst, medianinkomst för boende i Sverige hela året, tkr efter region, ålder, tabellinnehåll och år', 'note': None, 'source': 'SCB', 'updated': '2024-01-12T05:52:00Z'},
            fetched=2024-10-11 20:13:35.475254,
            dataset=[{'region': '17 Värmlands län', 'ålder': 'totalt 16+ år', 'tabellinnehåll': 'Medianinkomst, tkr', 'år': '1999', 'value': 153.4}, {'region': '17 Värmlands län', 'ålder': 'totalt 16+ år', 'tabellinnehåll': 'Medianinkomst, tkr', 'år': '2000', 'value': 157.7}, {'region': '17 Värmlands län', 'ålder': 'totalt 16+ år', 'tabellinnehåll': 'Medianinkomst, tkr', 'år': '2001', 'value': 163.2}, ...

    >>> df = pd.DataFrame(tbl.dataset)
    >>> print(df)

                region          ålder      tabellinnehåll    år  value
    0    17 Värmlands län  totalt 16+ år  Medianinkomst, tkr  1999  153.4
    1    17 Värmlands län  totalt 16+ år  Medianinkomst, tkr  2000  157.7
    2    17 Värmlands län  totalt 16+ år  Medianinkomst, tkr  2001  163.2
    3    17 Värmlands län  totalt 16+ år  Medianinkomst, tkr  2002  170.0
    4    17 Värmlands län  totalt 16+ år  Medianinkomst, tkr  2003  176.8
    ..                ...            ...                 ...   ...    ...
    403       1785 Säffle  totalt 16+ år  Medianinkomst, tkr  2018  239.8
    404       1785 Säffle  totalt 16+ år  Medianinkomst, tkr  2019  246.3
    405       1785 Säffle  totalt 16+ år  Medianinkomst, tkr  2020  250.1
    406       1785 Säffle  totalt 16+ år  Medianinkomst, tkr  2021  255.5
    407       1785 Säffle  totalt 16+ år  Medianinkomst, tkr  2022  273.4

    [408 rows x 5 columns]
    """

    _tmp_dir = gettempdir()
    _cache = Path(_tmp_dir) / "pxwebpy_cache"

    def __init__(self, url, query=None, timeout=30) -> None:
        self.url: str = url
        self.query = query
        self.timeout: int = timeout
        self.dataset: Optional[list[dict]] = None
        self.metadata: dict = {
            key: None for key in ["label", "note", "source", "updated"]
        }
        self.fetched: Optional[datetime] = None
        self.__session = CachedSession(cache_name=self._cache, ttl=600)

    def __repr__(self) -> str:
        return f"""PxTable(url='{self.url}',
        query={self.query},
        timeout={self.timeout},
        metadata={self.metadata},
        fetched={self.fetched},
        dataset={self.dataset})"""

    def __len__(self) -> int:
        return len(self.dataset)

    def __eq__(self, other) -> bool:
        return self.dataset == other

    def get_data(self) -> None:
        """Get data from the API, modifying the object in-place."""

        json_data = _api.call(
            session=self.__session,
            url=self.url,
            query=self.query,
            timeout=self.timeout,
        )

        self.dataset = _data.unpack_table_data(json_data)

        metadata_keys = ["label", "note", "source", "updated"]

        self.metadata = self.metadata = {
            key: json_data.get(key) for key in metadata_keys
        }

        self.fetched = datetime.now()

    def __is_path(self, query: str) -> bool:
        """Check if query is a path or not"""
        try:
            return Path(query).is_file()
        except OSError:
            return False

    @property
    def query(self) -> Optional[dict]:
        """
        Getter for the query.
        """
        return self.__query

    @query.setter
    def query(
        self,
        query: Optional[
            Union[
                str,
                dict,
            ]
        ],
    ) -> None:
        """
        Set the JSON query from a string representing a path or a JSON structure that is either a string or a dict.
        """

        if query is not None and not isinstance(query, (str, dict)):
            raise TypeError(
                f"""Invalid input for `query`.
                    Expected `str`, `dict` or `None`, got {type(query)!r}."""
            )

        if isinstance(query, dict):
            try:
                if (response_format := query["response"]["format"]) != "json-stat2":
                    raise TypeError(
                        f"""Response format must be 'json-stat2', \
                        got '{response_format}'."""
                    )
            except KeyError as err:
                raise KeyError(f"Missing key: {err}") from err

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

    def create_query(self, query_struct: dict[str, list[str]]) -> None:
        """
        This function will convert any textual display names of variables and value into the identifier code and values
        that the API expects.
        The creation of a query modifies the object in-place.

        Parameters
        ----------
        query : dict[str, list[str]]
            A dict where each key represents a variable name and its values is a list of values to filter.

        Example
        --------
        Creating a simple query:

        >>> tbl = PxTable(url="https://api.scb.se/OV0104/v1/doris/sv/ssd/START/ME/ME0104/ME0104D/ME0104T4")

        >>> tbl.create_query({"valår": ["*"], "region": ["Riket"]})

        >>> tbl.query

        {'query': [{'code': 'Tid', 'selection': {'filter': 'all', 'values': ['*']}},
        {'code': 'Region', 'selection': {'filter': 'item', 'values': ['00']}}],
        'response': {'format': 'json-stat2'}}
        """

        # Check that the query structure holds up
        for key, value in query_struct.items():
            if not isinstance(key, str):
                raise ValueError("Keys in the query must be `string`.")

            if not isinstance(value, list) or not all(
                isinstance(v, str) for v in value
            ):
                raise ValueError("Values in the query must be a `list` of `strings`.")

        # Get the table variables and values
        json_data = _api.call(
            session=self.__session, url=self.url, timeout=self.timeout
        )

        query = _data.build_query(json_data=json_data, query=query_struct)

        self.query = {"query": query, "response": {"format": "json-stat2"}}

    def get_table_variables(self) -> Union[dict, None]:
        """
        Returns a dict of variables and the respective values with texts from the table.
        """
        json_data = _api.call(
            session=self.__session, url=self.url, timeout=self.timeout
        )

        return _data.unpack_table_variables(json_data)
