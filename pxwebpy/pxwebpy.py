"""Helper to make fetching data from PxWeb a bit easier"""
from pathlib import Path
import json
from json import JSONDecodeError
from warnings import warn
import itertools
from datetime import datetime
import requests


class PxWeb:
    """
    A helper object to get data from the PxWeb API.
    The table dataset is collected as a long format.

    Parameters
    ----------
    url : str
        The PxWeb API URL for the table to query.
    query :  str or Path
        The query must be in JSON format, supplied either as a string or Path to a file.
    autofetch : bool, default True
        Whether to automatically fetch data from the URL upon instantiation of the object.

    Example
    --------
    Fetching data with a very simple query and turning it into a Pandas dataframe:

    >>> import pandas as pd

    >>> URL = "https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1"

    >>> QUERY = "
            {
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
        "

    >>> tbl = PxWeb(URL, QUERY)
    >>> print(tbl)

    PxWeb(url='https://api.scb.se/OV0104/v1/doris/sv/ssd/START/HE/HE0110/HE0110A/SamForvInk1',
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

    def __init__(self, url, query, autofetch=True) -> None:
        self.url: str = url
        self.query: dict | Path = query
        self.dataset: list[dict] | None = None
        self.metadata: dict = {key: None for key in ["label", "source", "updated"]}
        self.last_refresh: datetime | None = None

        try:
            response_format = self.query["response"]["format"]
            if response_format != "json-stat2":
                raise TypeError(
                    f"""Response format must be 'json-stat2', \
                    got '{self.query["response"]["format"]}'."""
                )
        except KeyError as err:
            raise KeyError(f"Invalid query format. {err} not found.") from err

        if autofetch:
            self.get_data()

    def __repr__(self):
        return f"""PxWeb(url='{self.url}',
        query={self.query},
        metadata={self.metadata},
        last_refresh={self.last_refresh},
        dataset={self.dataset})"""

    def get_data(self) -> None:
        """Get data from the API"""
        response = requests.post(self.url, json=self.query, timeout=10)
        if response.status_code == 200:
            json_data = json.loads(response.text)

            self.dataset = self._unpack_data(json_data)

            # Check if any metadata is missing and warn the user.
            missing_metadata = []

            try:
                label = json_data["label"]
            except KeyError:
                missing_metadata.append("label")

            try:
                source = json_data["source"]
            except KeyError:
                missing_metadata.append("source")

            try:
                updated = json_data["updated"]
            except KeyError:
                missing_metadata.append("updated")

            if missing_metadata:
                warn(
                    f"Response is missing the following \
                        metadata keys: {', '.join(missing_metadata)}."
                )

            self.metadata.update({"label": label, "source": source, "updated": updated})
            self.last_refresh = datetime.now()

        else:
            warn(f"Failed to retrieve data: {response.status_code}: {response.reason}")

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

    @property
    def query(self) -> dict:
        """
        Getter for the query.
        """
        return self.__query

    @query.setter
    def query(self, query: Path | str) -> None:
        """
        Set the JSON query, accepting either a `Path` to a file or a `str`.
        """
        match query:
            case Path():
                with open(query, mode="r", encoding="utf-8") as read_file:
                    try:
                        self.__query = json.load(read_file)
                    except JSONDecodeError as err:
                        print(f"An error occured: {err}")
                        raise ValueError(
                            "Provided file could not be decoded as JSON."
                        ) from err
            case str():
                try:
                    self.__query = json.loads(query)
                except JSONDecodeError as err:
                    print(f"An error occured: {err}")
                    raise ValueError(
                        "Provided string could not be decoded as JSON."
                    ) from err
            case None:
                raise ValueError("Query cannot be None.")
            case _:
                raise TypeError(
                    f"Invalid input for `query`. \
                    Expected `str` or `Path`, got {type(query)!r}."
                )
