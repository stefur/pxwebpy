"""Helper to make fetching data from PxWeb a bit easier"""
from pathlib import Path
import json
from json import JSONDecodeError
from warnings import warn
import itertools
import requests


class PxWeb:
    """
    A helper to manage and get data from the PxWeb API.
    If provided with both URL and query when instantiating ,
    it will automatically try to get data from the API.
    This behaviour can be turned off using the
    parameter `autofetch` <bool> (Default: True).
    """

    def __init__(self, url=None, json_query=None, autofetch=True) -> None:
        """
        Initialize the PxWeb instance.

        :param url: The PxWeb API URL.
        :param json_query: The query in JSON format.
        :param autofetch: Whether to automatically fetch data upon instantiation.
        """
        self.url: str = url
        self.query: dict | None = json_query
        self.data: dict | None = None
        self.autofetch: bool = autofetch

        if self.url is not None and self.query is not None and self.autofetch:
            self.get_data()

    def get_data(self, json_query=None) -> None:
        """Get data from the API"""
        if json_query is not None:
            self.query = json_query
        if self.query is not None:
            response = requests.post(self.url, json=self.query, timeout=10)
            if response.status_code == 200:
                self.data = json.loads(response.text)
            else:
                warn(
                    f"Warning: failed to retrieve data, \
                        received: {response.status_code}: {response.reason}"
                )
        else:
            raise ValueError("Cannot get data if query is None.")

    def to_dicts(self) -> list[dict] | None:
        """
        Takes the response json-stat and turns it into a list of dicts that can
        be used to convert into a dataframe, using either pandas or polars.
        """
        if self.query is None or self.data is None:
            raise ValueError("`query` and/or `data` cannot be None.")
        if self.query["response"]["format"] != "json-stat":
            raise TypeError("Currently only response format 'json-stat' is supported.")
        query_dims = [dim["code"] for dim in self.query["query"]]
        data_dims = self.data["dataset"]["dimension"]

        category_labels = {}
        for dim in data_dims:
            if dim in query_dims and dim != "ContentsCode":
                label = data_dims[dim]["category"]["label"]
                category_labels.update({data_dims[dim]["label"]: label.values()})
            if dim == "ContentsCode":
                value_label = list(data_dims[dim]["category"]["label"].values())[0]

        result = [
            dict(zip(category_labels.keys(), x))
            for x in itertools.product(*category_labels.values())
        ]

        all_values = self.data["dataset"]["value"]

        for value, dict_row in zip(all_values, result):
            dict_row.update({value_label: value})

        return result

    def toggle_autofetch(self, enable: bool) -> None:
        """
        Toggle the autofetch behavior.

        :param enable: True to enable autofetch, False to disable.
        """
        self.autofetch = enable

    @property
    def query(self) -> dict | None:
        """
        Getter for the query

        :return: The query as a dictionary.
        """
        return self.__query

    @query.setter
    def query(self, json_query: Path | str | None) -> None:
        """
        Set the query, accepting different variants

        :param json_query: The query in JSON format or a path to a JSON file.
        """
        match json_query:
            case Path():
                with open(json_query, mode="r", encoding="utf-8") as read_file:
                    try:
                        self.__query = json.load(read_file)
                    except JSONDecodeError as err:
                        print(f"An error occured: {err}")
                        raise ValueError(
                            "Provided file could not be decoded as JSON."
                        ) from err

            case str():
                try:
                    self.__query = json.loads(json_query)
                except JSONDecodeError as err:
                    print(f"An error occured: {err}")
                    raise ValueError(
                        "Provided string could not be decoded as JSON."
                    ) from err
            case None:
                self.__query = None
            case _:
                raise TypeError(
                    f"Invalid input for `json_query`. \
                    Expected `str` or `Path`, got {type(json_query)!r}."
                )

    @query.deleter
    def query(self) -> None:
        """
        Delete the query, setting it to None.
        """
        self.__query = None
