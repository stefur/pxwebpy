"""Helper to make fetching data from PxWeb a bit easier"""
from pathlib import Path
import json
from json import JSONDecodeError
from warnings import warn
import itertools
import requests


class PxWeb:
    """
    A helper class to get data from the PxWeb API.
    """

    def __init__(self, url, query, autofetch=True) -> None:
        """
        :param url: The PxWeb API URL.
        :param query: The query in JSON format.
        :param autofetch: Whether to automatically fetch data upon instantiation.
        """
        self.url: str = url
        self.query: dict = query
        self.dataset: list[dict] | None = None
        self.autofetch: bool = autofetch

        try:
            response_format = self.query["response"]["format"]
            if response_format != "json-stat":
                raise TypeError(
                    f"""Response format must be 'json-stat', got '{self.query["response"]["format"]}'."""
                )
        except KeyError as err:
            raise KeyError(f"Invalid format query format. {err} not found.")

        if self.autofetch:
            self.get_data()

    def __repr__(self):
        return f"PxWeb(url='{self.url}', query={self.query}, autofetch={self.autofetch}, data={self.dataset})"

    def get_data(self) -> None:
        """Get data from the API"""
        response = requests.post(self.url, json=self.query, timeout=10)
        if response.status_code == 200:
            self.dataset = self._response_handler(json.loads(response.text))
        else:
            warn(f"Failed to retrieve data: {response.status_code}: {response.reason}")

    def _response_handler(self, response: dict) -> list[dict]:
        """
        Takes the response json-stat and turns it into a list of dicts that can
        be used to convert into a dataframe, using either pandas or polars.
        """
        if response is None:
            raise ValueError("response cannot be None.")

        query_dims = [dim["code"] for dim in self.query["query"]]
        response_dims = response["dataset"]["dimension"]

        category_labels = {}
        for dim in response_dims:
            if dim in query_dims and dim != "ContentsCode":
                label = response_dims[dim]["category"]["label"]
                category_labels.update({response_dims[dim]["label"]: label.values()})
            if dim == "ContentsCode":
                value_label = list(response_dims[dim]["category"]["label"].values())[0]

        result = [
            dict(zip(category_labels.keys(), x))
            for x in itertools.product(*category_labels.values())
        ]

        all_values = response["dataset"]["value"]

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
