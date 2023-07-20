"""Helper to make fetching data from PxWeb a bit easier"""
from pathlib import Path
import json
from json import JSONDecodeError
from warnings import warn
from itertools import cycle
import requests


class PxWeb:
    """
    A helper to manage and get data from the PxWeb API.
    If provided with both URL and query when instantiating ,
    it will automatically try to get data from the API.
    This behaviour can be turned off using the
    toggle "autofetch" (Default: True).
    """

    def __init__(self, url=None, json_query=None, autofetch=True) -> None:
        self.url: str = url
        self.query: dict | None = json_query
        self.data: dict | None = None

        if self.url is not None and self.query is not None and autofetch:
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

    def to_dict(self) -> dict | None:
        """Takes json-stat and turns it into a dict"""
        if self.query is None or self.data is None:
            return warn("`query` and/or `data` cannot be None.")
        if self.query["response"]["format"] != "json-stat":
            return warn("Currently only response format 'json-stat' is supported.")
        query_dims = [dim["code"] for dim in self.query["query"]]
        data_dims = self.data["dataset"]["dimension"]

        all_labels = {}
        label_lengths = {}
        for dim in data_dims:
            if dim in query_dims and dim != "ContentsCode":
                labels = data_dims[dim]["category"]["label"]
                length = {dim: len(labels)}
                label_lengths.update(length)
                all_labels.update({data_dims[dim]["label"]: labels.values()})
            if dim == "ContentsCode":
                value_label = list(data_dims[dim]["category"]["label"].values())[0]

        max_length = max(label_lengths.values())

        # Extend all labels to match up length of data
        for key, value in all_labels.items():
            new_length = self.__extend_list(value, max_length)
            all_labels[key] = new_length

        all_labels.update({value_label: self.data["dataset"]["value"]})

        return all_labels

    def __extend_list(self, values: list, length: int) -> list:
        """Recycle items in a list up to given length"""
        cycle_values = cycle(values)
        result = []
        for _i in range(length):
            result.append(next(cycle_values))
        return result

    @property
    def query(self) -> dict | None:
        """Getter for the query"""
        return self.__query

    @query.setter
    def query(self, json_query: Path | str | None) -> None:
        """Set the query, accepting different variants"""
        match json_query:
            case Path():
                with open(json_query, mode="r", encoding="utf-8") as read_file:
                    self.__query = json.load(read_file)
            case str():
                try:
                    self.__query = json.loads(json_query)
                except JSONDecodeError as err:
                    warn(
                        f"Warning: Provided string could not be decoded as JSON. Error: {err}"
                    )
            case None:
                self.__query = None
            case _:
                warn(
                    f"Invalid input for `json_query`. \
                    Expected `str` or `Path`, got {type(json_query)!r}."
                )
