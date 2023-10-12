"""Helper to make fetching data from PxWeb a bit easier"""
from pathlib import Path
import json
from json import JSONDecodeError
from warnings import warn
import itertools
import requests
from datetime import datetime


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
        self.metadata: dict = {key: None for key in ["label", "source", "updated"]}
        self.last_refresh: datetime | None = None
        self.autofetch: bool = autofetch

        try:
            response_format = self.query["response"]["format"]
            if response_format != "json-stat2":
                raise TypeError(
                    f"""Response format must be 'json-stat2', got '{self.query["response"]["format"]}'."""
                )
        except KeyError as err:
            raise KeyError(f"Invalid query format. {err} not found.")

        if self.autofetch:
            self.get_data()

    def __repr__(self):
        return f"""PxWeb(url='{self.url}',
        query={self.query},
        metadata={self.metadata},
        autofetch={self.autofetch},
        last_refresh={self.last_refresh},
        data={self.dataset})"""

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
                    f"Response is missing the following metadata keys: {', '.join(missing_metadata)}."
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

        # Check if the response contains a metric, otherwise we default to using "value" as label
        try:
            metric = response["role"]["metric"]
        except KeyError:
            metric = None
            value_label = "value"

        category_labels = {}
        for dim in response_dims:
            if metric is not None and dim in metric:
                # Extract the first (and only) metric label from the values list or use an empty string as a default
                value_label = next(
                    iter(response_dims[dim]["category"]["label"].values()), ""
                )
            else:
                label = response_dims[dim]["category"]["label"]
                category_labels.update({response_dims[dim]["label"]: label.values()})

        result = [
            dict(zip(category_labels.keys(), x))
            for x in itertools.product(*category_labels.values())
        ]

        all_values = response["value"]

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
