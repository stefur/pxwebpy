"""Helper to make fetching data from PxWeb a bit easier"""
from pathlib import Path
import json
import io
import csv
from typing import Any
import requests


def get_data(json_query: Path | str, url: str) -> dict[Any, Any]:
    """Sends a json query and returns the response with data in a dict"""
    match json_query:
        case Path():
            with open(json_query, mode="r", encoding="utf-8") as read_file:
                query = json.load(read_file)
        case str():
            query = json.loads(json_query)
        case _:
            raise TypeError(
                f"Invalid input for `json_query`. \
                Expected `str` or `Path`, got {type(json_query)!r}."
            )

    if query["response"]["format"] != "csv":
        raise ValueError(
            'Invalid value for `format` of `response` in query. Only "csv" is supported.'
        )

    response = requests.post(url, json=query, timeout=10)
    read = csv.reader(io.StringIO(response.text))

    data: dict[Any, Any] = {}
    i = 0
    for row in read:
        if i == 0:
            for item in row:
                data[item] = []
        else:
            for key, item in zip(data.keys(), row):
                data[key].append(item)

        i += 1

    return data
