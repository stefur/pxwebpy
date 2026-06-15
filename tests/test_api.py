import json
from collections.abc import Iterator
from typing import Any

import pytest
import responses
from syrupy.assertion import SnapshotAssertion
from utils import BASE_URL, load_response

from pxweb import PxApi


@pytest.fixture
def api() -> Iterator[PxApi]:
    """Mock an PxApi object instantiation"""
    with responses.RequestsMock() as rsps:
        (
            rsps.add(
                method=responses.GET,
                url=BASE_URL + "/config",
                json=load_response("config.json"),
            ),
        )
        rsps.add(
            method=responses.GET,
            url=BASE_URL + "/tables",
            json=load_response("tables.json"),
        )
        yield PxApi(BASE_URL)


def add_mock(method: str, path: str, response: str, **kwargs: Any) -> None:
    responses.add(
        method=method,
        url=BASE_URL + path,
        json=load_response(response),
        **kwargs,
    )


@responses.activate
def test_search(api: PxApi) -> None:
    add_mock("GET", "/tables", "search.json")
    api.search(query="befolkning", past_days=30)

    request = responses.calls[-1].request
    # Make sure the query is correctly formatted
    assert (
        request.url
        == "https://mock.api/tables?lang=sv&query=befolkning&pastDays=30"
    )


@responses.activate
def test_get_table_data(api: PxApi, snapshot: SnapshotAssertion) -> None:
    add_mock("GET", "/tables/TAB6471/data", "TAB6471_data.json")
    dataset = api.get_table_data(table_id="TAB6471")
    assert dataset == snapshot


@responses.activate
def test_get_table_data_iter(api: PxApi) -> None:
    add_mock("GET", "/tables/TAB6471/data", "TAB6471_data.json")
    iterator = api.get_table_data_iter(table_id="TAB6471")
    assert not isinstance(iterator, list)
    rows = list(iterator)
    assert all(isinstance(row, dict) for row in rows)


@responses.activate
def test_get_table_data_all_iter(api: PxApi) -> None:
    add_mock("GET", "/tables/TAB6471/metadata", "TAB6471_metadata.json")
    add_mock("POST", "/tables/TAB6471/data", "TAB6471_data.json")

    iterator = api.get_table_data_all_iter(table_id="TAB6471")

    assert not isinstance(iterator, list)

    rows = list(iterator)
    assert len(rows) > 1
    assert all(isinstance(row, dict) and "value" in row for row in rows)
    assert rows == api.get_table_data_all(table_id="TAB6471")


def test_get_table_data_only_list_or_strings(api: PxApi) -> None:
    with pytest.raises(ValueError):
        api.get_table_data(table_id="TAB6471", value_codes={"some_var": 42})  # type: ignore[dict-item]


def test_get_table_data_only_strings_in_list(api: PxApi) -> None:
    with pytest.raises(ValueError):
        api.get_table_data(
            table_id="TAB6471",
            value_codes={"some_var": ["1", "2", 42]},  # type: ignore[list-item]
        )


@responses.activate
def test_get_table_data_coerce_to_list(api: PxApi) -> None:
    add_mock("POST", "/tables/TAB6471/data", "TAB6471_data.json")
    api.get_table_data(
        table_id="TAB6471",
        value_codes={
            "Alder": ["25"],
            "Tid": "2025M01",
            "ContentsCode": "000007SF",
        },
    )
    request_body = json.loads(responses.calls[-1].request.body)  # type: ignore[arg-type]
    selection = {
        s["variableCode"]: s["valueCodes"] for s in request_body["selection"]
    }

    # Ensure the outgoing request is indeed containgin the selections as lists
    assert selection["Tid"] == ["2025M01"]
    assert selection["ContentsCode"] == ["000007SF"]
    assert selection["Alder"] == ["25"]


@responses.activate
def test_get_table_variables(api: PxApi, snapshot: SnapshotAssertion) -> None:
    add_mock("GET", "/tables/TAB6471/metadata", "TAB6471_metadata.json")

    variables = api.get_table_variables(table_id="TAB6471")
    assert snapshot == variables


@responses.activate
def test_tables_on_path(api: PxApi, snapshot: SnapshotAssertion) -> None:
    add_mock("GET", "/tables", "tables.json")
    variables = api.tables_on_path(path_id="AM0211C")
    assert variables == snapshot


@responses.activate
def test_get_paths(api: PxApi, snapshot: SnapshotAssertion) -> None:
    add_mock("GET", "/tables", "tables.json")
    paths = api.get_paths()
    assert paths == snapshot


@responses.activate
def test_get_paths_with_filter(api: PxApi, snapshot: SnapshotAssertion) -> None:
    add_mock("GET", "/tables", "tables.json")
    paths = api.get_paths(path_id="AM")
    assert paths == snapshot


@responses.activate
def test_get_table_data_expands_wildcards_with_code_list(api: PxApi) -> None:
    add_mock("GET", "/codelists/agg_Ålder10årJ", "agg_Ålder10årJ.json")
    add_mock("GET", "/tables/TAB6471/metadata", "TAB6471_metadata.json")
    # The data in the response does not match the actual query, but that is not the point.
    # We want to check that the wildcard is expanded correctly.
    add_mock("POST", "/tables/TAB6471/data", "TAB6471_data.json")
    api.get_table_data(
        table_id="TAB6471",
        value_codes={"Kon": ["*"], "Alder": ["*"]},
        code_list={"Alder": "agg_Ålder10årJ"},
    )
    request_body = json.loads(responses.calls[-1].request.body)  # type: ignore[arg-type]
    selection = {
        s["variableCode"]: s["valueCodes"] for s in request_body["selection"]
    }
    assert selection["Alder"] == [
        "-9",
        "10-19",
        "20-29",
        "30-39",
        "40-49",
        "50-59",
        "60-69",
        "70-79",
        "80-89",
        "90-99",
        "100+",
    ]
    assert selection["Kon"] == ["TotSa", "1", "2"]
