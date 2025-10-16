import sys
from datetime import datetime, timedelta, timezone

import pytest

from pxweb import PxApi


@pytest.fixture
def api():
    return PxApi("scb")


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="Test requires fromisoformat support for 'Z' timezone, which requires Python >=3.11. Disabling for now, this test should be better anyway.",
)
def test_search(api):
    """Check that all results are within 30 days"""
    search_result = api.search(query="befolkning", past_days=30)

    timestamps = [
        datetime.fromisoformat(table["updated"]) for table in search_result["tables"]
    ]

    now = datetime.now(tz=timezone.utc)

    assert all((now - timestamp) <= timedelta(days=30) for timestamp in timestamps)


def test_get_table_data(api):
    dataset = api.get_table_data(table_id="TAB6471")

    assert isinstance(dataset, list)
    assert len(dataset) > 1


def test_get_table_data_only_list_or_strings(api):
    with pytest.raises(ValueError):
        api.get_table_data(table_id="TAB6471", value_codes={"some_var": 42})


def test_get_table_data_only_strings_in_list(api):
    with pytest.raises(ValueError):
        api.get_table_data(table_id="TAB6471", value_codes={"some_var": ["1", "2", 42]})


def test_get_table_data_coerce_to_list(api):
    # Tid and ContentsCode should get coerced
    api.get_table_data(
        table_id="TAB6471",
        value_codes={
            "Alder": ["25"],
            "Tid": "2025M01",
            "ContentsCode": "000007SF",
        },
    )


def test_get_table_variables(api):
    variables = api.get_table_variables(table_id="TAB6471")
    # TODO Improve testing by targeting the expected structure instead
    assert isinstance(variables, dict)
    assert len(variables) > 0

def test_get_paths(api):
    subpaths = api.get_paths("AM0101")
    assert isinstance(subpaths, list)
    assert len(subpaths) > 0

def test_tables_on_path(api):
    tables = api.tables_on_path("AM0101C")
    assert isinstance(tables, list)
    assert len(tables) > 0
