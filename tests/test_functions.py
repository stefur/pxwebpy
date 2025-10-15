import json

import pytest

from pxweb._internal.functions import (
    build_query,
    count_data_cells,
    expand_wildcards,
    split_value_codes,
)


@pytest.fixture
def value_codes():
    return {
        "ContentsCode": ["000003O5"],
        "Region": ["0114", "0115", "0117"],
        "Kon": ["1", "2"],
        "Alder": ["-9", "10-19", "20-29", "30-39"],
        "Tid": ["2024M01", "2024M02", "2024M03"],
    }


@pytest.fixture
def value_codes_wildcard():
    return {
        "ContentsCode": ["000003O5"],
        "Region": ["0114", "0115", "0117"],
        "Kon": ["*"],
        "Alder": ["*"],
        "Tid": ["2024M01", "2024M02", "2024M03"],
    }


@pytest.fixture
def code_list():
    return {"Alder": "agg_Ålder10årJ"}


def test_count_data_cells(value_codes):
    """Counting the data cells from a given set of value codes should return an expected integer"""

    assert count_data_cells(value_codes) == 72


@pytest.mark.parametrize("cl", ["code_list", None])
def test_build_query(value_codes, cl, snapshot, request):
    cl = request.getfixturevalue(cl) if cl else None
    assert snapshot == build_query(value_codes, cl), (
        "The build_query() function does not create correct output"
    )


def test_split_query(value_codes):
    splits = split_value_codes(value_codes, 30)

    assert len(splits) == 4


@pytest.mark.parametrize(
    "source",
    [
        "tests/code_list_agg_Ålder10årJ.json",
        "tests/table_variables_TAB5444.json",
    ],
)
def test_expand_wildcards(source, value_codes_wildcard, snapshot):
    """Wildcards should expand correctly"""
    with open(source, "r") as file:
        source_loaded = json.load(file)

    # Using only the code list will return Kon with wildcards,
    # while the table_variables will be expanded on the default 1-year age groups for Alder
    # and get all Kon
    result = expand_wildcards(value_codes_wildcard, source_loaded)

    assert result == snapshot
