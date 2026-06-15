import pytest
from syrupy.assertion import SnapshotAssertion

from pxweb._internal.functions import (
    build_query,
    count_data_cells,
    expand_wildcards,
    split_value_codes,
)


@pytest.fixture
def value_codes() -> dict[str, list[str]]:
    return {
        "ContentsCode": ["000003O5"],
        "Region": ["0114", "0115", "0117"],
        "Kon": ["1", "2"],
        "Alder": ["-9", "10-19", "20-29", "30-39"],
        "Tid": ["2024M01", "2024M02", "2024M03"],
    }


@pytest.fixture
def code_list() -> dict[str, str]:
    return {"Alder": "agg_Ålder10årJ"}


def test_count_data_cells(value_codes: dict[str, list[str]]) -> None:
    """Counting the data cells from a given set of value codes should return an expected integer"""

    assert count_data_cells(value_codes) == 72


@pytest.mark.parametrize("cl", ["code_list", None])
def test_build_query(
    value_codes: dict[str, list[str]],
    cl: str | None,
    snapshot: SnapshotAssertion,
    request: pytest.FixtureRequest,
) -> None:
    cl = request.getfixturevalue(cl) if cl else None
    assert snapshot == build_query(value_codes, cl), (
        "The build_query() function does not create correct output"
    )


def test_split_query(value_codes: dict[str, list[str]]) -> None:
    splits = split_value_codes(value_codes, 30)

    assert len(splits) == 4


def test_expand_wildcards(snapshot: SnapshotAssertion) -> None:
    value_codes = {
        "Kon": ["*"],
        "Alder": ["*"],
        "Tid": ["2024M01", "2024M02"],
    }
    source = {
        "Kon": {
            "category": {
                "label": {"1": "män", "2": "kvinnor", "TotSa": "totalt"}
            }
        },
        "Alder": {
            "category": {
                "label": {"0": "0 år", "1": "1 år", "2": "2 år", "3": "3 år"}
            }
        },
    }
    assert expand_wildcards(value_codes, source) == snapshot
