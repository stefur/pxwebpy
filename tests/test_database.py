from datetime import datetime, timedelta, timezone

import pytest

from pxwebpy import PxDatabase


@pytest.fixture
def db():
    return PxDatabase("scb")


def test_go_to(db):
    """Go around some folders"""
    db.go_to("BE0101A")
    assert len(db.get_contents().get("tables")) > 0
    assert len(db.get_contents().get("folders")) == 0

    # Resetting and going again, this time a single step from the top node
    db.reset()
    db.go_to("AM")

    # Should list a bunch of subfolders
    assert len(db.get_contents().get("folders")) > 0
    assert len(db.get_contents().get("tables")) == 0


def test_reset(db):
    db.go_to("BE0101")
    db.reset()

    link: str = next(
        link["href"] for link in db.here().get("links") if link["rel"] == "self"
    )

    # Assuming we're back at navigation, we should get an index in the string
    assert link.find("navigation") > 0

    # And since we did a reset, there should be no history either
    assert len(db.history()) == 0


def test_back(db):
    """Going back from the nagivation toplevel should raise an error"""
    with pytest.raises(IndexError):
        db.back()


def test_search(db):
    """Check that all results are within 30 days"""
    search_result = db.search(query="befolkning", past_days=30)

    timestamps = [
        datetime.fromisoformat(table["updated"]) for table in search_result["tables"]
    ]

    now = datetime.now(tz=timezone.utc)

    assert all((now - timestamp) <= timedelta(days=30) for timestamp in timestamps)


def test_get_contents(db):
    contents = db.get_contents()

    assert isinstance(contents, dict)
    assert len(contents.get("folders")) > 0


def test_get_table_data(db):
    dataset = db.get_table_data(table_id="TAB6471")

    assert isinstance(dataset, list)
    assert len(dataset) > 1


def test_get_table_data_only_list_or_strings(db):
    with pytest.raises(ValueError):
        db.get_table_data(table_id="TAB6471", value_codes={"some_var": 42})


def test_get_table_data_only_strings_in_list(db):
    with pytest.raises(ValueError):
        db.get_table_data(table_id="TAB6471", value_codes={"some_var": ["1", "2", 42]})


def test_get_table_data_coerce_to_list(db):
    # Tid and ContentsCode should get coerced
    db.get_table_data(
        table_id="TAB6471",
        value_codes={"Alder": ["25"], "Tid": "2025M01", "ContentsCode": "000007SF"},
    )


def test_get_table_variables(db):
    variables = db.get_table_variables(table_id="TAB6471")
    # TODO Improve testing by targeting the expected structure instead
    assert isinstance(dict, variables)
    assert len(variables) > 0
