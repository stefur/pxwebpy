import pytest

from pxwebpy import PxDatabase, PxTable


@pytest.fixture
def db():
    return PxDatabase("scb")


def test_go_to(db):
    """Go around some paths"""
    # This should path lead to a folder with tables only
    db.go_to("Befolkning", "Befolkningsstatistik", "Folkmängd")
    assert len(db.get_contents().get("tables")) > 0
    assert len(db.get_contents().get("folders")) == 0

    # Resetting and going again, this time a single step from the top node
    db.reset()
    db.go_to("Arbetsmarknad")

    # Should list a bunch of subfolders
    assert len(db.get_contents().get("folders")) > 0
    assert len(db.get_contents().get("tables")) == 0


def test_reset(db):
    db.go_to("Befolkning", "Befolkningsstatistik", "Folkmängd")
    db.reset()

    # TODO Should be an attribute, or a getter function maybe
    link: str = next(
        link["href"]
        for link in db._current_location.get("links")
        if link["rel"] == "self"
    )

    # Assuming we're back at navigation, we should get an index in the string
    assert link.find("navigation") > 0

    # And since we're back at the top, there should be no previous
    assert len(db._previous_location) == 0


def test_back(db):
    """Going back from the nagivation toplevel should raise an error"""
    with pytest.raises(IndexError):
        db.back()


def test_get_contents(db):
    contents = db.get_contents()

    assert isinstance(contents, dict)
    assert len(contents.get("folders")) > 0


def test_get_table(db):
    db.go_to("Befolkning", "Befolkningsstatistik", "Folkmängd")
    table = db.get_table("Folkmängden efter ålder och kön. År 1860-2024")

    assert isinstance(table, PxTable)

    db.reset()

    # Now go for the full path directly
    table = db.get_table(
        "Befolkning",
        "Befolkningsstatistik",
        "Folkmängd",
        "Folkmängden efter ålder och kön. År 1860-2024",
    )

    assert isinstance(table, PxTable)
