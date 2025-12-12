import pytest

from src.orm.exceptions import ConstraintFailed


def test_update_single_field(db_library_initial_data):
    original = db_library_initial_data.select_rows("library", isbn=1234567890123)[0]
    assert original.title == "Title 1"

    db_library_initial_data.update("library", {"title": "Updated Title"}, isbn=1234567890123)
    updated = db_library_initial_data.select_rows("library", isbn=1234567890123)[0]
    assert updated.title == "Updated Title"
    assert updated.author == "Author 1"


def test_update_multiple_fields(db_library_initial_data):
    db_library_initial_data = db_library_initial_data
    db_library_initial_data.update("library", {"title": "New Title", "author": "New Author"}, isbn=1234567890124)
    updated = db_library_initial_data.select_rows("library", isbn=1234567890124)[0]
    assert updated.title == "New Title"
    assert updated.author == "New Author"


def test_update_with_unique_constraint_violation(db_library_initial_data):
    with pytest.raises(ConstraintFailed):
        db_library_initial_data.update("library", {"isbn": 1234567890123}, isbn=1234567890124)