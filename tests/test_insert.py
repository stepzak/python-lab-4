import pytest
from src.orm.exceptions import ConstraintFailed
from src.book import Book


def test_insert(db_library):
    book = Book("Title 3", "Author 2", 2010, "Genre 2", 1234567890125, 100)
    db_library.insert("library", book)
    assert book in db_library.select_rows("library")

def test_insert_duplicate(db_library_initial_data):
    book = db_library_initial_data.select_rows("library")[0]
    with pytest.raises(ConstraintFailed):
        db_library_initial_data.insert("library", book)

def test_insert_wrong_type(db_library):
    with pytest.raises(TypeError):
        db_library.insert("library", object())

def test_rollback_insert(db_library_initial_data):
    book1 = Book("Title 3", "Author 2", 2010, "Genre 2", 1234567890130, 100)
    book2 = db_library_initial_data.select_rows("library")[0]
    try:
        with db_library_initial_data.transaction():
            db_library_initial_data.insert("library", book1)
            db_library_initial_data.insert("library", book2)
            assert False
    except ConstraintFailed:
        data = db_library_initial_data.select_rows("library")
        assert book1 not in data
