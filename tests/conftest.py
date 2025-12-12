import pytest
import src.constants as cst
from src.book import Book
from src.database.session import DatabaseSession
from src.orm.table import DictConstraints

@pytest.fixture
def session():
    session = DatabaseSession()
    yield session
    session.drop_dtype("BOOK")

@pytest.fixture
def db_library(session):
    session.create_dtype("BOOK", Book)
    session.create_table(
        "library",
        "BOOK",
        constraints = DictConstraints({cst.Constraint.UNIQUE: {"isbn"}})
    )
    session.create_idx("library", "base", "genre")
    session.create_idx("library", "base", "author")
    session.create_idx("library", "range", "year")
    yield session


@pytest.fixture
def db_library_initial_data(db_library):
    initial_data = [
        Book("Title 1", "Author 1", 2000, "Genre 2", 1234567890123, 100),
        Book("Title 2", "Author 2", 2015, "Genre 1", 1234567890124, 150),
        Book("Title 3", "Author 2", 2010, "Genre 1", 1234567890125, 125),
    ]
    for book in initial_data:
        db_library.insert("library", book)

    yield db_library