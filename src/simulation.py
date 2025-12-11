from dataclasses import dataclass
from typing import Any
import random

from src.database.session import DatabaseSession
from src.orm.collection import Collection
from src.orm.table import Table, DictConstraints
from src.book import Book
from src.constants import EventType, Constraint
from src.app_logger import AppLogger

@dataclass
class Event:
    type: EventType
    args: tuple
    result: Any


class LibrarySimulation:
    def __init__(self):
        self.session = DatabaseSession()
        self.history = []
        self.logger = AppLogger.get_logger(__name__)

    def init_table(self):
        self.session.create_dtype("book", Book)
        constraints = DictConstraints({Constraint.UNIQUE: {"isbn"}})
        self.session.create_table("library", "book", constraints)
        self.session.create_idx("library", "base", "genre")
        self.session.create_idx("library", "base", "author")
        self.session.create_idx("library", "range", "year")
        initial_data = [
            Book("Title 1", "Author 1", 2000, "Genre 1", 1234567890123, 100),
            Book("Title 2", "Author 2", 2015, "Genre 1", 1234567890124, 150),
            Book("Title 3", "Author 2", 2010, "Genre 2", 1234567890125, 125),
        ]
        for book in initial_data:
            self.session.insert("library", book)

        self.logger.info("Table initialized")

    def run_simulation(self, step: int = 20, seed: int | None = None) -> None:
        random.seed(seed)
        self.init_table()
        isbn_range = [1000000000000, 9999999999999]
        for step in range(step):
