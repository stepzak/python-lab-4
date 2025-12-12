from dataclasses import dataclass
import src.orm.exceptions as exc
import random

from src.database.session import DatabaseSession
from src.orm.collection import ImmutableCollection
from src.orm.table import DictConstraints
from src.book import Book
import src.constants as cst
from src.app_logger import AppLogger

@dataclass
class Event:
    type: cst.EventType
    args: tuple
    result: bool

    def __repr__(self):
        return f"Event(type={self.type.value}, args={self.args}, result={self.result})"

@dataclass
class SimulationResults:
    result: ImmutableCollection[Book]
    history: list[Event]

    def __eq__(self, other):
        return (self.result == other.result) and (self.history == other.history)

class LibrarySimulation:
    def __init__(self,
                 author_list: list | None = None,
                 genre_list: list | None = None,
                 title_list: list | None = None,
                 pages_range: tuple[int, int] | None = None,
                 years_range: tuple[int, int] | None = None,
                 ):
        self.session = DatabaseSession()
        self.history = []
        self.logger = AppLogger.get_logger(__name__)

        self.isbn_range = (1000000000000, 9999999999999)
        self.author_list = author_list or cst.DEFAULT_AUTHORS
        self.genre_list = genre_list or cst.DEFAULT_GENRES
        self.title_list = title_list or cst.DEFAULT_TITLES
        self.pages_range = pages_range or cst.DEFAULT_PAGE_RANGE
        self.year_range = years_range or cst.DEFAULT_YEAR_RANGE

    def init_table(self):
        self.session.create_dtype("book", Book)
        constraints = DictConstraints({cst.Constraint.UNIQUE: {"isbn"}})
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
            with self.session.transaction():
                self.session.insert("library", book)

        self.logger.info("Table initialized")


    def process_add(self):
        isbn = random.randint(self.isbn_range[0], self.isbn_range[1])
        title = random.choice(self.title_list)
        author = random.choice(self.author_list)
        genre = random.choice(self.genre_list)
        year = random.randint(self.year_range[0], self.year_range[1])
        pages = random.randint(self.pages_range[0], self.pages_range[1])
        book = Book(
            title=title,
            isbn=isbn,
            author=author,
            genre=genre,
            year=year,
            pages=pages,

        )
        event_log = Event(
            cst.EventType.ADD,
            (title, author, genre, year, pages, isbn),
            True
        )
        try:
            with self.session.transaction():
                self.session.insert("library", book)
        except exc.ConstraintFailed:
            self.logger.error("Failed to add book: unique constraint failed")
            event_log.result = False
        self.logger.info(event_log.__repr__())
        self.history.append(event_log)

    def get_random_book(self) -> Book | None:
        books = list(self.session.select_rows("library"))
        if not books:
            self.logger.error("No books found")
            return None
        rand: Book = random.choice(books)
        return rand

    def process_remove(self):
        event_log = Event(
            cst.EventType.REMOVE_BOOK,
            tuple(),
            True
        )
        rand = self.get_random_book()
        if not rand:
            self.logger.error("Failed to remove book: none are present")
            event_log.result = False
        else:
            isbn = rand.isbn
            event_log.args = (isbn,)
            with self.session.transaction():
                try:
                    self.session.delete("library", isbn=isbn)
                except Exception as e:
                    self.logger.error(e)
                    event_log.result = False
        self.history.append(event_log)
        self.logger.info(event_log.__repr__())

    def process_read(self):
        event_log = Event(
            cst.EventType.READ_BOOK,
            args = tuple(),
            result = True
        )
        book = self.get_random_book()
        if not book:
            self.logger.error("Failed to read book: none are present")
            event_log.result = False
        else:
            book()
            event_log.args = (book.isbn,)

        self.history.append(event_log)
        self.logger.info(event_log.__repr__())

    def process_valid_query(self):
        action = random.randint(0, 2)
        event_log = Event(
            cst.EventType.VALID_QUERY,
            (action,),
            True
        )
        match action:
            case 0:
                query = {"year__ge": 2000, "year__lt": 2011}
                res = self.session.select("library", **query)
            case 1:
                query = {"pages__gt": self.pages_range[0]}
                res = self.session.select("library", **query)
            case 2:
                rand = self.get_random_book()
                if not rand:
                    self.logger.error("Failed to get book: none are present")
                    event_log.result = False
                else:
                    query = {"isbn": rand.isbn}
                    res = self.session.select("library", **query)
            case _:
                query = {}
                res = set()
        event_log.args = event_log.args + (query,)
        queries = ("year", "pages", "isbn")
        self.logger.info(f"Got {len(res)} results by {queries[action]} query")
        self.history.append(event_log)
        self.logger.info(event_log.__repr__())

    def process_not_found_query(self):
        query = {"pages__lt": self.pages_range[0]-1}
        res = self.session.select("library", **query)
        self.logger.info(f"Got {len(res)} results by page query(0 expected)")
        event = Event(
            cst.EventType.ZERO_RESULT_QUERY,
            tuple(),
            True
        )
        self.history.append(event)
        self.logger.info(event.__repr__())

    def process_update_book(self):
        event_log = Event(
            cst.EventType.UPDATE_BOOK,
            tuple(),
            True
        )
        book = self.get_random_book()
        if not book:
            self.logger.error("Failed to update book: none are present")
            event_log.result = False
        else:
            isbn = book.isbn
            title = random.choice(self.title_list)
            author = random.choice(self.author_list)
            event_log.args = (isbn, title, author)
            with self.session.transaction():
                update = {"title": title, "author": author}
                try:
                    self.session.update("library", update, isbn=isbn)
                except exc.ConstraintFailed:
                    self.logger.error("Failed to update book: unique constraint failed")
                    event_log.result = False

        self.history.append(event_log)
        self.logger.info(event_log.__repr__())

    def run_simulation(self, step: int = 20, seed: int | None = None) -> SimulationResults:
        random.seed(seed)
        self.init_table()
        for step in range(step):
            event = random.choice([*cst.EventType])
            match event:
                case cst.EventType.ADD:
                    self.process_add()

                case cst.EventType.REMOVE_BOOK:
                    self.process_remove()

                case cst.EventType.READ_BOOK:
                    self.process_read()

                case cst.EventType.VALID_QUERY:
                    self.process_valid_query()

                case cst.EventType.UPDATE_BOOK:
                    self.process_update_book()

        results = self.session.select_rows("library")
        return SimulationResults(
            result=results,
            history=self.history,
        )

    def drop_database(self):
        self.logger.info("Dropping database")
        self.session = DatabaseSession()

if __name__ == "__main__":
    sim = LibrarySimulation()
    AppLogger.configure_logger()
    results = []
    for i in range(3):
        res = sim.run_simulation(seed = 52, step = 40)
        print(res)
        results.append(res)
        sim.drop_database()
    print(results[0] == results[1])
