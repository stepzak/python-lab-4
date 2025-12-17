import operator
from pathlib import Path
from typing import Callable
from enum import StrEnum
import src.orm.operators as ops

LOG_FILE_PATH: Path = Path("/var/log/library_simulator/log.log")
LOG_FORMAT: str = "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s"

DEFAULT_AUTHORS: list[str] = ["Arthur Morgan", "Geralt of Rivia", "Ihave Nofantasy", "Funnyname Done", "Luke Skywalker"]
DEFAULT_GENRES: list[str] =  ["Thriller", "Adventure", "Sci-Fi", "Detective", "Horror"]
DEFAULT_TITLES: list[str] = ["Title1", "Title2", "Title3", "Title4", "Title5", "Title6"]
DEFAULT_YEAR_RANGE: tuple[int, int] = (1814, 2020)
DEFAULT_PAGE_RANGE: tuple[int, int] = (25, 550)

STEPS: int = 20
SEED: int = 52

OPERATORS: dict[str, Callable] = {
    'gt': operator.gt,
    'ge': operator.ge,
    'lt': operator.lt,
    'le': operator.le,
    'eq': operator.eq,
    'in': ops.in_,
}

class EventType(StrEnum):
    ADD = 'add'
    VALID_QUERY = 'valid_query'
    ZERO_RESULT_QUERY = 'zero_result_query'
    UPDATE_BOOK = 'update_book'
    READ_BOOK = 'read_book'
    REMOVE_BOOK = 'remove_book'

class Constraint(StrEnum):
    UNIQUE = 'unique'
    FOREIGN_KEY = 'foreign_key'