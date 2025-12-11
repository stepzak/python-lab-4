import operator
from pathlib import Path
from typing import Callable
from enum import StrEnum
import src.orm.operators as ops

LOG_FILE_PATH: Path = Path("/var/log/library_simulator/log.log")
LOG_FORMAT: str = "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s"

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
    POP = 'pop'
    VALID_QUERY = 'valid_query'
    ZERO_RESULT_QUERY = 'zero_result_query'
    UPDATE_BOOK = 'update_book'
    READ_BOOK = 'read_book'
    REMOVE_BOOK = 'remove_book'

class Constraint(StrEnum):
    UNIQUE = 'unique'