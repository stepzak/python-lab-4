from dataclasses import dataclass
from typing import Any


@dataclass
class Insert:
    table_name: str
    position: int

@dataclass
class Update:
    table_name: str
    position: int
    old_row: Any

@dataclass
class Delete:
    table_name: str
    position: int
    row: Any