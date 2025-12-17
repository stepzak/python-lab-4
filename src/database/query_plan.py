import enum
from dataclasses import dataclass
from typing import Any


class QueryType(enum.Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    SELECT = "SELECT"
    CREATE = "CREATE"


@dataclass
class QueryPlan:
    operation: QueryType
    table: str
    kwargs: dict[str, Any]

