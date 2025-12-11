from src.orm.table import Table
from src.constants import Constraint


class ConstraintFailed(Exception):
    def __init__(self, constraint_type: Constraint, field: str, value) -> None:
        self.message = f"Constraint {constraint_type.value} failed for field {field}: {value!r}"
        super().__init__(self.message)
        self.constraint_type = constraint_type
        self.field = field
        self.value = value

class TableNotCreated(Exception):
    def __init__(self, table: Table) -> None:
        self.message = f"Table {table}"

class IndexExists(Exception):
    def __init__(self, field: str) -> None:
        self.message = f"Index for {field} already exists"
        super().__init__(self.message)