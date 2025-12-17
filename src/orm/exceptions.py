from src.constants import Constraint

class ORMException(Exception):
    def __init__(self, message):
        super().__init__(message)

class ConstraintFailed(ORMException):
    def __init__(self, constraint_type: Constraint, field: str, value) -> None:
        self.message = f"Constraint {constraint_type.value} failed for field {field}: {value!r}"
        super().__init__(self.message)
        self.constraint_type = constraint_type
        self.field = field
        self.value = value

class TableNotCreated(ORMException):
    def __init__(self) -> None:
        self.message = f"Table not created"
        super().__init__(self.message)

class IndexExists(ORMException):
    def __init__(self, field: str) -> None:
        self.message = f"Index for {field} already exists"
        super().__init__(self.message)