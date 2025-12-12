from collections import UserDict
from dataclasses import replace
from functools import wraps
from typing import TypeVar, Generic, Type, Callable
from typing import Iterator
from src.orm.index.factory import IndexFactory
from src.orm.collection import Collection
from src.orm.index.abstract import AbstractIndex
import src.constants as cst
import src.orm.exceptions as exc
from typing import get_type_hints

T = TypeVar('T')

class DictConstraints(UserDict[cst.Constraint, set[str]]):
    def __init__(self, data):
        super().__init__()
        self.update(data)

    def __setitem__(self, key: cst.Constraint, value: set[str]):
        if not isinstance(key, cst.Constraint):
            raise TypeError(f"Key '{key}' is not a constraint.")
        self.data[key] = set(value)

def is_created(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self.created:
            raise exc.TableNotCreated()
        return func(*args, **kwargs)
    return wrapper

class Table(Generic[T]):
    def __init__(self, collection: Collection[T], constraints: DictConstraints):
        self._indexes: dict[str, AbstractIndex] = {}
        self._rows = collection
        self.constraints = constraints
        self.created = False

    def create(self):
        if self.created:
            return
        uq_csrt = self.constraints.get(cst.Constraint.UNIQUE, set())
        for uq in uq_csrt:
            self.create_index('base', uq)
        self.created = True

    @property
    def dtype(self) -> Type[T]:
        return self._rows.dtype

    def create_index(self, index_type: str, field_name: str) -> None:
        if field_name not in self._indexes:
            idx = IndexFactory.create(index_type, field_name)
            idx.rebuild(self._rows)
            self._indexes[field_name] = idx
        else:
            raise exc.IndexExists(field_name)

    def drop_index(self, field_name: str):
        if field_name not in self._indexes:
            return
        self._indexes.pop(field_name)

    def create_constraint(self, constraint: cst.Constraint, fields: set[str]):
        self.constraints.setdefault(constraint, set()).update(fields)

        if constraint == cst.Constraint.UNIQUE:
            for field in fields:
                if field not in self._indexes:
                    self.create_index('base', field)

    def drop_constraint(self, constraint: cst.Constraint, fields: set[str]):
        if constraint in self.constraints:
            existing = self.constraints[constraint]
            to_remove = existing & fields

            if constraint == cst.Constraint.UNIQUE:
                for field in to_remove:
                    self.drop_index(field)

            self.constraints[constraint] -= fields
            if not self.constraints[constraint]:
                self.constraints.pop(constraint)


    @is_created
    def append(self, item: T) -> None:
        uq_csrt = self.constraints.get(cst.Constraint.UNIQUE, [])
        for field in uq_csrt:
            key = getattr(item, field)
            if key in self._indexes[field]:
                raise exc.ConstraintFailed(cst.Constraint.UNIQUE, field, key)
        pos = len(self._rows)
        self._rows.append(item)
        for idx in self._indexes.values():
            idx.on_append(item, pos)

    @is_created
    def pop(self) -> T:
        item = self._rows.pop()
        pos = len(self._rows)
        for idx in self._indexes.values():
            idx.on_pop(item, pos)
        return item

    def rebuild_indexes(self):
        for idx in self._indexes.values():
            idx.rebuild(self._rows)

    @is_created
    def remove(self, item: T) -> None:
        self._rows.remove(item)
        self.rebuild_indexes()

    @is_created
    def update_at(self, pos: int, updates: dict) -> None:
        old_row = self._rows[pos]
        new_row = replace(old_row, **updates)
        uq_csrt = self.constraints.get(cst.Constraint.UNIQUE, set())
        for field in uq_csrt:
            if field in updates:
                old_key = getattr(old_row, field)
                new_key = getattr(new_row, field)
                if old_key != new_key and new_key in self._indexes[field]:
                    raise exc.ConstraintFailed(cst.Constraint.UNIQUE, field, new_key)
        self._rows[pos] = new_row

        for idx in self._indexes.values():
            idx.on_update(old_row, new_row, pos)

    def _full_scan(self, field: str, op_func: Callable, value):
        res = set()
        for n, row in enumerate(self._rows, start=0):
            attr = getattr(row, field, None)
            if op_func(attr, value):
                res.add(n)
        return res

    @is_created
    def insert(self, item: T, index: int, auto_update: bool = True) -> None:
        self._rows.insert(index, item)
        if auto_update:
            self.rebuild_indexes()

    @is_created
    def query(self, **filters) -> set[int]:
        """
        :param filters: kwarg, passed as: FIELD__OPERATOR = VALUE; e. g. query(name__eq = 'Steve', age__gt = 18).
        query(name = 'Steve') == query(name__eq = 'Steve')
        :return: set of indexes
        """
        hints = get_type_hints(self.dtype)
        result: set[int] = set()
        if not filters:
            return set(range(len(self._rows)))
        for filter_, value in filters.items():


            if "__" in filter_:
                field, op = filter_.rsplit('__', 1)
                if op not in cst.OPERATORS:
                    field, op = filter_, "eq"
            else:
                field, op = filter_, "eq"

            op_func = cst.OPERATORS[op]
            typeof = hints[field]
            value = typeof(value)
            idx = self._indexes.get(field, None)
            if idx:
                try:
                    res = idx.get_positions_for_query(op_func, value)
                except NotImplementedError:
                   res = self._full_scan(field, op_func, value)
            else:
                res = self._full_scan(field, op_func, value)


            if not res:
                return res
            if not result:
                result = res
            else:
                result &= res
        return result

    @is_created
    def remove_by_index(self, index: int, auto_update: bool = True) -> None:
        self._rows.pop(index)
        if auto_update:
            self.rebuild_indexes()


    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self) -> Iterator[T]:
        return iter(self._rows)

    def __getitem__(self, index: int) -> T:
        return self._rows[index]