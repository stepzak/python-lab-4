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

class DictConstraints(UserDict[cst.Constraint, tuple[set[str], list]]):
    """
    Dict of constraints. Uses ``src.constants.Constraint`` as key and tuple of set of positions, list of args as its value
    """
    def __init__(self, data):
        super().__init__()
        self.update(data)

    def __setitem__(self, key: cst.Constraint, value: tuple[set[str], list]):
        if not isinstance(key, cst.Constraint):
            raise TypeError(f"Key '{key}' is not a constraint.")
        if not isinstance(value[0], set) or not isinstance(value[1], list):
            raise TypeError(f"Value '{value}' is not a valid constraint.")
        self.data[key] = value

def is_created(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self.created:
            raise exc.TableNotCreated()
        return func(*args, **kwargs)
    return wrapper

class Table(Generic[T]):
    """
    Table class
    :param collection: collection to init table with
    :param constraints: constraints to use
    """
    def __init__(self, collection: Collection[T], constraints: DictConstraints):
        self._indexes: dict[str, AbstractIndex] = {}
        self._rows = collection
        self.constraints = constraints
        self.created = False

    def create(self):
        """
        Creates table(creates indexes for unique values etc.)
        :return:
        """
        if self.created:
            return
        uq_csrt = self.constraints.get(cst.Constraint.UNIQUE, (set(), []))[0] | self.constraints.get(cst.Constraint.FOREIGN_KEY, (set(), []))[0]
        for uq in uq_csrt:
            self.create_index('base', uq)
        self.created = True

    @property
    def dtype(self) -> Type[T]:
        return self._rows.dtype

    def create_index(self, index_type: str, field_name: str) -> None:
        """
        Creates index
        :param index_type: type of index
        :param field_name: field to create index on
        :return:
        """
        if field_name not in self._indexes:
            idx = IndexFactory.create(index_type, field_name)
            idx.rebuild(self._rows)
            self._indexes[field_name] = idx
        else:
            raise exc.IndexExists(field_name)

    def drop_index(self, field_name: str):
        """
        Drops index
        :param field_name: field to drop index on
        :return:
        """
        if field_name not in self._indexes:
            return
        self._indexes.pop(field_name)

    def create_constraint(self, constraint: cst.Constraint, fields: set[str], args: list | None = None) -> None:
        """
        Creates constraint
        :param constraint: ``src.constants.Constraint`` type of constraint
        :param fields: fields to create constraint on
        :param args: arguments to create constraint with
        """
        self.constraints.setdefault(constraint, (set(), args))[0].update(fields)
        match constraint:
            case cst.Constraint.UNIQUE:
                for field in fields:
                    if field not in self._indexes:
                        self.create_index('base', field)
            case cst.Constraint.FOREIGN_KEY:
                for field in fields:
                    if field not in self._indexes:
                        self.create_index('base', field)


    def drop_constraint(self, constraint: cst.Constraint, fields: set[str]):
        """
        Drops constraint
        :param constraint: type of constraint
        :param fields: fields to drop constraint on
        """
        if constraint in self.constraints:
            existing = self.constraints[constraint][0]
            to_remove = existing & fields

            if constraint == cst.Constraint.UNIQUE:
                for field in to_remove:
                    self.drop_index(field)

            self.constraints[constraint] -= fields
            if not self.constraints[constraint]:
                self.constraints.pop(constraint)


    @is_created
    def append(self, item: T) -> None:
        """
        Append item to table
        :param item: row of ``table dtype`` type
        :return:
        """
        if not isinstance(item, self.dtype):
            raise TypeError(f"Item '{item}' is not a valid type(expected {self.dtype})")
        uq_csrt = self.constraints.get(cst.Constraint.UNIQUE, (set(), list))
        for field in uq_csrt[0]:
            key = getattr(item, field)
            if key in self._indexes[field]:
                raise exc.ConstraintFailed(cst.Constraint.UNIQUE, field, key)
        pos = len(self._rows)
        self._rows.append(item)
        for idx in self._indexes.values():
            idx.on_append(item, pos)

    @is_created
    def pop(self) -> T:
        """
        Pop item from table
        :return item: row of ``table dtype`` type
        """
        item = self._rows.pop()
        pos = len(self._rows)
        for idx in self._indexes.values():
            idx.on_pop(item, pos)
        return item

    def rebuild_indexes(self):
        """
        Rebuild indexes
        :return:
        """
        for idx in self._indexes.values():
            idx.rebuild(self._rows)

    @is_created
    def remove(self, item: T) -> None:
        """
        Remove item from table
        :param item: item to remove from table(only first encountered)
        :return:
        """
        self._rows.remove(item)
        self.rebuild_indexes()

    @is_created
    def update_at(self, pos: int, updates: dict) -> None:
        """
        Update row on position ``pos``
        :param pos: position to update row on
        :param updates: dict of updates(e.g. {'name': 'New name'})
        :return:
        """
        old_row = self._rows[pos]
        new_row = replace(old_row, **updates)
        uq_csrt = self.constraints.get(cst.Constraint.UNIQUE, (set(), []))
        for field in uq_csrt[0]:
            if field in updates:
                old_key = getattr(old_row, field)
                new_key = getattr(new_row, field)
                if old_key != new_key and new_key in self._indexes[field]:
                    raise exc.ConstraintFailed(cst.Constraint.UNIQUE, field, new_key)
        self._rows[pos] = new_row

        for idx in self._indexes.values():
            idx.on_update(old_row, new_row, pos)

    def _full_scan(self, field: str, op_func: Callable, value):
        """
        Full scan table with given filter
        :param field: field to scan table with
        :param op_func: operator function
        :param value: value to compare with
        :return:
        """
        res = set()
        for n, row in enumerate(self._rows, start=0):
            attr = getattr(row, field, None)
            if op_func(attr, value):
                res.add(n)
        return res

    @is_created
    def insert(self, item: T, index: int, auto_update: bool = True) -> None:
        """
        Insert item into table
        :param item: row to insert
        :param index: position to insert item in
        :param auto_update: auto rebuild indexes
        :return:
        """
        self._rows.insert(index, item)
        if auto_update:
            self.rebuild_indexes()

    @is_created
    def query(self, **filters) -> set[int]:
        """
        :param filters: kwarg, passed as: FIELD__OPERATOR = VALUE; e. g. query(name__eq = 'Steve', age__gt = 18).
        :return set of indexes
        """
        #hints = get_type_hints(self.dtype)
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
        """
        Remove item from table
        :param index: position to remove item from
        :param auto_update: auto rebuild indexes
        :return:
        """
        self._rows.pop(index)
        if auto_update:
            self.rebuild_indexes()


    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self) -> Iterator[T]:
        return iter(self._rows)

    def __getitem__(self, index: int) -> T:
        return self._rows[index]