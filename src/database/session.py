from collections import UserDict
from contextlib import contextmanager
from dataclasses import is_dataclass, replace
from typing import Any, TypeVar, Generic, Sequence
import src.constants as cst
from src.database.log_operations import Insert, Update, Delete
from src.orm.collection import Collection, ImmutableCollection
from src.orm.table import Table, DictConstraints

K = TypeVar("K")
V = TypeVar("V")
DataclassInstance = Any

class RaiseOnExistDict(UserDict[K, V], Generic[K, V]):
    def __init__(self):
        super().__init__()

    def __setitem__(self, key, value):
        if key in self:
            raise KeyError(f"Key {key} already exists")
        self.data[key] = value


class DatabaseSession:
    def __init__(self):
        self._tables: RaiseOnExistDict[str, Table] = RaiseOnExistDict()
        self._dtypes: RaiseOnExistDict[str, DataclassInstance] = RaiseOnExistDict()
        self._transaction: list | None = None

    def begin(self) -> None:
        if self._transaction is not None:
            raise RuntimeError("Transaction already in progress")
        self._transaction = []

    def commit(self) -> None:
        if self._transaction is None:
            raise RuntimeError("No transaction in progress")
        self._transaction = None

    def rollback(self) -> None:
        if self._transaction is None:
            raise RuntimeError("No transaction in progress")

        for operation in self._transaction[::-1]:
            self.rollback_action(operation)

        self._transaction = None

    def rollback_action(self, action) -> None:
        table = self._tables[action.table_name]
        if isinstance(action, Delete):
            table.insert(action.row, action.position, auto_update=True)
        elif isinstance(action, Insert):
            table.remove_by_index(action.position, auto_update=True)
        elif isinstance(action, Update):
            table._rows[action.position] = action.old_row
            table.rebuild_indexes()

    @contextmanager
    def transaction(self):
        self.begin()
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()
            raise

    def create_dtype(self, name: str, dtype: DataclassInstance, if_not_exist: bool = False):
        if name in self._dtypes and if_not_exist:
            return

        if not is_dataclass(dtype):
            raise TypeError(f"Dtype {dtype} is not a dataclass")
        self._dtypes[name] = dtype

    def drop_dtype(self, name: str):
        dtype = self._dtypes[name]
        tables_to_drop = [
            table_name for table_name, table in self._tables.items()
            if table.dtype == dtype
        ]
        for table_name in tables_to_drop:
            del self._tables[table_name]
        del self._dtypes[name]

    def create_table(self, name: str, dtype_name: str, constraints: DictConstraints, if_not_exist: bool = False):
        if name in self._tables and if_not_exist:
            return
        dtype = self._dtypes[dtype_name]
        collection = Collection(dtype)
        table = Table(collection, constraints)
        table.create()
        self._tables[name] = table

    def drop_table(self, name: str):
        self._tables.pop(name)

    def insert(self, table_name: str, row):
        table = self._tables[table_name]
        table.append(row)
        pos = len(table) - 1
        if self._transaction is not None:
            op = Insert(table_name, pos)
            self._transaction.append(op)

    def select(self, table_name: str, **filters) -> set[int]:
        """
        :param table_name: table name
        :param filters: kwarg, passed as: FIELD__OPERATOR = VALUE; e. g. query(name__eq = 'Steve', age__gt = 18).
        query(name = 'Steve') == query(name__eq = 'Steve')
        :return: set of indexes
        """
        table = self._tables[table_name]
        return table.query(**filters)

    def select_rows(self, table_name: str, **filters) -> ImmutableCollection:
        """
        :param table_name: table name
        :param filters: kwarg, passed as: FIELD__OPERATOR = VALUE; e. g. query(name__eq = 'Steve', age__gt = 18).
        query(name = 'Steve') == query(name__eq = 'Steve')
        :return: tuple of records
        """
        table = self._tables[table_name]
        positions = self.select(table_name, **filters)
        collection = Collection(table.dtype)
        for i in positions:
            collection.append(table[i])
        return ImmutableCollection(collection)


    def update(self, table_name: str, values: dict, **filters,) -> None:
        """
        :param table_name: table name
        :param values: dict with keys 'name' and 'value'(e. g. {'name': 'New Updated Fresh Name'})
        :param filters: kwarg, passed as: FIELD__OPERATOR = VALUE; e. g. query(name__eq = 'Steve', age__gt = 18).
        query(name = 'Steve') == query(name__eq = 'Steve')
        :return: None
        """

        table = self._tables[table_name]
        positions = list(self.select(table_name, **filters))

        for pos in positions:
            old_row = table[pos]
            table.update_at(pos, values)
            if self._transaction is not None:
                self._transaction.append(Update(table_name, pos, old_row))


    def delete(self, table_name: str, **filters) -> None:
        """
        :param table_name: table name
        :param filters: kwarg, passed as: FIELD__OPERATOR = VALUE; e. g. query(name__eq = 'Steve', age__gt = 18).
        query(name = 'Steve') == query(name__eq = 'Steve')
        :return: None
        """
        table = self._tables[table_name]
        query_res = self.select(table_name, **filters)
        sort = sorted(query_res, reverse=True)
        for res in sort:
            del_val = table[res]
            op = Delete(table_name, res, del_val)
            table.remove_by_index(res, auto_update=False)
            if self._transaction is not None:
                self._transaction.append(op)

        table.rebuild_indexes()

    def create_idx(self, table_name: str, idx_type: str, field: str):
        table = self._tables[table_name]
        table.create_index(idx_type, field)

    def drop_idx(self, table_name: str, field: str):
        table = self._tables[table_name]
        table.drop_index(field)

    def create_constraint(self, table_name: str, constraint: cst.Constraint, fields: set[str]):
        table = self._tables[table_name]
        table.create_constraint(constraint, fields)

    def drop_constraint(self, table_name: str, constraint: cst.Constraint, fields: set[str]):
        table = self._tables[table_name]
        table.drop_constraint(constraint, fields)