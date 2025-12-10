from typing import TypeVar, Generic, Type, Callable
from typing import Iterator
from src.base.index.factory import IndexFactory
from src.base.collection import Collection
from src.base.index.abstract import AbstractIndex
import src.constants as cst

T = TypeVar('T')

class Table(Generic[T]):
    def __init__(self, collection: Collection[T]):
        self._indexes: dict[str, AbstractIndex] = {}
        self._rows = collection

    @property
    def dtype(self) -> Type[T]:
        return self._rows.dtype

    def create_index(self, index_type: str, field_name: str) -> None:
        if field_name not in self._indexes:
            idx = IndexFactory.create(index_type, field_name)
            idx.rebuild(self._rows)
            self._indexes[field_name] = idx

    def append(self, item: T) -> None:
        pos = len(self._rows)
        self._rows.append(item)
        for idx in self._indexes.values():
            idx.on_append(item, pos)

    def pop(self) -> T:
        item = self._rows.pop()
        pos = len(self._rows)
        for idx in self._indexes.values():
            idx.on_pop(item, pos)
        return item

    def remove(self, item: T) -> None:
        self._rows.remove(item)
        for idx in self._indexes.values():
            idx.rebuild(self._rows)

    def update_at(self, pos: int, new_row: T) -> None:
        if not isinstance(new_row, self.dtype):
            raise TypeError(f"Expected instance of {self.dtype.__name__}")
        old_row = self._rows[pos]
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

    def query(self, **filters) -> set[int]:
        """
        :param filters: kwarg, passed as: FIELD__OPERATOR = VALUE; e. g. query(name__eq = 'Steve', age__gt = 18)
        :return: set of indexes
        """
        result: set[int] = set()
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


    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self) -> Iterator[T]:
        return iter(self._rows)