import operator
from collections import UserDict
from typing import Any, Iterator

from sortedcontainers import SortedDict

from src.orm.index.abstract import AbstractIndex
import src.orm.operators as ops

class BaseIndex(UserDict[Any, set[int]], AbstractIndex):

    def __init__(self, field_name: str):
        super().__init__()
        self.field_name = field_name

    def get_positions_for_query(self, op, value):
        if op is operator.eq:
            return self.get(value, set())
        elif op == ops.in_:
            res = set()
            for k in value:
                res |= self.get(k, set())
            return res

        raise NotImplementedError

class RangeIndex(AbstractIndex):
    def __init__(self, field_name: str):
        self.field_name = field_name
        self._data = SortedDict()

    def __getitem__(self, item) -> set[int]:
        return self._data[item]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __contains__(self, item):
        return item in self._data

    def __iter__(self):
        return iter(self._data)

    def __delitem__(self, key):
        del self._data[key]

    def clear(self):
        self._data.clear()

    def setdefault(self, key, default):
        return self._data.setdefault(key, default)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def _get_slice(self, start_key: Any = None, end_key: Any = None, inclusive_start: bool = True, inclusive_end: bool = True) -> Iterator[set[int]]:
        if start_key is None and end_key is None:
            yield from self._data.values()
            return

        keys = self._data.irange(
            minimum=start_key,
            maximum=end_key,
            inclusive=(inclusive_start, inclusive_end)
        )
        for k in keys:
            yield self[k]

    def find_positions_gt(self, value: Any) -> set[int]:
        result = set()
        for positions in self._get_slice(start_key=value, inclusive_start=False):
            result |= positions
        return result

    def find_positions_ge(self, value: Any) -> set[int]:
        result = set()
        for positions in self._get_slice(start_key=value, inclusive_start=True):
            result |= positions
        return result

    def find_positions_lt(self, value: Any) -> set[int]:
        result = set()
        for positions in self._get_slice(end_key=value, inclusive_end=False):
            result |= positions
        return result

    def find_positions_le(self, value: Any) -> set[int]:
        result = set()
        for positions in self._get_slice(end_key=value, inclusive_end=True):
            result |= positions
        return result

    def find_positions_between(self, low: Any, high: Any, inclusive: bool = True) -> set[int]:
        result = set()
        for positions in self._get_slice(
            start_key=low,
            end_key=high,
            inclusive_start=inclusive,
            inclusive_end=inclusive
        ):
            result |= positions
        return result

    def get_positions_for_query(self, op, value) -> set[int]:
        match op:
            case operator.eq:
                return self.get(value, set())
            case operator.gt:
                return self.find_positions_gt(value)
            case operator.ge:
                return self.find_positions_ge(value)
            case operator.lt:
                return self.find_positions_lt(value)
            case operator.le:
                return self.find_positions_le(value)
            case ops.in_:
                res = set()
                for k in value:
                    res |= self.get(k, set())
                return res
            case _:
                raise NotImplementedError