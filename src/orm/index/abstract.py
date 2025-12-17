from abc import abstractmethod, ABC

from src.orm.collection import Collection


class AbstractIndex(ABC):
    """
    Abstract class for indexes.
    """
    field_name: str

    @abstractmethod
    def __setitem__(self, key, value): ...

    @abstractmethod
    def __getitem__(self, key): ...

    @abstractmethod
    def __delitem__(self, key): ...

    @abstractmethod
    def clear(self):
        """Clear the index."""

    @abstractmethod
    def get_positions_for_query(self, op, value) -> set[int]:
        """
        Get positions for query.
        :param op: operator
        :param value: value to compare using operator
        :return: set of positions
        """

    def _add_element(self, key, val: int):
        """
        Add element to index. Must implement ``setdefault`` method
        :param key: usually the indexed value
        :param val: usually position

        """
        self.setdefault(key, set()).add(val)

    def rebuild(self, rows: Collection):
        """
        Rebuild the index.
        :param rows: rows to rebuild index with
        """
        self.clear()
        for pos, row in enumerate(rows):
            key = getattr(row, self.field_name)
            self._add_element(key, pos)

    def on_append(self, row, pos: int):
        """
        Callback for append element.
        :param row: row to be appended
        :param pos: position row will be on
        """
        key = getattr(row, self.field_name)
        self._add_element(key, pos)

    def on_update(self, old_row, new_row, pos: int):
        """
        Callback for update element.
        :param old_row: row before update
        :param new_row: row after update
        :param pos: position row is on
        """
        old_key = getattr(old_row, self.field_name)
        new_key = getattr(new_row, self.field_name)
        if old_key == new_key:
            return
        self[old_key].discard(pos)
        if not self[old_key]:
            del self[old_key]
        self._add_element(new_key, pos)

    def on_pop(self, row, pos: int):
        """
        Callback for pop element.
        :param row: row that is popped
        :param pos: position row is on
        """
        key = getattr(row, self.field_name)
        if key in self and pos in self[key]:
            self[key].discard(pos)
            if not self[key]:
                del self[key]