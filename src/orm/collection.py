from typing import TypeVar, Type, Generic, Iterator

T = TypeVar('T')

class Collection(Generic[T]):
    def __init__(self, dtype: Type[T]):
        if not isinstance(dtype, type):
            raise TypeError("`dtype` must be a class (type)")
        self._dtype = dtype
        self._items = []

    @property
    def dtype(self) -> Type[T]:
        return self._dtype

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, index: int) -> T:
        return self._items[index]

    def __setitem__(self, index: int, value: T):
        if not isinstance(value, self._dtype):
            raise TypeError(f"`value` must be an instance of {self._dtype.__name__}")
        self._items[index] = value

    def append(self, item: T) -> None:
        if not isinstance(item, self._dtype):
            raise TypeError(f"`item` must be an instance of {self._dtype.__name__}")
        self._items.append(item)

    def remove(self, item: T) -> None:
        if not isinstance(item, self._dtype):
            raise TypeError(f"`item` must be an instance of {self._dtype.__name__}")
        self._items.remove(item)

    def insert(self, item: T, index: int) -> None:
        if not isinstance(item, self._dtype):
            raise TypeError(f"`item` must be an instance of {self._dtype.__name__}")
        self._items.insert(index, item)

    def pop(self, index: int = -1) -> T:
        return self._items.pop(index)

    def index(self, item: T) -> int:
        if not isinstance(item, self._dtype):
            raise TypeError(f"`item` must be an instance of {self._dtype.__name__}")
        return self._items.index(item)

    def __contains__(self, item: object) -> bool:
        return item in self._items

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __eq__(self, other) -> bool:
        if isinstance(other, list):
            return other == self._items
        if isinstance(other, Collection):
            return self._items == other._items
        return False

class ImmutableCollection(Generic[T]):
    def __init__(self, collection: Collection[T]):
        self._collection = collection

    def __getitem__(self, index: int) -> T:
        return self._collection[index]

    @property
    def dtype(self):
        return self._collection.dtype

    def __len__(self) -> int:
        return len(self._collection)

    def __iter__(self) -> Iterator[T]:
        return iter(self._collection)

    def __contains__(self, item: T) -> bool:
        return item in self._collection

    def __str__(self) -> str:
        return str(self._collection._items)

    def __repr__(self) -> str:
        return f"ImmutableCollection({str(self)})"

    def __eq__(self, other) -> bool:
        if isinstance(other, tuple):
            return other == self._collection
        if isinstance(other, ImmutableCollection):
            return self._collection == other._collection
        return False