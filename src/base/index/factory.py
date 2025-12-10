import src.base.index.index_types as i_t
from src.base.index.abstract import AbstractIndex


class IndexFactory:
    _instances = {
        "base": i_t.BaseIndex,
        "range": i_t.RangeIndex,
    }

    @classmethod
    def create(cls, index_name: str, field_name: str) -> AbstractIndex:
        if index_name not in cls._instances:
            raise KeyError(f"Index name '{index_name}' not found.")
        return cls._instances[index_name](field_name)