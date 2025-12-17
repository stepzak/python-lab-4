import src.orm.index.index_types as i_t
from src.orm.index.abstract import AbstractIndex


class IndexFactory:
    """
    Index factory.
    """
    _instances = {
        "base": i_t.BaseIndex,
        "range": i_t.RangeIndex,
    }

    @classmethod
    def create(cls, index_type: str, field_name: str) -> AbstractIndex:
        """
        Factory method to create an index.
        :param index_type: type of index
        :param field_name: field to create index on
        :return:
        """
        if index_type not in cls._instances:
            raise KeyError(f"Index name '{index_type}' not found.")
        return cls._instances[index_type](field_name)