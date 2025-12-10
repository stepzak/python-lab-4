import operator
from typing import Callable

import src.base.operators as ops

OPERATORS: dict[str, Callable] = {
    'gt': operator.gt,
    'ge': operator.ge,
    'lt': operator.lt,
    'le': operator.le,
    'eq': operator.eq,
    'in': ops.in_,
}