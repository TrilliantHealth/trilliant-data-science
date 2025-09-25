import datetime
from typing import Type

from ..registry import Registry
from . import builtin
from .util import Gen, T


class GenTypeRegistry(Registry[Type[T], Gen[T]]):
    # This class def only exists because of the mypy error "Type variable is unbound".
    # We use the typevar here however to assert that the registered generators generate instances of the
    # types they're registered to
    pass


# we define the registry here before passing it to the recursion so that the recursion can reference it
# after it's already registered implementations in the various type-specific sibling modules to this one
GEN_REGISTRY: GenTypeRegistry = GenTypeRegistry(
    [
        (type(None), builtin.random_null),
        (int, builtin.random_int),
        (bool, builtin.random_bool),
        (float, builtin.random_float),
        (str, builtin.random_str),
        (bytes, builtin.random_bytes),
        (bytearray, builtin.random_bytearray),
        (datetime.date, builtin.random_date),
        (datetime.datetime, builtin.random_datetime),
    ]
)
