from typing import Any, Type

from ..registry import Registry
from .util import Check


def true(value: Any) -> bool:
    return True


ISINSTANCE_REGISTRY: Registry[Type, Check] = Registry(
    {Any: true, object: true}  # type: ignore [dict-item]
)
