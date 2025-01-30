"""Bring your own serialization."""

import typing as ty
from weakref import WeakValueDictionary

from thds.core.log import getLogger

from ..._utils.once import Once
from .types import Deserializer, Serializer, T

V = ty.TypeVar("V")

logger = getLogger(__name__)


class NeedsToBeWeakReferenceable(TypeError):
    pass


class ByIdRegistry(ty.Generic[T, V]):
    """When you want to use something as the key for a runtime-only
    dictionary, but the thing doesn't support being hashed.
    """

    def __init__(self) -> None:
        self._objects: ty.Dict[int, T] = WeakValueDictionary()  # type: ignore
        self._values: ty.Dict[int, V] = dict()

    def __setitem__(self, obj: T, value: V) -> None:
        try:
            self._objects[id(obj)] = obj
            self._values[id(obj)] = value
        except TypeError as te:
            raise NeedsToBeWeakReferenceable(f"{obj} needs to be weak-referenceable") from te

    def __contains__(self, obj: T) -> bool:
        return id(obj) in self._objects and self._objects[id(obj)] is obj

    def __getitem__(self, obj: T) -> V:
        if obj not in self:
            raise KeyError(str(obj))
        return self._values[id(obj)]


class ByIdSerializer:
    """Proxies id()-based memoizing serialization for large in-memory objects.

    For use with something like CallablePickler, which will allow this
    object to recognize registered objects and provide their
    serialization.

    Thread-safe at the time of (deferred) serialization, but all calls
    to `register` should be done prior to beginning concurrent serialization.

    The Deserializer returned by the Serializer should ideally not
    occupy much memory, as it will be cached.
    """

    def __init__(self, registry: ByIdRegistry[ty.Any, Serializer]) -> None:
        self._registry = registry
        self._desers: ty.Dict[int, Deserializer] = dict()
        self._once = Once()

    def __call__(self, obj: ty.Any) -> ty.Union[None, Deserializer]:
        if obj in self._registry:

            def serialize_and_cache() -> None:
                logger.info(f"Serializing object {type(obj)} {id(obj)}")
                self._desers[id(obj)] = self._registry[obj](obj)

            self._once.run_once(id(obj), serialize_and_cache)
            return self._desers[id(obj)]
        return None
