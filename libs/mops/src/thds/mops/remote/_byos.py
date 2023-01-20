"""Bring your own serialization."""
import typing as ty
from weakref import WeakValueDictionary

from thds.core.log import getLogger

from ._once import Once
from ._pickle import Deserializer, Serializer, T

logger = getLogger(__name__)


class NeedsToBeWeakReferenceable(TypeError):
    pass


class BYOS:
    """Provides one-time, cached serialization for large in-memory objects.

    Thread-safe by necessity.

    The Deserializer returned by the Serialized should ideally not occupy much memory.
    """

    def __init__(self):
        self._registry: ty.Dict[int, ty.Any] = WeakValueDictionary()  # type: ignore
        self.sers: ty.Dict[int, Serializer] = dict()
        self.desers: ty.Dict[int, Deserializer] = dict()
        self.once = Once()

    def byos(self, obj: T, serializer: Serializer):
        logger.info(f"Registering obj {type(obj)} {id(obj)} for one-time serialization")
        try:
            self._registry[id(obj)] = obj
            self.sers[id(obj)] = serializer
        except TypeError as te:
            raise NeedsToBeWeakReferenceable(f"{obj} needs to be weak-referenceable") from te

    def __call__(self, obj: ty.Any) -> ty.Union[None, Deserializer]:
        if id(obj) in self._registry and self._registry[id(obj)] is obj:

            def serialize_and_cache():
                logger.info(f"Serializing object {type(obj)} {id(obj)}")
                self.desers[id(obj)] = self.sers[id(obj)](obj)

            self.once.run_once(id(obj), serialize_and_cache)
            return self.desers[id(obj)]
        return None
