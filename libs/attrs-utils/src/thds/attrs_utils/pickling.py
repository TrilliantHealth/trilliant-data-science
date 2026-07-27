import dataclasses
import typing as ty
from functools import lru_cache

import attrs


@lru_cache(None)
def _field_names(cls: type) -> ty.Sequence[str]:
    if dataclasses.is_dataclass(cls):
        return [f.name for f in dataclasses.fields(cls)]
    elif attrs.has(cls):
        return [f.name for f in attrs.fields(cls)]
    else:
        raise TypeError(
            f"{PickleOnlyAttrs.__qualname__} can only be used for modifying `__getstate__`/`__setstate__` behavior on "
            f"`attrs` or `dataclasses` records, not {cls.__qualname__}."
        )


class PickleOnlyAttrs:
    """A mixin for overriding `__getstate__`/`__setstate__` on record types (either `attrs` or `dataclasses`). If your
    record has any functionality that might otherwise mutate instance `__dict__`s (e.g. `functools.cached_property`s),
    this ensures that _only_ the officially-declared attributes are pickled. This potentially saves space (at the
    possible expense of time post-deserialization), and ensures consistent pickle representation regardless of record
    history in case e.g. the pickle bytes are needed for some kind of content addressing (e.g. as a key to a memoized
    computation).

    Note: for classes with slots, `attrs` will ignore and override these methods, but that's fine because attrs'
    implementation behaves as we intend here in those cases. Subclassing this in that case is harmless but has no
    noteable effect.
    """

    __slots__ = ()
    # ensures that inheritors don't get an empty __dict__ when they intend to use slots, which would lead to unwanted
    # memory use

    def __getstate__(self) -> dict[str, ty.Any]:
        cls = type(self)
        fields = _field_names(cls)  # type: ignore[arg-type]
        return {f: getattr(self, f) for f in fields}

    def __setstate__(self, state: ty.Mapping[str, ty.Any]) -> None:
        for name, value in state.items():
            object.__setattr__(self, name, value)
