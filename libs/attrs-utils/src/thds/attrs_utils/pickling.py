import dataclasses

import attrs


class PickleOnlyAttrs:
    """A mixin for overriding `__getstate__` on record types (either `attrs` or `dataclasses`). If your record has any
    functionality that might otherwise mutate instance `__dict__`s (e.g. `functools.cached_property`s), this ensures
    that _only_ the officially-declared attributes are pickled. This potentially saves space (at the possible expense
    of time post-deserialization), and ensures consistent pickle representation regardless of record history in case
    e.g. the pickle bytes are needed for some kind of content addressing (e.g. as a key to a memoized computation).
    """

    def __getstate__(self):
        cls = type(self)
        if dataclasses.is_dataclass(cls):
            fields = [f.name for f in dataclasses.fields(cls)]
        elif attrs.has(cls):
            fields = [f.name for f in attrs.fields(cls)]
        else:
            raise TypeError(
                f"{PickleOnlyAttrs.__qualname__} can only be used for modifying `__getstate__` behavior on `attrs` or "
                f"`dataclasses` records, not {cls.__qualname__}."
            )

        return {f: getattr(self, f) for f in fields}
