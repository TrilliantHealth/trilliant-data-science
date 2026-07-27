import dataclasses
import inspect
import pickle
import typing as ty
from functools import cached_property

import attrs
import pytest

from thds.attrs_utils import pickling


class _WithCachedProps(ty.Protocol):
    # just an interface type for annotating the tests below
    x: int
    y: int

    def __init__(self, x: int, y: int) -> None: ...

    def __getstate__(self) -> dict[str, ty.Any]: ...

    @cached_property
    def z(self) -> int:
        return self.x + self.y


# NOTE: dataclasses can't handle cached_property on slotted classes, unlike attrs, so we don't test that case here


@dataclasses.dataclass(frozen=False)
class DataClassWithCachedProps(pickling.PickleOnlyAttrs):
    x: int
    y: int

    @cached_property
    def z(self) -> int:
        return self.x + self.y


@dataclasses.dataclass(frozen=True)
class DataClassWithCachedPropsFrozen(pickling.PickleOnlyAttrs):
    x: int
    y: int

    @cached_property
    def z(self) -> int:
        return self.x + self.y


@attrs.define(slots=False, frozen=False)
class AttrsWithCachedProps(pickling.PickleOnlyAttrs):
    x: int
    y: int

    @cached_property
    def z(self) -> int:
        return self.x + self.y


@attrs.define(slots=False, frozen=True)
class AttrsWithCachedPropsFrozen(pickling.PickleOnlyAttrs):
    x: int
    y: int

    @cached_property
    def z(self) -> int:
        return self.x + self.y


@attrs.define(slots=True, frozen=False)
class AttrsWithCachedPropsAndSlots(pickling.PickleOnlyAttrs):
    x: int
    y: int

    @cached_property
    def z(self) -> int:
        return self.x + self.y


@attrs.define(slots=True, frozen=True)
class AttrsWithCachedPropsAndSlotsFrozen(pickling.PickleOnlyAttrs):
    x: int
    y: int

    @cached_property
    def z(self) -> int:
        return self.x + self.y


_TEST_CLASSES = [
    pytest.param(cls, id=cls.__name__)
    for cls in [
        DataClassWithCachedProps,
        DataClassWithCachedPropsFrozen,
        AttrsWithCachedProps,
        AttrsWithCachedPropsFrozen,
        AttrsWithCachedPropsAndSlots,
        AttrsWithCachedPropsAndSlotsFrozen,
    ]
]


def _ns(inst: _WithCachedProps) -> ty.Set[str]:
    try:
        return set(vars(inst))
    except TypeError:
        # slots
        return set(getattr(type(inst), "__slots__", ()))


@pytest.mark.parametrize("cls", _TEST_CLASSES)
def test_pickle_only_attrs_excludes_non_attrs(cls: type[_WithCachedProps]):
    assert pickling.PickleOnlyAttrs in inspect.getmro(cls)
    # make sure `cls` is a `PickleOnlyAttrs` subclass

    inst = cls(x=1, y=2)
    field_names_1 = _ns(inst)
    pickle_bytes_1 = pickle.dumps(inst)
    assert inst.__getstate__() == dict(x=1, y=2)

    _ = inst.z
    pickle_bytes_2 = pickle.dumps(inst)
    assert inst.__getstate__() == dict(x=1, y=2)

    if hasattr(inst, "__dict__"):
        field_names_2 = _ns(inst)
        assert field_names_1 < field_names_2
        assert field_names_2 - field_names_1 == {"z"}
        # `z` was added

    assert pickle_bytes_1 == pickle_bytes_2
    # but the pickles are the same

    if hasattr(inst, "__dict__"):
        inst = cls(x=1, y=2)
        inst.__dict__["foo"] = "bar"
        # arbitrary namespace insertion
        field_names_3 = set(vars(inst))
        pickle_bytes_3 = pickle.dumps(inst)

        assert field_names_1 < field_names_3
        assert field_names_3 - field_names_1 == {"foo"}
        # `foo` was added
        assert pickle_bytes_1 == pickle_bytes_3
        # but the pickles are the same


@pytest.mark.parametrize("cls", _TEST_CLASSES)
def test_pickle_only_attrs_roundtrip(cls: type[_WithCachedProps]):
    inst = cls(x=1, y=2)
    pickle_bytes_1 = pickle.dumps(inst)
    assert pickle.loads(pickle_bytes_1) == inst

    _ = inst.z
    pickle_bytes_2 = pickle.dumps(inst)
    assert pickle.loads(pickle_bytes_2) == inst


@dataclasses.dataclass(slots=True)
class DataClassSlotsOnly(pickling.PickleOnlyAttrs):
    x: int
    y: int


def test_slots_only_dataclass_roundtrips():
    # can't test mutation of the namespace with a cached_property on this one since dataclasses doesn't allow it, but
    # we can at least ensure the pickle round-trips
    inst = DataClassSlotsOnly(1, 2)
    assert not hasattr(inst, "__dict__")  # slots
    assert inst.__getstate__() == dict(x=1, y=2)
    assert pickle.loads(pickle.dumps(inst)) == inst


def test_non_record_raises_on_getstate():
    class NotARecord(pickling.PickleOnlyAttrs):
        pass

    with pytest.raises(TypeError, match="PickleOnlyAttrs"):
        pickle.dumps(NotARecord())
