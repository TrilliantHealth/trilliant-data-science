import dataclasses
import pickle
from functools import cached_property

import attrs
import pytest

from thds.attrs_utils import pickling


class _WithCachedProps(pickling.PickleOnlyAttrs):
    x: int
    y: int

    @cached_property
    def z(self) -> int:
        return self.x + self.y


@dataclasses.dataclass
class DataClassWithCachedProps(_WithCachedProps):
    x: int
    y: int


@dataclasses.dataclass(slots=True)
class DataClassWithCachedPropsAndSlots(_WithCachedProps):
    x: int
    y: int


@attrs.define
class AttrsWithCachedProps(_WithCachedProps):
    x: int
    y: int


@attrs.define(slots=True)
class AttrsWithCachedPropsAndSlots(_WithCachedProps):
    x: int
    y: int


@pytest.mark.parametrize(
    "cls",
    [
        pytest.param(cls, id=cls.__name__)
        for cls in [
            DataClassWithCachedProps,
            DataClassWithCachedPropsAndSlots,
            AttrsWithCachedProps,
            AttrsWithCachedPropsAndSlots,
        ]
    ],
)
def test_pickle_only_attrs_excludes_non_attrs(cls: _WithCachedProps):
    inst = cls(x=1, y=2)  # type: ignore[operator]
    field_names_1 = set(vars(inst))
    pickle_bytes_1 = pickle.dumps(inst)

    _ = inst.z
    field_names_2 = set(vars(inst))
    pickle_bytes_2 = pickle.dumps(inst)

    assert field_names_1 < field_names_2
    assert field_names_2 - field_names_1 == {"z"}
    # `z` was added
    assert pickle_bytes_1 == pickle_bytes_2
    # but the pickles are the same

    inst = cls(x=1, y=2)  # type: ignore[operator]
    inst.__dict__["foo"] = "bar"
    # arbitrary namespace insertion
    field_names_3 = set(vars(inst))
    pickle_bytes_3 = pickle.dumps(inst)

    assert field_names_1 < field_names_3
    assert field_names_3 - field_names_1 == {"foo"}
    # `foo` was added
    assert pickle_bytes_1 == pickle_bytes_3
    # but the pickles are the same
