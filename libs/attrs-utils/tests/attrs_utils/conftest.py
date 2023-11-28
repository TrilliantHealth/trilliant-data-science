import typing as ty
from typing import Literal, NamedTuple, NewType, Optional, TypeVar, Union

import attr

SimpleNewType = NewType("SimpleNewType", int)
NestedNewType = NewType("NestedNewType", SimpleNewType)
DoublyNestedNewType = NewType("DoublyNestedNewType", NestedNewType)


OptionalType = Optional[str]
DoublyOptionalType = Optional[OptionalType]
OptionalUnion = Optional[Union[int, str]]


LiteralInt = Literal[1, 2, 3]
LiteralStr = Literal["one", "two", "three"]
LiteralMixed = Literal[1, "two", 3, "four"]


TV = TypeVar("TV")


@attr.define
class RecordType:
    field: int


class NT(NamedTuple):
    x: int
    y: str


NTDynamic = NamedTuple("NTDynamic", [("x", int)])


@attr.define
class AttrsInherited(RecordType):
    other_field: Optional[str]


@attr.define
class AttrsInheritedAgain(AttrsInherited):
    other_field: str


TEST_TYPES = [
    bool,
    int,
    str,
    float,
    type(None),
    LiteralInt,
    LiteralStr,
    LiteralMixed,
    SimpleNewType,
    NestedNewType,
    DoublyNestedNewType,
    OptionalType,
    DoublyOptionalType,
    OptionalUnion,
    RecordType,
    NT,
    NTDynamic,
    ty.List[int],
    ty.List[ty.Optional[int]],
    ty.Dict[str, float],
    ty.Tuple[ty.Optional[int], str, float],
    ty.Optional[ty.List[int]],
    ty.Optional[ty.Dict[str, float]],
    ty.Union[ty.List[int], ty.Dict[str, float]],
    ty.DefaultDict[int, str],
    ty.Tuple[int, ...],
    ty.Mapping[int, NT],
    ty.MutableMapping[int, NTDynamic],
    ty.Tuple[NT, ...],
    ty.Collection[RecordType],
    ty.Sequence[DoublyNestedNewType],
]
