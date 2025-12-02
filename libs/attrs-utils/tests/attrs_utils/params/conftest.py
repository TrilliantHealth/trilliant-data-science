import dataclasses
import typing as ty

import attrs

T = ty.TypeVar("T")
U = ty.TypeVar("U", covariant=True)
V = ty.TypeVar("V", covariant=True)
W = ty.TypeVar("W")
X = ty.TypeVar("X")


# Parallel inheritance hierarchies with attrs and dataclasses


@attrs.define
class Foo(ty.Generic[T]):
    t: ty.Optional[T]


@dataclasses.dataclass
class DFoo(ty.Generic[T]):
    t: ty.Optional[T]


@attrs.define(slots=False)
class Bar(ty.Generic[U]):
    u: U


@dataclasses.dataclass
class DBar(ty.Generic[U]):
    u: U


@attrs.define(slots=False)
class Baz(Bar[V]):
    v: V


@dataclasses.dataclass
class DBaz(DBar[V]):
    v: V


@attrs.define(slots=False)
# slots=False to allow multiple bases without layout issues
class Qux(ty.Generic[X, W], Foo[W], Baz[X]):
    t: W  # specializing from Foo where this is Optional[W]
    w: W
    x: X
    baz: Baz[X]


@dataclasses.dataclass
class DQux(ty.Generic[X, W], DFoo[W], DBaz[X]):
    t: W
    w: W
    x: X
    baz: DBaz[X]


@attrs.define
class NonGeneric(Qux[int, str]):
    z: bool


@dataclasses.dataclass
class DNonGeneric(DQux[int, str]):
    z: bool


@attrs.define
class NotGenericAtAll(NonGeneric):
    bar: Bar[bool]


@dataclasses.dataclass
class DNotGenericAtAll(DNonGeneric):
    bar: DBar[bool]


@attrs.define
class PartiallyGeneric(NonGeneric, ty.Generic[T]):
    foo: Foo[T]


@dataclasses.dataclass
class DPartiallyGeneric(DNonGeneric, ty.Generic[T]):
    foo: DFoo[T]
