import typing as ty
from typing import Collection, Dict, List, Mapping, Sequence, TypeVar

import pytest

from thds.attrs_utils.params import parameterized_mro

from .conftest import (
    Bar,
    Baz,
    DBar,
    DBaz,
    DFoo,
    DNonGeneric,
    DNotGenericAtAll,
    DQux,
    Foo,
    NonGeneric,
    NotGenericAtAll,
    Qux,
    T,
    U,
    V,
    W,
    X,
)

TV = TypeVar("TV")


@pytest.mark.parametrize(
    "type_, expected",
    [
        pytest.param(Foo[float], (Foo[float],), id="attrs simple"),
        pytest.param(Foo, (Foo[T],), id="attrs simple unparameterized"),
        pytest.param(Bar[str], (Bar[str],), id="attrs simple covariant"),
        pytest.param(
            Baz[int],
            (
                Baz[int],
                Bar[int],
            ),
            id="attrs one-level inheritance",
        ),
        pytest.param(
            Qux[int, float],
            (Qux[int, float], Foo[float], Baz[int], Bar[int]),
            id="attrs multiple inheritance",
        ),
        pytest.param(Foo[TV], (Foo[TV],), id="attrs with new typevar"),
        pytest.param(Bar[V], (Bar[V],), id="attrs with reused covariant typevar"),
        pytest.param(
            Baz[T],
            (
                Baz[T],
                Bar[T],
            ),
            id="attrs one-level inheritance with typevar",
        ),
        pytest.param(
            Qux[T, float],
            (Qux[T, float], Foo[float], Baz[T], Bar[T]),
            id="attrs multiple inheritance with mixed reused typevar and type",
        ),
        pytest.param(
            Qux[U, TV],
            (Qux[U, TV], Foo[TV], Baz[U], Bar[U]),
            id="attrs multiple inheritance with mixed reused and new typevars",
        ),
        pytest.param(
            Qux,
            (Qux[X, W], Foo[W], Baz[X], Bar[X]),
            id="attrs multiple inheritance unparameterized",
        ),
        pytest.param(
            NonGeneric,
            (NonGeneric, Qux[int, str], Foo[str], Baz[int], Bar[int]),
            id="attrs deep inheritance with concrete types",
        ),
        pytest.param(
            NotGenericAtAll,
            (NotGenericAtAll, Qux[int, str], Foo[str], Baz[int], Bar[int]),
            id="attrs deep inheritance with concrete types and non-generic subclass",
        ),
        pytest.param(DFoo[float], (DFoo[float],), id="dataclasses simple"),
        pytest.param(DFoo, (DFoo[T],), id="dataclasses simple unparameterized"),
        pytest.param(DBar[str], (DBar[str],), id="dataclasses simple covariant"),
        pytest.param(
            DBaz[int],
            (
                DBaz[int],
                DBar[int],
            ),
            id="dataclasses one-level inheritance",
        ),
        pytest.param(
            DQux[int, float],
            (DQux[int, float], DFoo[float], DBaz[int], DBar[int]),
            id="dataclasses multiple inheritance",
        ),
        pytest.param(DFoo[TV], (DFoo[TV],), id="dataclasses with new typevar"),
        pytest.param(DBar[V], (DBar[V],), id="dataclasses with reused covariant typevar"),
        pytest.param(
            DBaz[T],
            (
                DBaz[T],
                DBar[T],
            ),
            id="dataclasses one-level inheritance with typevar",
        ),
        pytest.param(
            DQux[T, float],
            (DQux[T, float], DFoo[float], DBaz[T], DBar[T]),
            id="dataclasses multiple inheritance with mixed reused typevar and type",
        ),
        pytest.param(
            DQux[U, TV],
            (DQux[U, TV], DFoo[TV], DBaz[U], DBar[U]),
            id="dataclasses multiple inheritance with mixed reused and new typevars",
        ),
        pytest.param(
            DQux,
            (DQux[X, W], DFoo[W], DBaz[X], DBar[X]),
            id="dataclasses multiple inheritance unparameterized",
        ),
        pytest.param(
            DNonGeneric,
            (DNonGeneric, DQux[int, str], DFoo[str], DBaz[int], DBar[int]),
            id="dataclasses deep inheritance with concrete types",
        ),
        pytest.param(
            DNotGenericAtAll,
            (DNotGenericAtAll, DQux[int, str], DFoo[str], DBaz[int], DBar[int]),
            id="dataclasses deep inheritance with concrete types and non-generic subclass",
        ),
        # The following tests are for standard library generics. Currently, parameterized_mro does not trace the
        # inheritance relationships of these (which would require some delicate tracking of relationships between
        # collections.abc and typing annotation types), so the expected MROs only contain the type itself.
        pytest.param(
            Collection[int],
            (Collection[int],),
            id="standard generic collection",
        ),
        pytest.param(
            List[TV],
            (List[TV],),
            id="standard generic list with typevar",
        ),
        pytest.param(
            Sequence[str],
            (Sequence[str],),
            id="standard generic sequence",
        ),
        pytest.param(
            Mapping[str, int],
            (Mapping[str, int],),
            id="standard generic mapping",
        ),
        pytest.param(
            Dict[TV, V],
            (Dict[TV, V],),
            id="standard generic dict with mixed typevar",
        ),
    ],
)
def test_parameterized_mro(type_: ty.Type, expected: ty.Tuple[ty.Type, ...]):
    mro = parameterized_mro(type_)
    assert mro == expected
