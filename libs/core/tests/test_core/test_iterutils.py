import contextlib
import typing as ty

import pytest

from thds.core import iterutils, types

T = ty.TypeVar("T")
H = ty.TypeVar("H")


def always(value: bool) -> ty.Callable[[int, int], bool]:
    def predicate(*args, **kwargs) -> bool:
        return value

    return predicate


def is_prime(n: int) -> bool:
    return n > 1 and all(n % i != 0 for i in range(2, n))


@pytest.mark.parametrize(
    "items, key, expected",
    [
        ([], always(True), {}),
        ([3, 2, 1], always(False), {False: [3, 2, 1]}),
        (range(10), is_prime, {False: [0, 1, 4, 6, 8, 9], True: [2, 3, 5, 7]}),
    ],
)
def test_groupby(items: ty.Iterable[T], key: ty.Callable[[T], H], expected: iterutils.Grouped[H, T]):
    result = iterutils.groupby(key, items)
    assert result == expected


@pytest.mark.parametrize(
    "counts, tiebreaker, expected",
    [
        (dict(), None, None),
        (dict(a=1), None, "a"),
        (dict(a=1, b=2), None, "b"),
        (dict(a=2, b=2, c=1), None, "b"),  # tiebreaker, highest char
        (dict(b=2, a=2), None, "b"),
        (dict(a=1, b=1, c=1), lambda c: -ord(c), "a"),  # tiebreaker, lowest char
    ],
)
def test_most_common(
    counts: ty.Mapping[types.T_Ord, int],
    tiebreaker: ty.Callable[[types.T_Ord], ty.Any] | None,
    expected: types.T_Ord | None,
):
    result = (
        iterutils.most_common(counts)
        if tiebreaker is None
        else iterutils.most_common(counts, tiebreaker=tiebreaker)
    )
    assert result == expected


@pytest.mark.parametrize(
    "groups, tiebreaker, expected, exc",
    [
        ({}, None, None, ValueError),
        (dict(a=[1]), None, ("a", [1]), None),
        (dict(a=[1, 2], b=[3, 4]), None, ("b", [3, 4]), None),  # tiebreaker, highest char
        (dict(c=[5, 6], a=[1, 2], b=[3, 4]), None, ("c", [5, 6]), None),
        (
            dict(c=[5, 6], a=[1, 2], b=[3, 4]),
            lambda c: -ord(c),
            ("a", [1, 2]),
            None,
        ),  # tiebreaker, lowest char
    ],
)
def test_most_common_grouped(
    groups: iterutils.Grouped[types.T_Ord, T],
    tiebreaker: ty.Callable[[types.T_Ord], ty.Any] | None,
    expected: tuple[types.T_Ord, list[T]],
    exc: ty.Type[Exception] | None,
):
    with contextlib.nullcontext() if exc is None else pytest.raises(exc):
        result = (
            iterutils.most_common_grouped(groups)
            if tiebreaker is None
            else iterutils.most_common_grouped(groups, tiebreaker=tiebreaker)
        )
        assert result == expected


@pytest.mark.parametrize(
    "items, expected",
    [
        ([], []),
        (range(10), list(range(10))),
        ([5, 2, 4, 3, 1], [5, 2, 4, 3, 1]),
        ([1, 1, 2, 3, 1, 2, 4, 3, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5]),
    ],
)
def test_unique(items: ty.Iterable[T], expected: list[T]):
    result = list(iterutils.unique(items))
    assert result == expected


@pytest.mark.parametrize(
    "items, key, expected",
    [
        ([], iterutils.identity, []),
        (range(10), iterutils.identity, list(range(10))),
        ([5, 2, 4, 3, 1], is_prime, [5, 4]),
        ([1, 1, 2, 3, 1, 2, 4, 3, 1, 2, 3, 4, 5], lambda x: x <= 4, [1, 5]),
    ],
)
def test_unique_by(items: ty.Iterable[T], key: ty.Callable[[T], H], expected: list[T]):
    result = list(iterutils.unique_by(key, items))
    assert result == expected


def same_modulo(modulus: int) -> ty.Callable[[int, int], bool]:
    def predicate(a: int, b: int) -> bool:
        return (a % modulus) == (b % modulus)

    return predicate


def both_prime(a: int, b: int) -> bool:
    return is_prime(a) and is_prime(b)


@pytest.mark.parametrize(
    "items, predicate, expected",
    [
        ([], always(True), []),
        ([1], always(True), [[1]]),
        ([1, 2, 3, 4, 5], always(True), [[1, 2, 3, 4, 5]]),
        ([1, 2, 3, 4, 5], always(False), [[1], [2], [3], [4], [5]]),
        ([1, 2, 3, 4, 5, 6], same_modulo(2), [[1, 3, 5], [2, 4, 6]]),
        ([1, 2, 3, 4, 5, 6], same_modulo(3), [[1, 4], [2, 5], [3, 6]]),
        ([6, 4, 1, 3, 2, 5], same_modulo(3), [[6, 3], [4, 1], [2, 5]]),
        ([1, 2, 3, 4, 5, 6, 7, 8], both_prime, [[1], [2, 3, 5, 7], [4], [6], [8]]),
        ([5, 3, 6, 1, 4, 8, 7, 2], both_prime, [[5, 3, 7, 2], [6], [1], [4], [8]]),
    ],
)
def test_lazy_connected_components(items, predicate, expected):
    components = iterutils.lazy_connected_components(predicate, items)
    assert components == expected
