"""Helpers for working with iterables. Intentionally *not* named 'itertools' to prevent confusion with the standard
library module of the same name."""

import collections
import itertools
import typing as ty
from typing import Hashable, Iterable, Iterator

from .types import Ord, T_Ord

T = ty.TypeVar("T")
U = ty.TypeVar("U")
H = ty.TypeVar("H", bound=Hashable)


def identity(item: T) -> T:
    return item


def first(seq: Iterable[T]) -> T | None:
    """Return the first item in the iterable if not empty, otherwise return None"""
    return next(iter(seq), None)


Grouped = ty.Mapping[U, list[T]]


def groupby(key: ty.Callable[[T], H], iterable: ty.Iterable[T]) -> Grouped[H, T]:
    """
    A simple groupby function that takes a function and an iterable and returns a dictionary of lists.
    """
    result = collections.defaultdict(list)
    for item in iterable:
        result[key(item)].append(item)
    return result


def most_common(
    counts: ty.Mapping[T_Ord, int], *, tiebreaker: ty.Callable[[T_Ord], Ord] = identity
) -> T_Ord | None:
    """Choose the most common item from a counter, breaking ties by choosing the item with the highest value."""
    if not counts:
        return None
    return max(counts.items(), key=lambda kv: (kv[1], tiebreaker(kv[0])))[0]


def most_common_grouped(
    items: Grouped[T_Ord, T], *, tiebreaker: ty.Callable[[T_Ord], Ord] = identity
) -> tuple[T_Ord, ty.Sequence[T]]:
    """Choose the most common item from a set of groups, breaking ties by choosing the item with the highest value.
    Return the most common key and its associated group. `items` is assumed to be nonempty; handling the empty case is
    the responsibility of the caller."""
    return max(items.items(), key=lambda kv: (len(kv[1]), tiebreaker(kv[0])))


def unique(items: ty.Iterable[H]) -> ty.Iterator[H]:
    """Unique values from an iterable, preserving order."""
    seen = set()
    for item in items:
        if item not in seen:
            yield item
            seen.add(item)


def unique_by(key: ty.Callable[[T], H | None], it: Iterable[T]) -> Iterator[T]:
    """Yield items from an iterable, preserving order, such that no item's key appears more than once, excluding items
    whose key is `None`. Excluding null keys allows the caller to avoid iterating and calling the key twice on each
    element in case certain keys are to be excluded."""
    seen: ty.Set[H] = set()
    for i in it:
        if (k := key(i)) is not None and k not in seen:
            yield i
            seen.add(k)


def at_most_one_unique(items: Iterable[T]) -> bool:
    """Lazy, constant-memory version of `len(set(items)) <= 1`. If you need to use a custom key, just compose with `map`
    as `at_most_one_unique(map(key, items))`."""
    items_ = iter(items)
    try:
        first = next(items_)
    except StopIteration:
        return True
    else:
        for i in items_:
            if i != first:
                return False
        return True


def lazy_connected_components(
    edge_predicate: ty.Callable[[T, T], bool], items: ty.Iterable[T]
) -> ty.List[list[T]]:
    """Compute connected components of a graph lazily; saves one from evaluating the edge predicate on all pairs.
    Items within each component are ordered by their index within the input iterable, and components are ordered by
    the lowest index of members."""
    components: ty.OrderedDict[int, list[T]] = collections.OrderedDict()
    next_component_id = 0
    for item in items:
        ids_overlapping = [
            id_
            for id_, component in components.items()
            if any(edge_predicate(item, other) for other in component)
        ]
        if not ids_overlapping:
            components[next_component_id] = [item]
            next_component_id += 1
        else:
            component = components[ids_overlapping[0]]
            for id_ in itertools.islice(ids_overlapping, 1, None):
                component.extend(components.pop(id_))
            component.append(item)
    return list(components.values())
