from functools import lru_cache, partial, reduce
from operator import contains
from typing import Any, Callable, Collection, Iterable, Mapping, Optional, Sequence, Type, TypeVar

T = TypeVar("T")
U = TypeVar("U")

Check = Callable[[Any], bool]


def check_in_values(values: Collection) -> Check:
    return partial(contains, frozenset(values))


def _isinstance(type_: Type, value: Any) -> bool:
    # flipped isinstance for use with partial
    return isinstance(value, type_)


def simple_isinstance(type_: Type) -> Check:
    return partial(_isinstance, type_)


def all_values(check: Check, iter_: Optional[Callable[[Any], Iterable]], values: Iterable) -> bool:
    return all(map(check, values if iter_ is None else iter_(values)))


@lru_cache(None)
def check_all_values(check: Check, iter_: Optional[Callable[[Any], Iterable]] = None) -> Check:
    return partial(all_values, check, iter_)


def both(check1: Check, check2: Check, value: Any) -> bool:
    return check1(value) and check2(value)


@lru_cache(None)
def check_both(check1: Check, check2: Check) -> Check:
    return partial(both, check1, check2)


@lru_cache(None)
def check_all(*checks: Check) -> Check:
    return reduce(check_both, checks)


def either(check1: Check, check2: Check, value: Any) -> bool:
    return check1(value) or check2(value)


@lru_cache(None)
def check_either(check1: Check, check2: Check) -> Check:
    return partial(either, check1, check2)


@lru_cache(None)
def check_any(*checks: Check) -> Check:
    return reduce(check_either, checks)


def tuple_(checks: Sequence[Check], values: Sequence) -> bool:
    return (len(checks) == len(values)) and all(check(v) for check, v in zip(checks, values))


def typed_tuple(
    instancecheck: Check,
    checks: Sequence[Check],
    iter_: Optional[Callable[[Any], Iterable]],
    values: Sequence,
) -> bool:
    return instancecheck(values) and tuple_(checks, values if iter_ is None else tuple(iter_(values)))


@lru_cache(None)
def check_tuple(*checks: Check) -> Check:
    return partial(tuple_, checks)


@lru_cache(None)
def check_typed_tuple(
    instancecheck: Check, *checks: Check, iter_: Optional[Callable[[Any], Iterable]] = None
) -> Check:
    return partial(typed_tuple, instancecheck, checks, iter_)


def _attrs(names: Iterable[str], obj) -> Iterable:
    return (getattr(obj, name) for name in names)


@lru_cache(None)
def check_attrs(instancecheck: Check, names: Sequence[str], *checks: Check) -> Check:
    return check_typed_tuple(instancecheck, *checks, iter_=partial(_attrs, names))


def _items(mapping: Mapping):
    return mapping.items()


@lru_cache(None)
def check_mapping(instancecheck: Check, k_check: Check, v_check: Check) -> Check:
    return check_both(instancecheck, check_all_values(check_tuple(k_check, v_check), _items))


@lru_cache(None)
def check_collection(instancecheck: Check, v_check: Check) -> Check:
    return check_both(instancecheck, check_all_values(v_check))
