from typing import Callable, Dict, Sequence, Type, TypeVar

import attrs

from ..params import attrs_fields_parameterized
from ..registry import Registry
from ..type_utils import is_namedtuple_type
from .util import Gen, T

AT = TypeVar("AT", bound=attrs.AttrsInstance)


class GenRecordByFieldNameRegistry(Registry[Type, Dict[str, Gen]]):
    # this class def only exists because of the mypy error "Type variable is unbound"
    pass


CUSTOM_ATTRS_BY_FIELD_REGISTRY = GenRecordByFieldNameRegistry()


def random_attrs(
    constructor: Callable[..., AT], arg_gens: Sequence[Gen[T]], kwarg_gens: Dict[str, Gen[T]]
) -> AT:
    return constructor(*(gen() for gen in arg_gens), **{name: gen() for name, gen in kwarg_gens.items()})


def _register_random_gen_by_field(type_: Type[T], **gens: Gen):
    if attrs.has(type_):
        fields = attrs_fields_parameterized(type_)
        names: Sequence[str] = [f.name for f in fields]
    elif is_namedtuple_type(type_):
        names = type_._fields
    else:
        raise TypeError(f"Don't know how to interpret {type_} as a record type")

    unknown_names = set(gens).difference(names)
    assert not unknown_names, f"Unknown fields: {unknown_names}"

    CUSTOM_ATTRS_BY_FIELD_REGISTRY.register(type_, gens)


def register_random_gen_by_field(**gens: Gen):
    """Register random generators for the fields of a record type by specifying random generators by name

    Example:

        from typing import NamedTuple

        @register_random_gen_by_field(foo=lambda: 42):
        class Bar(NamedTuple):
            foo: int
    """

    def decorator(type_: Type[AT]) -> Gen[AT]:
        _register_random_gen_by_field(type_, **gens)
        return type_

    return decorator
