"""Special handling of typevar resolution for `attrs` and `dataclasses` record types."""

import copy
import dataclasses
import functools
import typing as ty

import attrs
import typing_inspect as ti

from ..utils import signature_preserving_cache
from .parameterize import parameterize, parameterized_mro


def field_origins(
    cls: ty.Type,
) -> ty.Dict[str, ty.Type]:
    """Map field names of a record type (e.g. `dataclasses` or `attrs`) to the parameterized base class where the field
    is first defined."""

    # find first class in MRO where each field is defined
    field_origins: ty.Dict[str, ty.Type] = dict()
    for base in parameterized_mro(cls):
        origin = ti.get_origin(base) or base
        for name in getattr(origin, "__annotations__", {}).keys():
            if name not in field_origins:
                field_origins[name] = base

    return field_origins


def _parameterize_attrs_field(
    field_origins: ty.Mapping[str, ty.Type],
    field: attrs.Attribute,
) -> attrs.Attribute:
    if parameterized_origin := field_origins.get(field.name):
        if field.type is None:
            return field
        else:
            concrete_type = parameterize(field.type, parameterized_origin)
            return field.evolve(type=concrete_type)
    else:
        return field


@signature_preserving_cache
def attrs_fields_parameterized(
    attrs_cls: ty.Type[attrs.AttrsInstance],
) -> ty.Sequence[attrs.Attribute]:
    """`attrs.fields` does not resolve typevars in the field types when base classes provide type parameters.
    This appears to be true even for classes decorated with `attrs.resolve_types`, which may only resolve ForwardRefs.
    This function has the same signature as `attrs.fields` but returns `Attribute`s with fully resolved `type` attributes.
    """
    try:
        attrs_cls = attrs.resolve_types(attrs_cls, include_extras=True)
    except Exception:
        pass

    return list(
        map(
            functools.partial(_parameterize_attrs_field, field_origins(attrs_cls)),
            attrs.fields(attrs_cls),
        )
    )


def _parameterize_dataclass_field(
    field_origins: ty.Mapping[str, ty.Type],
    field: dataclasses.Field,
) -> dataclasses.Field:
    if parameterized_origin := field_origins.get(field.name):
        concrete_type = parameterize(field.type, parameterized_origin)
        if concrete_type == field.type:
            return field
        else:
            new_field = copy.copy(field)
            new_field.type = concrete_type
            return new_field
    else:
        return field


@signature_preserving_cache
def dataclass_fields_parameterized(
    dataclass_cls: ty.Type,
) -> ty.Sequence[dataclasses.Field]:
    """`dataclasses.fields` does not resolve typevars in the field types when base classes provide type parameters.
    This function has the same signature as `dataclasses.fields` but returns `Field`s with fully resolved `type` attributes.
    """

    return list(
        map(
            functools.partial(_parameterize_dataclass_field, field_origins(dataclass_cls)),
            dataclasses.fields(ti.get_origin(dataclass_cls) or dataclass_cls),
        )
    )
