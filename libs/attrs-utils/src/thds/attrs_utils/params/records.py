"""Special handling of typevar resolution for `attrs` and `dataclasses` record types."""

import copy
import dataclasses
import typing as ty

import attrs
import typing_inspect as ti

from ..utils import signature_preserving_cache
from .parameterize import parameterize, parameterized_mro
from .utils import TypeOrVar


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


def _replace_dataclass_field_type(
    dataclass_field: dataclasses.Field, new_type: TypeOrVar
) -> dataclasses.Field:
    new_field = copy.copy(dataclass_field)
    new_field.type = new_type  # type: ignore[assignment]
    # ^ NOTE: mypy thinks dataclasses.Field.type is always 'type', but it can be any type hint
    return new_field


_FT = ty.TypeVar("_FT", attrs.Attribute, dataclasses.Field)


def _parameterize_attrs_or_dataclass_field(
    field_origins: ty.Mapping[str, ty.Type],
    field: _FT,
) -> _FT:
    if (parameterized_origin := field_origins.get(field.name)) is not None:
        field_type = type(None) if field.type is None else field.type
        # shouldn't happen in pracice after type resolution, but we keep it here to make mypy happy
        concrete_type = parameterize(field_type, parameterized_origin)
        if concrete_type == field.type:
            return field
        elif isinstance(field, attrs.Attribute):
            return field.evolve(type=concrete_type)
        else:
            # dataclasses.Field
            return _replace_dataclass_field_type(field, concrete_type)
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
    attrs.resolve_types(
        ti.get_origin(attrs_cls) or attrs_cls, include_extras=True
    )  # this mutates in place
    origins = field_origins(attrs_cls)
    return [_parameterize_attrs_or_dataclass_field(origins, field) for field in attrs.fields(attrs_cls)]


def _resolve_dataclass_fields(
    dataclass_cls: ty.Type,
) -> ty.Tuple[dataclasses.Field, ...]:
    resolved_annotations = ty.get_type_hints(dataclass_cls, include_extras=True)
    fields = dataclasses.fields(dataclass_cls)
    return tuple(_replace_dataclass_field_type(f, resolved_annotations[f.name]) for f in fields)


@signature_preserving_cache
def dataclass_fields_parameterized(
    dataclass_cls: ty.Type,
) -> ty.Sequence[dataclasses.Field]:
    """`dataclasses.fields` does not resolve typevars in the field types when base classes provide type parameters.
    This function has the same signature as `dataclasses.fields` but returns `Field`s with fully resolved `type` attributes.
    """

    origins = field_origins(dataclass_cls)
    return [
        _parameterize_attrs_or_dataclass_field(origins, field)
        for field in _resolve_dataclass_fields(ti.get_origin(dataclass_cls) or dataclass_cls)
    ]
