"""Utilities for (de)serializing visit tasks."""

import dataclasses
import typing as ty
import warnings
from functools import partial

import attrs
import cattrs

from thds.attrs_utils.isinstance import isinstance as isinstance_
from thds.attrs_utils.params import attrs_fields_parameterized, dataclass_fields_parameterized

from .converter import Struct

T = ty.TypeVar("T")


def _structure_with_defaults(
    base_converter: cattrs.Converter,
    defaults: dict[str, ty.Any],
    data: ty.Any,
    cl: ty.Type[T],
) -> T:
    return base_converter.structure({**defaults, **data}, cl)


def structure_hook_with_defaults(
    base_converter: cattrs.Converter,
    type_: ty.Type[T],
    defaults: dict[str, ty.Any],
    override: bool = False,
) -> Struct[T]:
    """Cattrs structure hook for record types that allows for default values for missing fields, to allow for
    deserializing data that may be missing fields that were added later.

    :param base_converter: The cattrs converter to use for structuring the data after filling in defaults. This allows
      use of a custom converter with other hooks registered, while still getting the default-filling behavior of this hook.
    :param type_: The type to structure into. Must be an attrs or dataclass record type.
    :param defaults: A mapping from field names to default values to use for any fields which may be missing in the serialized
      data. When absent in serialized data, the specified fields will be supplied with these default values.
    :param override: If True, the default values will override any field default values defined in the type itself.
    """

    if attrs.has(type_):
        fields: ty.Collection[attrs.Attribute | dataclasses.Field] = attrs_fields_parameterized(type_)
    elif dataclasses.is_dataclass(type_):
        fields = dataclass_fields_parameterized(type_)
    else:
        raise TypeError(
            f"structure_hook_with_defaults only supports attrs or dataclass record types, but got {type_}"
        )

    thisfunc = structure_hook_with_defaults.__qualname__
    field_types = {field.name: field.type for field in fields if field.name in defaults}
    if unknown := set(defaults) - set(field_types):
        warnings.warn(
            f"{thisfunc} received default values for fields {unknown} which are not present in type {type_}"
        )
        defaults = {name: default for name, default in defaults.items() if name in field_types}

    if not defaults:
        # no longer needed; the defaulted fields have been removed
        return base_converter.get_structure_hook(type_)

    if not override:
        # pop out any default values which already have a default defined in the type itself
        if field_defaults := {
            field.name: field.default
            for field in fields
            if field.name in defaults
            and field.default is not dataclasses.MISSING
            and field.default is not attrs.NOTHING
        }:
            warnings.warn(
                f"{thisfunc} received default values for fields {set(field_defaults)} which already "
                f"have default values defined in type {type_}. These defaults will be ignored since override=False, "
                f"but you may want to remove them from the `defaults` arg in your code."
            )
            defaults = {
                name: default for name, default in defaults.items() if name not in field_defaults
            }

    if badly_typed := {
        name: default
        for name, default in defaults.items()
        if not isinstance_(default, type(None) if (t := field_types[name]) is None else t)
    }:
        badly_typed_msg = ", ".join(
            f"{name} (expected {field_types[name]}, got {default!r})"
            for name, default in badly_typed.items()
        )
        raise TypeError(
            f"{thisfunc} received default values which are not of the expected type "
            f"for fields in {type_}: {badly_typed_msg}"
        )

    return ty.cast(Struct[T], partial(_structure_with_defaults, base_converter, defaults))
    # we know this is correct, but for some reason mypy has trouble with use of `partial` here in certain versions


def register_structure_hook_with_defaults(
    converter: cattrs.Converter,
    base_converter: cattrs.Converter,
    type_: ty.Type[T],
    defaults: dict[str, ty.Any],
    override: bool = False,
) -> None:
    """Register a cattrs structure hook for the given type which allows for default values for missing fields, to allow for
    deserializing data that may be missing fields that were added later.

    :param converter: The cattrs converter to register the hook on. This is the converter that will ultimately be used for
      structuring data of the given type, with the defaults filled in as specified.
    :param base_converter: The cattrs converter to use for structuring the data after filling in defaults. This allows
      use of a custom converter with other hooks registered, while still getting the default-filling behavior of this hook.
    :param type_: The type to structure into. Must be an attrs or dataclass record type.
    :param defaults: A mapping from field names to default values to use for any fields which may be missing in the serialized
      data. When absent in serialized data, the specified fields will be supplied with these default values.
    :param override: If True, the default values will override any field default values defined in the type itself.
    """

    converter.register_structure_hook(
        type_, structure_hook_with_defaults(base_converter, type_, defaults, override)
    )
