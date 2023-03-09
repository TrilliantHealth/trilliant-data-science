import typing as ty

import marshmallow as ma
import marshmallow.validate as mav


def _append_validator(validator: ty.Callable, field: ma.fields.Field) -> ma.fields.Field:
    """Marshmallow builds in arrays of validators, so this modifies the array"""
    if not field.validate:
        field.validate = validator
    field.validators.append(validator)
    return field


def nonempty_validator_xf(field: ma.fields.Field) -> ma.fields.Field:
    """Objects which have a length also have a known, meaningful default, e.g. empty string, empty list, empty dict.

    If you are requiring that an attribute have a value provided,
    then you're also stating that there is no meaningful default
    value for that attribute.

    If there is a meaningful default for the type, but no meaningful default for the attribute,
    then you should never be accepting the default value for that attribute.

    """

    def nonempty_validator(value: ty.Any) -> bool:
        try:
            if not len(value):
                raise mav.ValidationError("The length for a non-defaulting field must not be zero")
        except TypeError:
            pass
        return True

    if field.load_default == ma.missing:
        # this field should disallow 'empty'/falsy values, such as empty strings, empty lists, etc.
        return _append_validator(nonempty_validator, field)
    return field
