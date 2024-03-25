import typing as ty

import attrs
from attr._make import Factory  # cannot import the actual class via attrs


def has_value_default(default: ty.Any) -> bool:
    if isinstance(default, Factory):
        # a takes_self Factory requires an instance to create the default; therefore
        # there is no single, known default value to extract
        return not default.takes_self
    return default is not attrs.NOTHING


def extract_factory_default(default: ty.Any) -> ty.Any:
    if isinstance(default, Factory):
        assert not default.takes_self
        return default.factory()

    return default


def attrs_value_defaults(attrs_cls: ty.Type) -> ty.Dict[str, ty.Any]:
    """Returns a dictionary populated only by attribute names and their default values -
    and only for attributes which _have_ simple default values.
    """
    return {
        name: extract_factory_default(field.default)
        for name, field in attrs.fields_dict(attrs_cls).items()
        if has_value_default(field.default)
    }
