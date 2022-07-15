import typing as ty

import attrs
import marshmallow

T = ty.TypeVar("T")


def generate_attrs_post_load(attrs_class: ty.Type[T]):
    @marshmallow.post_load
    def post_load(self, data: dict, **_kw) -> T:
        return attrs_class(**data)

    return post_load


def _get_attrs_field_default(
    field: "attrs.Attribute[object]",
) -> object:
    # copyright Desert contributors - see LICENSE_desert.md
    if field.default == attrs.NOTHING:
        return marshmallow.missing
    if isinstance(field.default, attrs.Factory):  # type: ignore[arg-type]
        # attrs specifically doesn't support this so as to support the
        # primary use case.
        # https://github.com/python-attrs/attrs/blob/38580632ceac1cd6e477db71e1d190a4130beed4/src/attr/__init__.pyi#L63-L65
        if field.default.takes_self:  # type: ignore[attr-defined]
            return attrs.NOTHING
        return field.default.factory  # type: ignore[attr-defined]
    return field.default


class Attribute(ty.NamedTuple):
    name: str
    type: type
    init: bool
    default: object


def yield_attributes(attrs_class: type) -> ty.Iterator[Attribute]:
    hints = ty.get_type_hints(attrs_class)
    for attribute in attrs.fields(attrs_class):
        yield Attribute(
            attribute.name,
            hints.get(attribute.name, attribute.type),
            attribute.init,
            _get_attrs_field_default(attribute),
        )


def is_attrs_class(cls: type) -> bool:
    try:
        return bool(attrs.fields(cls))
    except TypeError:
        return False
